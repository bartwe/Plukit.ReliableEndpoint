using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Plukit.ReliableEndpoint {
    /*
     * 
     * Bidirectional
     * Ensures reliable, ordered delivery of a byte stream
     * Stream orientated and does not preserve message boundaries
     * Not threadsafe
     * 
     * Call Update() on your tick update tick.
     * 
     * call SendMessage to send data
     * ReceiveMessageCallback gets called for delivery of data sent by remote side
     * 
     * TransmitPacketCallback gets called with raw packet data to be put on the network
     * call ReceivePacket with the raw packet data received from the network
     * 
     * 
     * buffer.Length should not be used on these buffers, the value is not useful
     * 
     * transmitPacketCallback(byte[] buffer, int length) -> bool (channel congested/closed)
     * receiveMessageCallback(byte[] buffer, int offset, int length)
     * 
     * 
     */

    public class Channel {
        readonly Func<byte[], int, bool> _transmitPacketCallback;
        readonly Action<byte[], int, int> _receiveMessageCallback;
        bool _receiveMessageCallbackTakesOwnership;

        readonly Func<int, byte[]> _allocate;
        readonly Action<byte[]> _release;

        const int WindowAckSize = 256;
        const int WindowAckBytesSize = WindowAckSize / 8;
        const int OuterPacketSize = 1200;
        const int HeaderSize = 4 + 4 + 4 + WindowAckBytesSize;

        const int InitialResendDelay = 1000;
        const int MissedResendDelay = 250;
        const int ResendStandOff = 128;
        const int AckResendStandOff = 1;

        const int SendWindowFull = 2048;
        const int UnackedDataLimit = 1024 * 1024;

        const int MaxStandOff = 6;
        const int MaxAckStandOff = 6;

        const int IdleTimeout = 60000;

        const int ReSendAckOldestEveryNth = 5;

        bool _disposed;

        byte[] _messageBuffer;
        int _messageBufferOffset;

        int _sendWindowStart;
        int _sendWindowHighestAck;

        readonly List<Packet> _sendWindow = new List<Packet>();


        int _receiveWindowStart;
        readonly List<Packet> _receiveWindow = new List<Packet>();

        readonly Stopwatch _timer = Stopwatch.StartNew();

        int _sendSequenceId;
        bool _congested;
        int _unackedDataSize;

        bool _ackRequested;
        int _idleAckStandOff;
        long _idleAckTS;
        int _oldestPacketResendAckIndex;

        long _lastReceived;
        bool _idleTimeout;
        bool _failure;

        readonly uint _localSignature;
        uint _remoteSignature;
        bool _remoteSignatureKnown;

        bool _packetReceived;

        public bool Congested { get { return _congested; } }
        public bool Timeout { get { return _idleTimeout; } }
        public bool Failure { get { return _failure; } }
        public long DebugElapsedTimeBias;

        readonly List<int> _resendables = new List<int>();

        public Channel(bool serverSide, Func<int, byte[]> allocate, Action<byte[]> release, Func<byte[], int, bool> transmitPacketCallback, Action<byte[], int, int> receiveMessageCallback, bool receiveMessageCallbackTakesOwnership) {
            if (allocate == null)
                throw new ArgumentNullException("allocate");
            if (release == null)
                throw new ArgumentNullException("release");
            if (transmitPacketCallback == null)
                throw new ArgumentNullException("transmitPacketCallback");
            if (receiveMessageCallback == null)
                throw new ArgumentNullException("receiveMessageCallback");

            _allocate = allocate;
            _release = release;
            _transmitPacketCallback = transmitPacketCallback;
            _receiveMessageCallback = receiveMessageCallback;
            _receiveMessageCallbackTakesOwnership = receiveMessageCallbackTakesOwnership;
            var guid = Guid.NewGuid().ToByteArray();
            var stamp = (guid[0]) ^ (guid[1] << 8) ^ (guid[2] << 16) ^ (guid[3] << 24)
                        ^ (guid[4]) ^ (guid[5] << 8) ^ (guid[6] << 16) ^ (guid[7] << 24)
                        ^ (guid[8]) ^ (guid[9] << 8) ^ (guid[10] << 16) ^ (guid[11] << 24)
                        ^ (guid[12]) ^ (guid[13] << 8) ^ (guid[14] << 16) ^ (guid[15] << 24);
            //signature lsb encodes if the connection is server or clientsided, remote side must use a signature with the lowest bit flipped
            var signature = ((uint)(stamp & 0x7fffffff) << 1) | (serverSide ? 1u : 0u);

            _lastReceived = _timer.ElapsedMilliseconds + DebugElapsedTimeBias;
            _localSignature = signature;
        }

        public void Dispose() {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name);
            _disposed = true;
        }

        public void Update() {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name);

            if (Failure)
                return;

            FlushMessageBuffer();
            _congested = false;
            var now = _timer.ElapsedMilliseconds + DebugElapsedTimeBias;
            _unackedDataSize = 0;
            var packetSent = false;
            _resendables.Clear();
            for (var i = 0; i < _sendWindow.Count; ++i) {
                var packet = _sendWindow[i];
                if (packet.Buffer != null) {
                    var nextSend = CalcSendMoment(packet, (i + _sendWindowStart) > _sendWindowHighestAck);
                    _unackedDataSize += packet.Length;
                    if (((i < WindowAckSize) || (packet.SendCount == 0)) && (nextSend <= now)) {
                        WriteAckheader(packet.Buffer);
                        if (!_transmitPacketCallback(packet.Buffer, packet.Length)) {
                            _congested = true;
                            break;
                        }
                        packetSent = true;

                        packet.SendCount++;
                        if (packet.SendCount > MaxStandOff)
                            packet.SendCount = MaxStandOff;
                        _sendWindow[i] = packet;
                    }
                    else {
                        if (_resendables.Count < ReSendAckOldestEveryNth)
                            _resendables.Add(i);
                    }
                }
            }
            CheckCongested();

            if (_idleAckStandOff > MaxAckStandOff)
                _idleAckStandOff = MaxAckStandOff;

            var ackTS = _idleAckTS + ((_idleAckStandOff == 0) ? 0 : (AckResendStandOff << (_idleAckStandOff - 1)));

            if (_ackRequested || packetSent || _packetReceived) {
                // request ack to be send immediately next time
                _idleAckStandOff = 0;
                _idleAckTS = now;
                ackTS = 0;
                _ackRequested = false;
            }
            _packetReceived = false;

            if (!packetSent && (ackTS < now)) {
                if (_oldestPacketResendAckIndex >= _resendables.Count)
                    _oldestPacketResendAckIndex = 0;
                var oldestMissingPacket = -1;
                if (_resendables.Count > 0)
                    oldestMissingPacket = _resendables[_oldestPacketResendAckIndex];
                if (oldestMissingPacket == -1) {
                    var buffer = _allocate(HeaderSize);
                    buffer[0] = (byte)(_localSignature & 0xff);
                    buffer[1] = (byte)((_localSignature >> 8) & 0xff);
                    buffer[2] = (byte)((_localSignature >> 16) & 0xff);
                    buffer[3] = (byte)((_localSignature >> 24) & 0xff);
                    buffer[4] = buffer[5] = buffer[6] = buffer[7] = 255;
                    WriteAckheader(buffer);
                    if (!_transmitPacketCallback(buffer, HeaderSize))
                        _congested = true;
                    else {
                        _idleAckTS = now;

                        _idleAckStandOff++;
                    }
                    _release(buffer);
                }
                else {
                    var packet = _sendWindow[oldestMissingPacket];
                    WriteAckheader(packet.Buffer);
                    if (!_transmitPacketCallback(packet.Buffer, packet.Length)) {
                        _congested = true;
                    }
                    else {
                        packet.SendCount++;
                        _sendWindow[oldestMissingPacket] = packet;
                        _oldestPacketResendAckIndex++;
                        _idleAckTS = now;
                        _idleAckStandOff++;
                    }
                }
            }

            _idleTimeout = ((_receiveWindow.Count > 0) || (_sendWindow.Count > 0)) && (now - _lastReceived > IdleTimeout);
        }

        void WriteAckheader(byte[] buffer) {
            buffer[8] = (byte)((_receiveWindowStart >> 0) & 0xff);
            buffer[9] = (byte)((_receiveWindowStart >> 8) & 0xff);
            buffer[10] = (byte)((_receiveWindowStart >> 16) & 0xff);
            buffer[11] = (byte)((_receiveWindowStart >> 24) & 0xff);

            for (var i = 0; i < WindowAckBytesSize; ++i) {
                byte mask = 0;

                if (CanAckReceived(_receiveWindowStart + i * 8 + 0))
                    mask |= 0x01;
                if (CanAckReceived(_receiveWindowStart + i * 8 + 1))
                    mask |= 0x02;
                if (CanAckReceived(_receiveWindowStart + i * 8 + 2))
                    mask |= 0x04;
                if (CanAckReceived(_receiveWindowStart + i * 8 + 3))
                    mask |= 0x08;
                if (CanAckReceived(_receiveWindowStart + i * 8 + 4))
                    mask |= 0x10;
                if (CanAckReceived(_receiveWindowStart + i * 8 + 5))
                    mask |= 0x20;
                if (CanAckReceived(_receiveWindowStart + i * 8 + 6))
                    mask |= 0x40;
                if (CanAckReceived(_receiveWindowStart + i * 8 + 7))
                    mask |= 0x80;

                buffer[12 + i] = mask;
            }
        }

        bool CanAckReceived(int sequence) {
            if (sequence < _receiveWindowStart)
                throw new Exception("Sequence from before window start " + sequence + " " + _receiveWindowStart);
            var idx = sequence - _receiveWindowStart;
            if (idx >= _receiveWindow.Count)
                return false;
            return _receiveWindow[idx].Buffer != null;
        }

        void CheckCongested() {
            if (_sendWindow.Count > SendWindowFull)
                _congested = true;
            if (_unackedDataSize > UnackedDataLimit)
                _congested = true;
        }

        long CalcSendMoment(Packet packet, bool beyondAckHead) {
            if (packet.SendCount == 0)
                return 0;
            return packet.CreatedTS + (beyondAckHead ? InitialResendDelay : MissedResendDelay) + ResendStandOff << (packet.SendCount - 1);
        }

        // can take an arbitrarily sized message
        public void SendMessage(byte[] message, int offset, int length) {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name);
            if (message == null)
                throw new ArgumentNullException("message");
            if (offset < 0)
                throw new ArgumentOutOfRangeException("negative offset not allowed.");
            if (length < 0)
                throw new ArgumentOutOfRangeException("negative length not allowed.");
            if (offset + length > message.Length)
                throw new ArgumentOutOfRangeException("offset + length exceed message size");

            while (length > 0) {
                if (_messageBuffer == null) {
                    _messageBuffer = _allocate(OuterPacketSize);
                    _messageBufferOffset = HeaderSize;
                }
                var len = length;
                var remainder = OuterPacketSize - _messageBufferOffset;
                if (len > remainder)
                    len = remainder;
                Array.Copy(message, offset, _messageBuffer, _messageBufferOffset, len);
                offset += len;
                _messageBufferOffset += len;
                length -= len;
                if (_messageBufferOffset == OuterPacketSize)
                    FlushMessageBuffer();
            }
        }

        void FlushMessageBuffer() {
            if (_messageBuffer == null)
                return;
            if (_messageBufferOffset == HeaderSize)
                return;
            Packet packet;
            packet.SequenceId = _sendSequenceId++;
            packet.SendCount = 0;
            packet.CreatedTS = _timer.ElapsedMilliseconds + DebugElapsedTimeBias;
            packet.Buffer = _messageBuffer;
            packet.Length = _messageBufferOffset;

            packet.Buffer[0] = (byte)(_localSignature & 0xff);
            packet.Buffer[1] = (byte)((_localSignature >> 8) & 0xff);
            packet.Buffer[2] = (byte)((_localSignature >> 16) & 0xff);
            packet.Buffer[3] = (byte)((_localSignature >> 24) & 0xff);

            packet.Buffer[4] = (byte)(packet.SequenceId & 0xff);
            packet.Buffer[5] = (byte)((packet.SequenceId >> 8) & 0xff);
            packet.Buffer[6] = (byte)((packet.SequenceId >> 16) & 0xff);
            packet.Buffer[7] = (byte)((packet.SequenceId >> 24) & 0xff);

            _sendWindow.Add(packet);
            _messageBuffer = null;
            _messageBufferOffset = 0;
            _unackedDataSize += packet.Length;
            CheckCongested();
        }

        // packets need to be received whole, as sent 
        public void ReceivePacket(byte[] packet, int offset, int length) {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name);
            if (packet == null)
                throw new ArgumentNullException("message");
            if (offset < 0)
                throw new ArgumentOutOfRangeException("negative offset not allowed.");
            if (length < 0)
                throw new ArgumentOutOfRangeException("negative length not allowed.");
            if (offset + length > packet.Length)
                throw new ArgumentOutOfRangeException("offset + length exceed packet size");
            if (length == 0)
                return;
            var remoteSignature = ((uint)packet[0] | ((uint)packet[1] << 8) | ((uint)packet[2] << 16) | ((uint)packet[3] << 24));
            var sequenceId = packet[4] | (packet[5] << 8) | (packet[6] << 16) | (packet[7] << 24);
            var sendAckWindowhead = packet[8] | (packet[9] << 8) | (packet[10] << 16) | (packet[11] << 24);

            //Console.WriteLine("received:" + remoteSignature + ":" + sequenceId);
            if (!_remoteSignatureKnown) {
                if ((remoteSignature & 1) == (_localSignature & 1)) {
                    //Console.WriteLine("packet received with same low bit, dropping");
                    return; // client to client or server to server packet, drop
                }
                if ((sequenceId == 0) || ((sequenceId == -1) && (sendAckWindowhead == 0))) {
                    //Console.WriteLine("Associate remoteSignature:" + remoteSignature);
                    _remoteSignature = remoteSignature;
                    _remoteSignatureKnown = true;
                    // -1 case is not ideal cause that is a resend, but only way to move when the first packet got lost
                }
                else {
                    //Console.WriteLine("RemoteSignature not Known, sequence != 0, dropping");
                    return; // drop, not synced
                }
            }

            if (remoteSignature != _remoteSignature) {
                //Console.WriteLine("Remote signature mismatch :" + remoteSignature + " expected:" + _remoteSignature);
                return;
            }

            if (length < HeaderSize)
                throw new Exception("Packet too short. length: " + length);

            AckUpto(sendAckWindowhead);
            for (var i = 0; i < WindowAckBytesSize; ++i) {
                var ack = packet[12 + i];
                if (ack == 0)
                    continue;
                if ((ack & 0x01) != 0)
                    Ack(sendAckWindowhead + i * 8 + 0);
                if ((ack & 0x02) != 0)
                    Ack(sendAckWindowhead + i * 8 + 1);
                if ((ack & 0x04) != 0)
                    Ack(sendAckWindowhead + i * 8 + 2);
                if ((ack & 0x08) != 0)
                    Ack(sendAckWindowhead + i * 8 + 3);
                if ((ack & 0x10) != 0)
                    Ack(sendAckWindowhead + i * 8 + 4);
                if ((ack & 0x20) != 0)
                    Ack(sendAckWindowhead + i * 8 + 5);
                if ((ack & 0x40) != 0)
                    Ack(sendAckWindowhead + i * 8 + 6);
                if ((ack & 0x80) != 0)
                    Ack(sendAckWindowhead + i * 8 + 7);
            }
            if (sequenceId == -1) {
                if (length != HeaderSize)
                    throw new Exception("Header length incorrect " + length + " " + HeaderSize);
                return; // dataless ack
            }
            if (sequenceId < _receiveWindowStart)
                return; // old duplicate receive
            var ri = sequenceId - _receiveWindowStart;
            while (ri >= _receiveWindow.Count) {
                var p = new Packet();
                p.SequenceId = _receiveWindowStart + _receiveWindow.Count;
                _receiveWindow.Add(p);
            }
            var rp = _receiveWindow[ri];
            if (rp.SequenceId != sequenceId)
                throw new Exception("SequnceId mismatch " + rp.SequenceId + " " + sequenceId);
            if (rp.Buffer == null) {
                rp.Buffer = _allocate(length);
                rp.Length = length;
                if (rp.Buffer.Length < length)
                    throw new Exception("Incorrect length " + rp.Buffer.Length + " " + length);
                Array.Copy(packet, offset, rp.Buffer, 0, length);
                _receiveWindow[ri] = rp;
            }
            if (ri == 0)
                ProcessReceiveWindow();
            _ackRequested = true;
            _packetReceived = true;
            _lastReceived = _timer.ElapsedMilliseconds + DebugElapsedTimeBias;
        }

        void ProcessReceiveWindow() {
            var cleanupCount = 0;
            while (true) {
                if (_receiveWindow.Count == cleanupCount)
                    break;
                var packet = _receiveWindow[cleanupCount];
                if (packet.Buffer == null)
                    break;
                //Console.WriteLine("Received packet sequence:" + packet.SequenceId);
                _receiveMessageCallback(packet.Buffer, HeaderSize, packet.Length - HeaderSize);
                if (!_receiveMessageCallbackTakesOwnership)
                    _release(packet.Buffer);
                packet.Buffer = null;
                _receiveWindow[cleanupCount] = packet;
                cleanupCount++;
            }
            if (cleanupCount > 0) {
                _receiveWindow.RemoveRange(0, cleanupCount);
                _receiveWindowStart += cleanupCount;
            }
        }

        void Ack(int idx) {
            if (idx > _sendWindowHighestAck)
                _sendWindowHighestAck = idx;
            if (idx < _sendWindowStart) {
                //Console.WriteLine("old ack: " + idx + " window:" + _sendWindowStart);
                return;
            }
            var i = idx - _sendWindowStart;
            if (i >= _sendWindow.Count) {
                //Console.WriteLine("ack for unsent packet:" + idx + " send window:" + _sendWindowStart + ".." + (_sendWindowStart + _sendWindow.Count));
                _failure = true;
                return;
            }
            var packet = _sendWindow[i];
            if (packet.Buffer != null) {
                _release(packet.Buffer);
                packet.Buffer = null;
                _sendWindow[i] = packet;
            }

            var headCleanupCount = 0;
            while (headCleanupCount < _sendWindow.Count) {
                if (_sendWindow[headCleanupCount].Buffer != null)
                    break;
                headCleanupCount++;
            }
            if (headCleanupCount > 0) {
                _sendWindow.RemoveRange(0, headCleanupCount);
                _oldestPacketResendAckIndex -= headCleanupCount;
                if (_oldestPacketResendAckIndex < 0)
                    _oldestPacketResendAckIndex = 0;
                _sendWindowStart += headCleanupCount;
            }
        }

        void AckUpto(int idx) {
            // upto, but not including idx
            if (idx > _sendWindowStart + _sendWindow.Count) {
                //Console.WriteLine("ack for unsent packet:" + idx + " send window:" + _sendWindowStart + ".." + (_sendWindowStart + _sendWindow.Count));
                _failure = true;
                return;
            }
            while (_sendWindowStart < idx) {
                Ack(_sendWindowStart);
            }
        }

        struct Packet {
            public int SequenceId;
            public int SendCount;
            public long CreatedTS;
            public byte[] Buffer;
            public int Length;
        }
    }
}
