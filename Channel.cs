using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Plukit.ReliableEndpoint;

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
 * transmitPacketCallback(Memory<byte>) -> bool (channel congested/closed)
 * receiveMessageCallback(Memory<byte>)
 * 
 * 
 */
 
public sealed class Channel {
    const int _WindowAckSize = 256;
    const int _WindowAckBytesSize = _WindowAckSize / 8;
    const int _OuterPacketSize = 1100;
    const int _HeaderSize = 4 + 4 + 4 + _WindowAckBytesSize;

    const int _InitialResendDelay = 1000;
    const int _MissedResendDelay = 250;
    const int _ResendStandOff = 128;
    const int _AckResendStandOff = 1;

    const int _SendWindowFull = 2048;
    const int _UnackedDataLimit = 1024 * 1024;

    const int _MaxStandOff = 6;
    const int _MaxAckStandOff = 6;

    const int _IdleTimeout = 20000;
    const int _DisconnectTimeout = 3000;
    const int _DisconnectLoopsTimeoutRatio = 10;

    const int _ReSendAckOldestEveryNth = 5;
    readonly Func<Memory<byte>, bool> _transmitPacketCallback;
    readonly Action<Memory<byte>> _receiveMessageCallback;

    readonly Func<int, PacketBuffer> _allocate;
    readonly Action<PacketBuffer> _release;

    bool _disposed;

    PacketBuffer _messageBuffer;
    int _messageBufferOffset;

    int _sendWindowStart;
    int _sendWindowHighestAck;

    readonly List<Packet> _sendWindow = new();


    int _receiveWindowStart;
    readonly List<Packet> _receiveWindow = new();

    readonly Stopwatch _timer = Stopwatch.StartNew();

    int _sendSequenceId;
    int _unackedDataSize;

    bool _ackRequested;
    int _idleAckStandOff;
    long _idleAckTS;
    int _oldestPacketResendAckIndex;

    long _lastReceived;
    bool _idleTimeout;

    readonly uint _localSignature;
    uint _remoteSignature;
    bool _remoteSignatureKnown;

    bool _packetReceived;

    readonly List<int> _resendables = new();
    public long DebugElapsedTimeBias;
    bool _disconnecting;
    int _disconnectedLoops;

    public Channel(bool serverSide, Func<int, PacketBuffer> allocate, Action<PacketBuffer> release, Func<Memory<byte>, bool> transmitPacketCallback, Action<Memory<byte>> receiveMessageCallback) {
        _allocate = allocate ?? throw new ArgumentNullException(nameof(allocate));
        _release = release ?? throw new ArgumentNullException(nameof(release));
        _transmitPacketCallback = transmitPacketCallback ?? throw new ArgumentNullException(nameof(transmitPacketCallback));
        _receiveMessageCallback = receiveMessageCallback ?? throw new ArgumentNullException(nameof(receiveMessageCallback));
        var guid = Guid.NewGuid().ToByteArray();
        var stamp = guid[0] ^ (guid[1] << 8) ^ (guid[2] << 16) ^ (guid[3] << 24)
            ^ guid[4] ^ (guid[5] << 8) ^ (guid[6] << 16) ^ (guid[7] << 24)
            ^ guid[8] ^ (guid[9] << 8) ^ (guid[10] << 16) ^ (guid[11] << 24)
            ^ guid[12] ^ (guid[13] << 8) ^ (guid[14] << 16) ^ (guid[15] << 24);
        //signature lsb encodes if the connection is server or clientsided, remote side must use a signature with the lowest bit flipped
        var signature = ((uint)(stamp & 0x7fffffff) << 1) | (serverSide ? 1u : 0u);

        _lastReceived = _timer.ElapsedMilliseconds + DebugElapsedTimeBias;
        _localSignature = signature;
    }

    public bool Congested { get; private set; }

    public bool Timeout => _idleTimeout && !_disconnecting;

    public bool Failure { get; private set; }

    public bool Disconnected => Failure || _idleTimeout;

    public bool Disconnecting => Disconnected || _disconnecting;

    public void Dispose() {
        if (_disposed)
            throw new ObjectDisposedException(GetType().Name);
        _disposed = true;
        Failure = true;

        _resendables.Clear();

        for (var i = 0; i < _sendWindow.Count; ++i)
            _release(_sendWindow[i].Buffer);
        _sendWindow.Clear();
        for (var i = 0; i < _receiveWindow.Count; ++i)
            _release(_receiveWindow[i].Buffer);
        _receiveWindow.Clear();
    }

    public void Update() {
        if (_disposed)
            throw new ObjectDisposedException(GetType().Name);

        if (Disconnected)
            return;

        FlushMessageBuffer();
        Congested = false;
        var now = _timer.ElapsedMilliseconds + DebugElapsedTimeBias;
        var packetSent = false;
        if (!Disconnecting) {
            _unackedDataSize = 0;
            _resendables.Clear();
            for (var i = 0; i < _sendWindow.Count; ++i) {
                var packet = _sendWindow[i];
                if (!packet.Buffer.Memory.IsEmpty) {
                    var nextSend = CalcSendMoment(packet, (i + _sendWindowStart) > _sendWindowHighestAck);
                    _unackedDataSize += packet.Length;
                    if (((i < _WindowAckSize) || (packet.SendCount == 0)) && (nextSend <= now)) {
                        WriteAckheader(packet.Buffer.Memory.Span);
                        if (!_transmitPacketCallback(packet.Buffer.Memory[..packet.Length])) {
                            Congested = true;
                            break;
                        }
                        packetSent = true;

                        packet.SendCount++;
                        if (packet.SendCount > _MaxStandOff)
                            packet.SendCount = _MaxStandOff;
                        _sendWindow[i] = packet;
                    }
                    else {
                        if (_resendables.Count < _ReSendAckOldestEveryNth)
                            _resendables.Add(i);
                    }
                }
            }
            CheckCongested();
        }

        if (_idleAckStandOff > _MaxAckStandOff)
            _idleAckStandOff = _MaxAckStandOff;

        var ackTS = _idleAckTS + (_idleAckStandOff == 0 ? 0 : _AckResendStandOff << (_idleAckStandOff - 1));

        if (_ackRequested || packetSent || _packetReceived) {
            // request ack to be send immediately next time
            _idleAckStandOff = 0;
            _idleAckTS = now;
            ackTS = 0;
            _ackRequested = false;
        }
        _packetReceived = false;

        if (Disconnecting || (!packetSent && (ackTS < now))) {
            if (_oldestPacketResendAckIndex >= _resendables.Count)
                _oldestPacketResendAckIndex = 0;
            var oldestMissingPacket = -1;
            if (_resendables.Count > 0)
                oldestMissingPacket = _resendables[_oldestPacketResendAckIndex];
            var sendMetaPacket = Disconnecting || (oldestMissingPacket == -1);
            if (sendMetaPacket) {
                var bufferSize = _HeaderSize + 2;
                var packet = _allocate(bufferSize);
                var buffer = packet.Memory.Span;
                buffer[0] = (byte)(_localSignature & 0xff);
                buffer[1] = (byte)((_localSignature >> 8) & 0xff);
                buffer[2] = (byte)((_localSignature >> 16) & 0xff);
                buffer[3] = (byte)((_localSignature >> 24) & 0xff);
                buffer[4] = buffer[5] = buffer[6] = buffer[7] = 255;
                WriteAckheader(buffer);
                var messageSize = _HeaderSize;
                if (Disconnecting) {
                    buffer[messageSize++] = 99; // 'c' close
                    buffer[messageSize++] = (byte)(_disconnectedLoops > 255 ? 255 : _disconnectedLoops);
                }
                if (!_transmitPacketCallback(packet.Memory[..messageSize])) {
                    Congested = true;
                }
                else {
                    _idleAckTS = now;

                    _idleAckStandOff++;
                }
                _release(packet);
            }
            else {
                var packet = _sendWindow[oldestMissingPacket];
                WriteAckheader(packet.Buffer.Memory.Span);
                if (!_transmitPacketCallback(packet.Buffer.Memory[..packet.Length])) {
                    Congested = true;
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

        var idleTimeout = _IdleTimeout;
        if (_disconnecting) {
            if (_disconnectedLoops >= _DisconnectLoopsTimeoutRatio)
                idleTimeout = 0;
            else
                idleTimeout = _DisconnectTimeout * (_DisconnectLoopsTimeoutRatio - _disconnectedLoops) / _DisconnectLoopsTimeoutRatio;
        }
        _idleTimeout = (now - _lastReceived) > idleTimeout;
        if (_idleTimeout) {
            //Console.WriteLine("_idleTimeout: " + _idleTimeout + " rw: " + _receiveWindow.Count + " sw: " + _sendWindow.Count + " now: " + now + " _lastReceived: " + _lastReceived+" _disconnected: "+_disconnected);
        }
    }

    void WriteAckheader(Span<byte> buffer) {
        buffer[8] = (byte)((_receiveWindowStart >> 0) & 0xff);
        buffer[9] = (byte)((_receiveWindowStart >> 8) & 0xff);
        buffer[10] = (byte)((_receiveWindowStart >> 16) & 0xff);
        buffer[11] = (byte)((_receiveWindowStart >> 24) & 0xff);

        for (var i = 0; i < _WindowAckBytesSize; ++i) {
            byte mask = 0;

            if (CanAckReceived(_receiveWindowStart + (i * 8) + 0))
                mask |= 0x01;
            if (CanAckReceived(_receiveWindowStart + (i * 8) + 1))
                mask |= 0x02;
            if (CanAckReceived(_receiveWindowStart + (i * 8) + 2))
                mask |= 0x04;
            if (CanAckReceived(_receiveWindowStart + (i * 8) + 3))
                mask |= 0x08;
            if (CanAckReceived(_receiveWindowStart + (i * 8) + 4))
                mask |= 0x10;
            if (CanAckReceived(_receiveWindowStart + (i * 8) + 5))
                mask |= 0x20;
            if (CanAckReceived(_receiveWindowStart + (i * 8) + 6))
                mask |= 0x40;
            if (CanAckReceived(_receiveWindowStart + (i * 8) + 7))
                mask |= 0x80;

            buffer[12 + i] = mask;
        }
    }

    bool CanAckReceived(int sequence) {
        if (sequence < _receiveWindowStart)
            throw new("Sequence from before window start " + sequence + " " + _receiveWindowStart);
        var idx = sequence - _receiveWindowStart;
        if (idx >= _receiveWindow.Count)
            return false;
        return !_receiveWindow[idx].Buffer.Memory.IsEmpty;
    }

    void CheckCongested() {
        if (_sendWindow.Count > _SendWindowFull)
            Congested = true;
        if (_unackedDataSize > _UnackedDataLimit)
            Congested = true;
    }

    static long CalcSendMoment(Packet packet, bool beyondAckHead) {
        if (packet.SendCount == 0)
            return 0;
        return (packet.CreatedTS + (beyondAckHead ? _InitialResendDelay : _MissedResendDelay) + _ResendStandOff) << (packet.SendCount - 1);
    }

    // can take an arbitrarily sized message
    public void SendMessage(Span<byte> message) {
        if (_disposed)
            throw new ObjectDisposedException(GetType().Name);

        if (Disconnecting || Disconnected)
            return;

        var length = message.Length;
        var offset = 0;

        while (length > 0) {
            if (_messageBuffer.Memory.IsEmpty) {
                _messageBuffer = _allocate(_OuterPacketSize);
                _messageBufferOffset = _HeaderSize;
            }
            var len = length;
            var remainder = _OuterPacketSize - _messageBufferOffset;
            if (len > remainder)
                len = remainder;
            message.Slice(offset, len).CopyTo(_messageBuffer.Memory.Slice(_messageBufferOffset, len).Span);
            offset += len;
            _messageBufferOffset += len;
            length -= len;
            if (_messageBufferOffset == _OuterPacketSize)
                FlushMessageBuffer();
        }
    }

    void FlushMessageBuffer() {
        if (_messageBuffer.Memory.IsEmpty)
            return;
        if (_messageBufferOffset == _HeaderSize)
            return;
        Packet packet;
        packet.SequenceId = _sendSequenceId++;
        packet.SendCount = 0;
        packet.CreatedTS = _timer.ElapsedMilliseconds + DebugElapsedTimeBias;
        packet.Buffer = _messageBuffer;
        packet.Length = _messageBufferOffset;

        var buffer = packet.Buffer.Memory.Span;

        buffer[0] = (byte)(_localSignature & 0xff);
        buffer[1] = (byte)((_localSignature >> 8) & 0xff);
        buffer[2] = (byte)((_localSignature >> 16) & 0xff);
        buffer[3] = (byte)((_localSignature >> 24) & 0xff);

        buffer[4] = (byte)(packet.SequenceId & 0xff);
        buffer[5] = (byte)((packet.SequenceId >> 8) & 0xff);
        buffer[6] = (byte)((packet.SequenceId >> 16) & 0xff);
        buffer[7] = (byte)((packet.SequenceId >> 24) & 0xff);

        _sendWindow.Add(packet);
        _messageBuffer = default;
        _messageBufferOffset = 0;
        _unackedDataSize += packet.Length;
        CheckCongested();
    }

    public void Close() {
        _disconnecting = true;
    }

    // packets need to be received whole, as sent 
    public void ReceivePacket(Span<byte> packet) {
        if (_disposed)
            throw new ObjectDisposedException(GetType().Name);

        var remoteSignature = packet[0] | ((uint)packet[1] << 8) | ((uint)packet[2] << 16) | ((uint)packet[3] << 24);
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

        var length = packet.Length;

        if (length < _HeaderSize)
            throw new("Packet too short. length: " + length);

        AckUpto(sendAckWindowhead);
        for (var i = 0; i < _WindowAckBytesSize; ++i) {
            var ack = packet[12 + i];
            if (ack == 0)
                continue;
            if ((ack & 0x01) != 0)
                Ack(sendAckWindowhead + (i * 8) + 0);
            if ((ack & 0x02) != 0)
                Ack(sendAckWindowhead + (i * 8) + 1);
            if ((ack & 0x04) != 0)
                Ack(sendAckWindowhead + (i * 8) + 2);
            if ((ack & 0x08) != 0)
                Ack(sendAckWindowhead + (i * 8) + 3);
            if ((ack & 0x10) != 0)
                Ack(sendAckWindowhead + (i * 8) + 4);
            if ((ack & 0x20) != 0)
                Ack(sendAckWindowhead + (i * 8) + 5);
            if ((ack & 0x40) != 0)
                Ack(sendAckWindowhead + (i * 8) + 6);
            if ((ack & 0x80) != 0)
                Ack(sendAckWindowhead + (i * 8) + 7);
        }
        if (sequenceId != -1) {
            if (sequenceId >= _receiveWindowStart) {
                var ri = sequenceId - _receiveWindowStart;
                while (ri >= _receiveWindow.Count) {
                    var p = new Packet {
                        SequenceId = _receiveWindowStart + _receiveWindow.Count
                    };
                    _receiveWindow.Add(p);
                }
                var rp = _receiveWindow[ri];
                if (rp.SequenceId != sequenceId)
                    throw new("SequnceId mismatch " + rp.SequenceId + " " + sequenceId);
                if (rp.Buffer.Memory.IsEmpty) {
                    rp.Buffer = _allocate(length);
                    rp.Length = length;
                    if (rp.Buffer.Memory.Length < length)
                        throw new("Incorrect length " + rp.Buffer.Memory.Length + " " + length);
                    packet.CopyTo(rp.Buffer.Memory[..length].Span);
                    _receiveWindow[ri] = rp;
                }
                if (ri == 0)
                    ProcessReceiveWindow();
            }
        }
        else {
            var commandOffset = _HeaderSize;
            while (commandOffset < length) {
                switch (packet[commandOffset + 0]) {
                    case 99: // 'c' close command
                        commandOffset++;
                        if (commandOffset == length)
                            break;
                        var loops = packet[commandOffset];
                        if (!_disconnecting)
                            _lastReceived = _timer.ElapsedMilliseconds + DebugElapsedTimeBias;
                        _disconnecting = true;
                        if (loops >= _disconnectedLoops)
                            _disconnectedLoops = loops + 1;
                        commandOffset++;
                        continue;
                    default:
                        throw new("Packet with unknown meta command: " + packet[_HeaderSize + 0]);
                }
                throw new("Meta command incorrect length " + length + " " + _HeaderSize + " " + commandOffset);
            }
        }
        _ackRequested = true;
        _packetReceived = true;
        if (!_disconnecting)
            _lastReceived = _timer.ElapsedMilliseconds + DebugElapsedTimeBias;
    }

    void ProcessReceiveWindow() {
        var cleanupCount = 0;
        while (true) {
            if (_receiveWindow.Count == cleanupCount)
                break;
            var packet = _receiveWindow[cleanupCount];
            if (packet.Buffer.Memory.IsEmpty)
                break;
            //Console.WriteLine("Received packet sequence:" + packet.SequenceId);
            _receiveMessageCallback(packet.Buffer.Memory[_HeaderSize..packet.Length]);
            _release(packet.Buffer);
            packet.Buffer = default;
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
            Failure = true;
            return;
        }
        var packet = _sendWindow[i];
        if (!packet.Buffer.Memory.IsEmpty) {
            _release(packet.Buffer);
            packet.Buffer = default;
            _sendWindow[i] = packet;
        }

        var headCleanupCount = 0;
        while (headCleanupCount < _sendWindow.Count) {
            if (!_sendWindow[headCleanupCount].Buffer.Memory.IsEmpty)
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
        if (idx > (_sendWindowStart + _sendWindow.Count)) {
            //Console.WriteLine("ack for unsent packet:" + idx + " send window:" + _sendWindowStart + ".." + (_sendWindowStart + _sendWindow.Count));
            Failure = true;
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
        public PacketBuffer Buffer;
        public int Length;
    }
}