#nullable disable
using System;
using System.Collections.Generic;
using System.Threading;

namespace Plukit.ReliableEndpoint {
    public class TestC {
        // out of order/duplicate/dropped delivery
        public static void Run() {
            AChannel = new Channel(true, Allocator, Release, TransmitPacketA, ReceiveMessageA);
            BChannel = new Channel(false, Allocator, Release, TransmitPacketB, ReceiveMessageB);


            for (var i = 0; i < 1024 * 1024; ++i) {
                var b = new byte[1];
                b[0] = (byte)(i & 0x7f);
                AChannel.SendMessage(new(b, 0, 1));
                b[0] = (byte)(i & 0x7f | 0x80);
                BChannel.SendMessage(new(b, 0, 1));
            }

            for (var c = 0; ; ++c) {

                for (var i = 0; i < 20; ++i) {
                    AChannel.Update();
                    BChannel.Update();

                    //    Thread.Sleep(1);


                    var bias = Random.Next(1000);
                    AChannel.DebugElapsedTimeBias += bias;
                    BChannel.DebugElapsedTimeBias += bias;


                    if (APacketBuffer.Count > 1000) {
                        APacketBuffer.Clear();
                    }
                    if (BPacketBuffer.Count > 1000) {
                        BPacketBuffer.Clear();
                    }
                    while ((APacketBuffer.Count > 0) || (BPacketBuffer.Count > 0)) {
                        if (APacketBuffer.Count > 0) {
                            var a = Random.Next(Math.Min(16, APacketBuffer.Count));
                            var act = Random.Next(2);
                            if (act == 0) {
                                var buffer = APacketBuffer[a];
                                AChannel.ReceivePacket(new(buffer, 0, buffer.Length));
                            }
                            else
                                APacketBuffer.RemoveAt(a);
                        }

                        if (BPacketBuffer.Count > 0) {
                            var a = Random.Next(Math.Min(16, BPacketBuffer.Count));
                            var act = Random.Next(2);
                            if (act == 0) {
                                var buffer = BPacketBuffer[a];
                                BChannel.ReceivePacket(new(buffer, 0, buffer.Length));
                            }
                            else
                                BPacketBuffer.RemoveAt(a);
                        }
                    }
                }

                var ac = 0;
                foreach (var a in AReceived) {
                    for (var i = 0; i < a.Length; ++i) {
                        var x = ac + i;
                        var y = (byte)(x & 0x7f | 0x80);
                        if (a[i] != y)
                            throw new Exception();
                    }
                    ac += a.Length;
                }

                var bc = 0;
                foreach (var b in BReceived) {
                    for (var i = 0; i < b.Length; ++i) {
                        var x = bc + i;
                        var y = (byte)(x & 0x7f);
                        if (b[i] != y)
                            throw new Exception();
                    }
                    bc += b.Length;
                }

                if ((ac == 1024 * 1024) && (bc == 1024 * 1024))
                    break;

                if (c == 50) {
                    throw new Exception("Not all data was sent/received " + ac + " " + bc);
                }
            }
        }

        static void ReceiveMessageB(Memory<byte> buffer) {
            BReceived.Add(buffer.ToArray());
        }

        static void ReceiveMessageA(Memory<byte> buffer) {
            AReceived.Add(buffer.ToArray());
        }

        static bool TransmitPacketB(Memory<byte> buffer) {
            APacketBuffer.Add(buffer.ToArray());
            return true;
        }

        static bool TransmitPacketA(Memory<byte> buffer) {
            BPacketBuffer.Add(buffer.ToArray());
            return true;
        }

        static void Release(PacketBuffer obj) {
            var span = obj.Memory.Span;
            for (var i = 0; i < span.Length; ++i)
                span[i] = 0xff;
        }

        static PacketBuffer Allocator(int length) {
            return new() { Handle = 1, Memory = new(new byte[length]) };
        }

        public static Random Random = new Random();
        public static Channel AChannel;
        public static Channel BChannel;
        public static List<byte[]> AReceived = new();
        public static List<byte[]> BReceived = new();
        public static List<byte[]> APacketBuffer = new List<byte[]>();
        public static List<byte[]> BPacketBuffer = new List<byte[]>();
    }
}
