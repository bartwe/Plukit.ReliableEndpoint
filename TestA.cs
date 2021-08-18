#nullable disable
using System;
using System.Collections.Generic;

namespace Plukit.ReliableEndpoint {
    public sealed class TestA {
        public static Channel AChannel;
        public static Channel BChannel;
        public static List<byte[]> AReceived = new();

        public static List<byte[]> BReceived = new();

        // identity test, no unreliability at all
        public static void Run() {
            AChannel = new(true, Allocator, Release, TransmitPacketA, ReceiveMessageA);
            BChannel = new(false, Allocator, Release, TransmitPacketB, ReceiveMessageB);


            for (var i = 0; i < (1024 * 1024); ++i) {
                var b = new byte[1];
                b[0] = (byte)(i & 0x7f);
                AChannel.SendMessage(new(b, 0, 1));
                b[0] = (byte)((i & 0x7f) | 0x80);
                BChannel.SendMessage(new(b, 0, 1));
            }

            for (var i = 0; i < 1000; ++i) {
                AChannel.Update();
                BChannel.Update();
            }


            var ac = 0;
            foreach (var a in AReceived) {
                for (var i = 0; i < a.Length; ++i) {
                    var x = ac + i;
                    var y = (byte)((x & 0x7f) | 0x80);
                    if (a[i] != y)
                        throw new();
                }
                ac += a.Length;
            }

            var bc = 0;
            foreach (var b in BReceived) {
                for (var i = 0; i < b.Length; ++i) {
                    var x = bc + i;
                    var y = (byte)(x & 0x7f);
                    if (b[i] != y)
                        throw new();
                }
                bc += b.Length;
            }

            if (ac != (1024 * 1024))
                throw new();
            if (bc != (1024 * 1024))
                throw new();
        }

        static void ReceiveMessageB(Memory<byte> buffer) {
            BReceived.Add(buffer.ToArray());
        }

        static void ReceiveMessageA(Memory<byte> buffer) {
            AReceived.Add(buffer.ToArray());
        }

        static bool TransmitPacketB(Memory<byte> buffer) {
            AChannel.ReceivePacket(buffer.Span);
            return true;
        }

        static bool TransmitPacketA(Memory<byte> buffer) {
            BChannel.ReceivePacket(buffer.Span);
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
    }
}
