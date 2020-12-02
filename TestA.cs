using System;
using System.Collections.Generic;

namespace Plukit.ReliableEndpoint {
    class TestA {
        // identity test, no unreliability at all
        public static void Run() {
            AChannel = new Channel(true, Allocator, Release, TransmitPacketA, ReceiveMessageA, false);
            BChannel = new Channel(false, Allocator, Release, TransmitPacketB, ReceiveMessageB, false);


            for (var i = 0; i < 1024 * 1024; ++i) {
                var b = new byte[1];
                b[0] = (byte)(i & 0x7f);
                AChannel.SendMessage(b, 0, 1);
                b[0] = (byte)(i & 0x7f | 0x80);
                BChannel.SendMessage(b, 0, 1);
            }

            for (var i = 0; i < 1000; ++i) {
                AChannel.Update();
                BChannel.Update();
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

            if (ac != 1024 * 1024)
                throw new Exception();
            if (bc != 1024 * 1024)
                throw new Exception();
        }

        static void ReceiveMessageB(byte[] buffer, int offset, int length) {
            var b = new byte[length];
            for (var i = 0; i < length; ++i)
                b[i] = buffer[offset + i];
            BReceived.Add(b);
        }

        static void ReceiveMessageA(byte[] buffer, int offset, int length) {
            var b = new byte[length];
            for (var i = 0; i < length; ++i)
                b[i] = buffer[offset + i];
            AReceived.Add(b);
        }

        static bool TransmitPacketB(byte[] buffer, int length) {
            AChannel.ReceivePacket(buffer, 0, length);
            return true;
        }

        static bool TransmitPacketA(byte[] buffer, int length) {
            BChannel.ReceivePacket(buffer, 0, length);
            return true;
        }

        static void Release(byte[] obj) {
            for (var i = 0; i < obj.Length; ++i)
                obj[i] = 0xff;
        }

        static byte[] Allocator(int length) {
            return new byte[length];
        }

        public static Channel AChannel;
        public static Channel BChannel;
        public static List<byte[]> AReceived = new List<byte[]>();
        public static List<byte[]> BReceived = new List<byte[]>();
    }
}
