using System;
using System.Collections.Generic;
using System.Threading;

namespace Plukit.ReliableEndpoint {
    class TestC {
        // out of order/duplicate/dropped delivery
        public static void Run() {
            AChannel = new Channel(true, Allocator, Release, TransmitPacketA, ReceiveMessageA);
            BChannel = new Channel(false, Allocator, Release, TransmitPacketB, ReceiveMessageB);


            for (int i = 0; i < 1024 * 1024; ++i) {
                var b = new byte[1];
                b[0] = (byte)(i & 0x7f);
                AChannel.SendMessage(b, 0, 1);
                b[0] = (byte)(i & 0x7f | 0x80);
                BChannel.SendMessage(b, 0, 1);
            }

            for (int i = 0; i < 10000; ++i) {
                AChannel.Update();
                BChannel.Update();


                var bias = Random.Next(1000);
                AChannel.DebugElapsedTimeBias += bias;
                BChannel.DebugElapsedTimeBias += bias;


                if (APacketBuffer.Count > 0) {
                    var a = Random.Next(APacketBuffer.Count);
                    var act = Random.Next(2);
                    if (act == 0) {
                        var buffer = APacketBuffer[a];
                        AChannel.ReceivePacket(buffer, 0 , buffer.Length);
                    }
                    else
                        APacketBuffer.RemoveAt(a);
                }

                if (APacketBuffer.Count  >1000) {
                    APacketBuffer.Clear();
                }

                if (BPacketBuffer.Count > 0) {
                    var a = Random.Next(BPacketBuffer.Count);
                    var act = Random.Next(2);
                    if (act == 0) {
                        var buffer = BPacketBuffer[a];
                        BChannel.ReceivePacket(buffer, 0, buffer.Length);
                    }
                    else
                        BPacketBuffer.RemoveAt(a);
                }

                if (BPacketBuffer.Count > 1000) {
                    BPacketBuffer.Clear();
                }
            }


            var ac = 0;
            foreach (var a in AReceived) {
                for (int i = 0; i < a.Length; ++i) {
                    var x = ac + i;
                    var y = (byte)(x & 0x7f | 0x80);
                    if (a[i] != y)
                        throw new Exception();
                }
                ac += a.Length;
            }

            var bc = 0;
            foreach (var b in BReceived) {
                for (int i = 0; i < b.Length; ++i) {
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
            for (int i = 0; i < length; ++i)
                b[i] = buffer[offset + i];
            BReceived.Add(b);
        }

        static void ReceiveMessageA(byte[] buffer, int offset, int length) {
            var b = new byte[length];
            for (int i = 0; i < length; ++i)
                b[i] = buffer[offset + i];
            AReceived.Add(b);
        }

        static bool TransmitPacketB(byte[] buffer, int length) {
            var b = new byte[length];
            for (int i = 0; i < length; ++i)
                b[i] = buffer[i];
            APacketBuffer.Add(b);
            return true;
        }

        static bool TransmitPacketA(byte[] buffer, int length) {
            var b = new byte[length];
            for (int i = 0; i < length; ++i)
                b[i] = buffer[i];
            BPacketBuffer.Add(b);
            return true;
        }

        static void Release(byte[] obj) {
            for (int i = 0; i < obj.Length; ++i)
                obj[i] = 0xff;
        }

        static byte[] Allocator(int length) {
            return new byte[length];
        }

        public static Random Random = new Random();
        public static Channel AChannel;
        public static Channel BChannel;
        public static List<byte[]> AReceived = new List<byte[]>();
        public static List<byte[]> BReceived = new List<byte[]>();
        public static List<byte[]> APacketBuffer = new List<byte[]>();
        public static List<byte[]> BPacketBuffer = new List<byte[]>();
    }
}