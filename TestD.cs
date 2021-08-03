#nullable disable
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Plukit.ReliableEndpoint {
    class TestD {
        // Speed at different packetloss percentages
        // packetLossPercentage [0..100]
        public static void Run() {
            Console.WriteLine("Testing performance with packetloss");
            for (var i = 0; i <= 100; i = i * 2 + 1) {
                var sw = Stopwatch.StartNew();
                var loopCount = Run(i);
                Console.WriteLine(i + "% loss. duration(ms) " + sw.ElapsedMilliseconds + " packets:" + loopCount);
            }
            Console.ReadLine();
        }


        public static int Run(int packetLossPercentage) {
            var result = 0;
            var sendBytes = 1024 * 10240;

            Random = new Random(0);

            AReceived = new List<byte[]>();
            BReceived = new List<byte[]>();
            APacketBuffer = new Queue<KeyValuePair<long, byte[]>>();
            BPacketBuffer = new Queue<KeyValuePair<long, byte[]>>();

            AChannel = new Channel(true, Allocator, Release, TransmitPacketA, ReceiveMessageA, false);
            BChannel = new Channel(false, Allocator, Release, TransmitPacketB, ReceiveMessageB, false);

            LineLagMin = 300;
            LineLagMax = 400;

            Stopwatch = Stopwatch.StartNew();

            var bytesPerMillisecond = 1000;

            int sent = 0;

            while (true) {

                for (; sent < sendBytes && sent < bytesPerMillisecond * Stopwatch.ElapsedMilliseconds; ++sent) {
                    var b = new byte[1];
                    b[0] = (byte)(sent & 0x7f);
                    AChannel.SendMessage(b, 0, 1);
                    b[0] = (byte)(sent & 0x7f | 0x80);
                    BChannel.SendMessage(b, 0, 1);
                }

                Thread.Sleep(10);

                AChannel.Update();
                BChannel.Update();

                // kinda need this to live... ;)
//                if ((sent == sendBytes ) && (AChannel.DebugIdle() && BChannel.DebugIdle()))
  //                  break;

                /*
                var bias = Random.Next(1000);
                AChannel.DebugElapsedTimeBias += bias;
                BChannel.DebugElapsedTimeBias += bias;
                */

                int c= 0;
                while (APacketBuffer.Count > 0) {
                    if (APacketBuffer.Peek().Key > Stopwatch.ElapsedMilliseconds)
                        break;
                    c++;
                    var buffer = APacketBuffer.Dequeue().Value;
                    result+=buffer.Length;
                    var act = Random.Next(100);
                    if (act >= packetLossPercentage) {
                        AChannel.ReceivePacket(buffer, 0, buffer.Length);
                    }
                }
                while (BPacketBuffer.Count > 0) {
                    if (BPacketBuffer.Peek().Key > Stopwatch.ElapsedMilliseconds)
                        break;
                    c++;
                    var buffer = BPacketBuffer.Dequeue().Value;
                    result += buffer.Length;
                    var act = Random.Next(100);
                    if (act >= packetLossPercentage) {
                        BChannel.ReceivePacket(buffer, 0, buffer.Length);
                    }
                }
//                Console.WriteLine(c);
            }

            /*
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

            if (ac != sendBytes)
                throw new Exception();
            if (bc != sendBytes)
                throw new Exception();
            */
            /*
            Console.WriteLine(
            "A headerbytes: "+AChannel.headerbytes+
            " contentbytes: "+AChannel.contentbytes+
            " resendbytes: "+AChannel.resendbytes+
            " ackpacketbytes: "+AChannel.ackpacketbytes+
            " forceresendbytes: "+AChannel.forceresendbytes);

            Console.WriteLine(
            "B headerbytes: " + BChannel.headerbytes +
            " contentbytes: " + BChannel.contentbytes +
            " resendbytes: " + BChannel.resendbytes +
            " ackpacketbytes: " + BChannel.ackpacketbytes +
            " forceresendbytes: " + BChannel.forceresendbytes); return result;
             */
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
            var b = new byte[length];
            for (var i = 0; i < length; ++i)
                b[i] = buffer[i];
            APacketBuffer.Enqueue(new KeyValuePair<long, byte[]>(Stopwatch.ElapsedMilliseconds + Random.Next(LineLagMin, LineLagMax), b));
            return true;
        }

        static bool TransmitPacketA(byte[] buffer, int length) {
            var b = new byte[length];
            for (var i = 0; i < length; ++i)
                b[i] = buffer[i];
            BPacketBuffer.Enqueue(new KeyValuePair<long, byte[]>(Stopwatch.ElapsedMilliseconds + Random.Next(LineLagMin, LineLagMax), b));
            return true;
        }

        static void Release(byte[] obj) {
            for (var i = 0; i < obj.Length; ++i)
                obj[i] = 0xff;
        }

        static byte[] Allocator(int length) {
            return new byte[length];
        }

        public static Random Random;
        public static Stopwatch Stopwatch;
        public static Channel AChannel;
        public static Channel BChannel;
        public static List<byte[]> AReceived;
        public static List<byte[]> BReceived;
        public static Queue<KeyValuePair<long, byte[]>> APacketBuffer;
        public static Queue<KeyValuePair<long, byte[]>> BPacketBuffer;

        public static int LineLagMin;
        public static int LineLagMax;
    }
}
