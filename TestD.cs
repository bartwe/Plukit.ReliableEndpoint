#nullable disable
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Plukit.ReliableEndpoint;

public sealed class TestD {
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

    // Speed at different packetloss percentages
    // packetLossPercentage [0..100]
    public static void Run() {
        Console.WriteLine("Testing performance with packetloss");
        for (var i = 0; i <= 100; i = (i * 2) + 1) {
            var sw = Stopwatch.StartNew();
            var loopCount = Run(i);
            Console.WriteLine(i + "% loss. duration(ms) " + sw.ElapsedMilliseconds + " packets:" + loopCount);
        }
    }

    public static int Run(int packetLossPercentage) {
        var result = 0;
        var sendBytes = 1024 * 10240;

        Random = new(0);

        AReceived = new();
        BReceived = new();
        APacketBuffer = new();
        BPacketBuffer = new();

        AChannel = new(true, Allocator, Release, TransmitPacketA, ReceiveMessageA);
        BChannel = new(false, Allocator, Release, TransmitPacketB, ReceiveMessageB);

        LineLagMin = 300;
        LineLagMax = 400;

        Stopwatch = Stopwatch.StartNew();

        var bytesPerMillisecond = 1000;

        var sent = 0;
        var lc = 0;

        while (Stopwatch.ElapsedMilliseconds < 200) {
            for (; (sent < sendBytes) && (sent < (bytesPerMillisecond * Stopwatch.ElapsedMilliseconds)); ++sent) {
                var b = new byte[1];
                b[0] = (byte)(sent & 0x7f);
                AChannel.SendMessage(new(b, 0, 1));
                b[0] = (byte)((sent & 0x7f) | 0x80);
                BChannel.SendMessage(new(b, 0, 1));
            }

            Thread.Sleep(1);

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

            var c = 0;
            while (APacketBuffer.Count > 0) {
                if (APacketBuffer.Peek().Key > Stopwatch.ElapsedMilliseconds)
                    break;
                c++;
                var buffer = APacketBuffer.Dequeue().Value;
                result += buffer.Length;
                var act = Random.Next(100);
                if (act >= packetLossPercentage) {
                    AChannel.ReceivePacket(new(buffer, 0, buffer.Length));
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
                    BChannel.ReceivePacket(new(buffer, 0, buffer.Length));
                }
            }
            //                Console.WriteLine(c);
            lc++;
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
        return lc;
    }

    static void ReceiveMessageB(Memory<byte> buffer) {
        BReceived.Add(buffer.ToArray());
    }

    static void ReceiveMessageA(Memory<byte> buffer) {
        AReceived.Add(buffer.ToArray());
    }

    static bool TransmitPacketB(Memory<byte> buffer) {
        APacketBuffer.Enqueue(new(Stopwatch.ElapsedMilliseconds + Random.Next(LineLagMin, LineLagMax), buffer.ToArray()));
        return true;
    }

    static bool TransmitPacketA(Memory<byte> buffer) {
        BPacketBuffer.Enqueue(new(Stopwatch.ElapsedMilliseconds + Random.Next(LineLagMin, LineLagMax), buffer.ToArray()));
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
