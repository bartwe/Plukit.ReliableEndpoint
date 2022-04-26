using System;

namespace Plukit.ReliableEndpoint;

public struct PacketBuffer {
    public Memory<byte> Memory;
    public int Handle;
}