"""Relay framed Ethernet packets between QEMU guests over host loopback.

Background
----------
QEMU's socket network backend can connect multiple guests via a common
TCP server.  Each guest opens a TCP connection and sends/receives raw
Ethernet frames.  Frames are prefixed with a 4-byte big-endian length
so the receiver knows how many bytes to read for each frame.

This script acts as a simple Ethernet hub: every frame arriving from
one guest is forwarded to all other connected guests.  It is used to
provide the private cluster network (192.168.89.1x/24) between cosi-1,
cosi-2 and cosi-3, because QEMU's multicast socket mode does not work
reliably over the macOS loopback interface.
"""
import asyncio
import struct

# The switch listens only on the loopback address so it is not reachable
# from outside the Mac.  All QEMU guests connect to this same port.
HOST = "127.0.0.1"
PORT = 23890

# Maximum accepted Ethernet frame size.  Frames larger than this are
# considered malformed and cause the peer connection to be dropped.
MAX_FRAME = 65536


async def main():
    # Set of active asyncio StreamWriter objects, one per connected QEMU NIC.
    peers = set()

    async def connect(reader, writer):
        # Register the new guest NIC as a peer on the virtual switch.
        peers.add(writer)
        try:
            while True:
                # QEMU's socket backend prefixes each Ethernet frame with its
                # length as an unsigned 32-bit integer in network byte order.
                header = await reader.readexactly(4)
                (size,) = struct.unpack("!I", header)
                if not 14 <= size <= MAX_FRAME:
                    break
                # Re-attach the 4-byte length prefix so each recipient can
                # read the frame with the same framing format QEMU expects.
                frame = header + await reader.readexactly(size)
                # Broadcast to every other peer that is still connected.
                # This matches Ethernet hub semantics (not a learning switch).
                recipients = [peer for peer in peers if peer != writer and not peer.is_closing()]
                for peer in recipients:
                    peer.write(frame)
                # Flush all recipients concurrently; ignore individual errors
                # so a slow or dead peer does not block the others.
                await asyncio.gather(*(peer.drain() for peer in recipients), return_exceptions=True)
        except (asyncio.IncompleteReadError, ConnectionError):
            # Normal disconnection: the guest was shut down or the QEMU process ended.
            pass
        finally:
            # Deregister the peer so future frames are not forwarded to its closed writer.
            peers.discard(writer)
            writer.close()

    async with await asyncio.start_server(connect, HOST, PORT) as server:
        # Print exactly this string so vms.sh can detect that the switch is ready.
        print(f"Listening on {HOST}:{PORT}", flush=True)
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
