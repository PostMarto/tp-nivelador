import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket, size):
    buffer = bytearray(size)
    received = 0

    while received < size:
        try:
            chunk = socket.recv(size - received)
            if not chunk:
                raise ConnectionError("connection closed while receiving data")
        except OSError:
            raise

        buffer[received:received + len(chunk)] = chunk
        received += len(chunk)

    return bytes(buffer)

def send_all(socket: socket.socket, bytes):
    remaining = memoryview(bytes)

    while len(remaining) > 0:
        try:
            sent = socket.send(remaining)
        except OSError:
            raise

        if sent == 0:
            continue

        remaining = remaining[sent:]
