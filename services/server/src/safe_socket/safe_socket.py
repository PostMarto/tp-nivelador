import socket
import logger

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket, size):
    buffer = bytearray(size)
    received = 0

    while received < size:
        try:
            chunk = socket.recv(size - received)
        except OSError:
            logger.error("receive-all-error", logger.LogResult.fail)
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
            logger.error("send-all-error", logger.LogResult.fail)
            raise

        if sent == 0:
            logger.error("send-all-short-write", logger.LogResult.fail)
            raise ConnectionError("socket made no progress while sending data")

        remaining = remaining[sent:]
