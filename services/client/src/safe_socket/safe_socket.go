package safe_socket

import (
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

func SendAll(socket io.Writer, bytes []byte) error {

	for len(bytes) > 0 {
		n, err := socket.Write(bytes)
		if err != nil {
			logger.Error("send-all-error", logger.Fail)
			return err
		}
		if n == 0 {
			logger.Error("send-all-short-write", logger.Fail)
			return io.ErrShortWrite
		}
		bytes = bytes[n:]
	}

	return nil

}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	received := 0
	for received < size {
		n, err := socket.Read(buff[received:])
		received += n
		if err != nil {
			if received == size {
				return buff, nil
			}
			logger.Error("receive-all-error", logger.Fail)
			return buff[:received], err
		}
		if n == 0 {
			logger.Error("reveice-all-short-read", logger.Fail)
		}
	}

	return buff, nil
}
