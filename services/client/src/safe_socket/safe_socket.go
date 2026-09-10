package safe_socket

import (
	"io"
)

func SendAll(socket io.Writer, bytes []byte) error {

	for len(bytes) > 0 {
		n, err := socket.Write(bytes)
		if err != nil {
			return err
		}
		if n == 0 {
			continue
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
			return buff[:received], err
		}
		if n == 0 {
			continue
		}
	}

	return buff, nil
}
