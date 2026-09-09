package client

import (
	"errors"
	"fmt"
	"net"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/messages"
)

func (client *Client) process_message(message messages.Message) error {
	switch message.Header.Type {
	case messages.CONNECT_ACK:
		if client.status == CONNECTING {
			client.status = SENDING
			return client.start_send()
		}

	case messages.BET_ACK:
		if client.status == SENDING {
			return client.receive_bet_ack(message.Header.AckNum)
		}

	case messages.WINNER:
		if client.status == WAITING_WINNER {
			if err := client.save_winner(message); err != nil {
				return err
			}
			return client.send_winner_ack()
		}

	case messages.CONNECT_END:
		if err := client.send_close_ack(); err != nil {
			return err
		}
		client.status = CLOSING

	case messages.ERROR:
		return errors.New("server reported an error")
	}
	return nil
}

func (client *Client) save_winner(message messages.Message) error {
	if message.Body == nil {
		logger.Error("save-winner", logger.Fail)
		return errors.New("No body on the message")
	}
	body := message.Body
	_, err := fmt.Fprintf(client.writer, "%s,%s,%d,%04d-%02d-%02d,%d\n",
		body.Name,
		body.SurName,
		body.Dni,
		body.Year,
		body.Month,
		body.Day,
		body.Bet,
	)

	if err == nil {
		err = client.writer.Flush()
	}
	if err != nil {
		logger.Error("save-winner", logger.Fail)
		return errors.New("Could not write to file")
	}
	return nil
}

func (client *Client) send_winner_ack() error {
	message, err := messages.Build_message(messages.WINNER_ACK, 0, 0, client.id, "")
	if err != nil {
		logger.Error("send-winner-ack", logger.Fail)
		return errors.New("Could not create message")
	}
	return client.send(message)
}

func (client *Client) send_close_ack() error {
	message, err := messages.Build_message(messages.CONNECT_END_ACK, 0, 0, client.id, "")
	if err != nil {
		logger.Error("send-connect-end-ack", logger.Fail)
		return errors.New("Could not create message")
	}
	return client.send(message)
}

func (client *Client) send_bet_end() error {
	message, err := messages.Build_message(messages.BET_END, 0, 0, client.id, "")
	if err != nil {
		logger.Error("send-bet-end", logger.Fail)
		return errors.New("Could not create message")
	}
	return client.send(message)
}

func (client *Client) close_all() {
	if client.writer != nil {
		err := client.writer.Flush()
		if err != nil {
			logger.Error("close-all-flush", logger.Fail)
		}
		client.writer = nil
	}

	if client.input != nil {
		err := client.input.Close()
		if err != nil {
			logger.Error("close-all-input", logger.Fail)
		}
		client.input = nil
	}

	if client.output != nil {
		err := client.output.Close()
		if err != nil {
			logger.Error("close-all-output", logger.Fail)
		}
		client.output = nil
	}

	if client.conn != nil {
		err := client.conn.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Error("close-all-conn", logger.Fail)
		}
		client.conn = nil
	}
}

func (client *Client) start_send() error {
	if client.status != SENDING {
		return nil
	}
	for !client.input_done && len(client.messages_on_flight) < WINDOW_SIZE {
		if client.reader.Scan() {
			line := client.reader.Text()
			message, err := messages.Build_message(messages.BET, client.get_next_seq_num(), 0, client.id, line)
			if err != nil {
				logger.Error("start-send-build-message", logger.Fail)
				return err
			}
			if err := client.send(message); err != nil {
				return err
			}
		} else {
			if err := client.reader.Err(); err != nil {
				return err
			}
			client.input_done = true
		}
	}
	if client.input_done && len(client.messages_on_flight) == 0 {
		if err := client.send_bet_end(); err != nil {
			return err
		}
		client.status = WAITING_WINNER
	}
	return nil
}

func (client *Client) receive_bet_ack(seq_num uint8) error {
	message, exist := client.messages_on_flight[seq_num]
	if !exist {
		return nil
	}

	message.AckReceived = true
	client.messages_on_flight[seq_num] = message

	for {
		base_message, exist := client.messages_on_flight[client.window_base]
		if !exist || !base_message.AckReceived {
			break
		}

		delete(client.messages_on_flight, client.window_base)
		client.window_base++
	}

	return client.start_send()
}
