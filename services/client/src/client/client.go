package client

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/messages"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 50
const CONNECTION_ATTEMPS_DELAY_MS = 200
const CONNECTION_TIMEOUT = time.Second

const WINDOW_SIZE = 10
const MESSAGE_QUEUE_SIZE = 15

type ClientStatus uint8

const (
	CONNECTING     ClientStatus = 1   // 00000001
	SENDING        ClientStatus = 2   // 00000010
	WAITING_WINNER ClientStatus = 4   // 00000100
	WAITING_CLOSE  ClientStatus = 8   // 00001000
	CLOSING        ClientStatus = 128 // 10000000
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type SentMessage struct {
	Message     messages.Message
	AckReceived bool
}

type Client struct {
	conn               net.Conn
	config             ClientConfig
	status             ClientStatus
	messages_on_flight map[uint8]SentMessage
	input              *os.File
	output             *os.File
	reader             *bufio.Scanner
	writer             *bufio.Writer
	id                 uint32
	window_base        uint8
	input_done         bool
	last_batch         *messages.Batch
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	id, err := messages.Parse_string_uint(client.config.AgencyId)
	if err != nil {
		conn.Close()
		return nil, err
	}

	input, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, client.config.InputFile)
		conn.Close()
		return nil, err
	}

	output, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("open-output-file", logger.Fail, client.config.OutputFile)
		input.Close()
		conn.Close()
		return nil, err
	}

	client.messages_on_flight = make(map[uint8]SentMessage, 0)
	client.input = input
	client.output = output
	client.reader = bufio.NewScanner(input)
	client.writer = bufio.NewWriter(output)
	client.status = CONNECTING
	client.id = uint32(id)
	client.window_base = 0
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.DialTimeout("tcp", net.JoinHostPort(host, port), CONNECTION_TIMEOUT)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i+1, "err", err)
			if i+1 < CONNECTION_ATTEMPTS_MAX {
				time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			}
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) connect() error {
	connect_message, err := messages.Build_message(messages.CONNECT, 0, 0, uint32(client.id), "")
	if err != nil {
		return err
	}
	return client.send(connect_message)
}

func (client *Client) send(message messages.Message) error {
	binary_message := messages.Serialize(message)
	if err := safe_socket.SendAll(client.conn, binary_message); err != nil {
		logger.Error("send-message", logger.Fail)
		return err
	}

	if message.Header.Type == messages.BET {
		seq_num := message.Header.SeqNum
		client.messages_on_flight[seq_num] = SentMessage{message, false}
	}
	return nil
}

func (client *Client) send_batch(batch messages.Batch) error {
	serialized_batch := messages.Serialize_batch(batch)

	if err := safe_socket.SendAll(client.conn, serialized_batch); err != nil {
		logger.Error("send-batch", logger.Fail)
		return err
	}

	client.last_batch = &batch
	return nil
}

func (client *Client) resend_last_batch() error {
	if client.last_batch == nil {
		return errors.New("there is no batch to resend")
	}

	return client.send_batch(*client.last_batch)
}

func (client *Client) get_next_seq_num() uint8 {
	return client.window_base + uint8(len(client.messages_on_flight))
}

func (client *Client) Run() error {
	context, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var work_group sync.WaitGroup

	message_queue := make(chan messages.Message, MESSAGE_QUEUE_SIZE)
	processor_done := make(chan struct{})

	defer client.Close_all()
	if err := client.connect(); err != nil {
		return err
	}

	work_group.Add(1)

	go func() {
		defer close(processor_done)
		defer client.conn.Close()
		defer work_group.Done()

		for message := range message_queue {
			if err := client.process_message(message); err != nil {
				return
			}
		}
	}()

	go func() {
		select {
		case <-context.Done():
			_ = client.conn.Close()
		case <-processor_done:
		}
	}()

	read_err := client.producer(message_queue, processor_done)

	if read_err != nil && context.Err() == nil {
		logger.Error("producer-error", logger.Fail)
	}

	work_group.Wait()

	if context.Err() != nil {
		return nil
	}

	if client.status == CLOSING {
		return nil
	}

	return read_err
}

func (client *Client) producer(message_queue chan messages.Message, processor_done chan struct{}) error {
	defer close(message_queue)
	for {
		received_messages, err := messages.Read(client.conn)
		if err != nil {
			return err
		}
		for _, received_message := range received_messages {
			select {
			case message_queue <- received_message:
				if received_message.Header.Type == messages.CONNECT_END {
					return nil
				}
			case <-processor_done:
				return nil
			}
		}
	}
}

func (client *Client) Close_all() {
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
