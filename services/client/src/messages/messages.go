package messages

import (
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

type MessageType uint8

const (
	ITEMS_PER_DATE      = 3
	ITEMS_PER_LINE      = 5
	MAX_NAME_LEN        = 50
	UINT                = 64
	BODY_MIN_SIZE       = 12 // en bytes
	MESSAGE_HEADER_SIZE = 11 // en bytes
	BATCH_HEADER_SIZE   = 3
)

const (
	NameField = iota
	SurnameField
	DniField
	DateField
	BetField
)

const (
	CONNECT         MessageType = 1   // 00000001
	CONNECT_ACK     MessageType = 129 // 10000001
	BATCH           MessageType = 16  // 00010000
	BATCH_ACK       MessageType = 144 // 10010000
	BET             MessageType = 64  // 01000000
	BET_ACK         MessageType = 192 // 11000000
	BET_END         MessageType = 66  // 01000010
	WINNER          MessageType = 32  // 00100000
	WINNER_ACK      MessageType = 160 // 10100000
	CONNECT_END     MessageType = 3   // 00000011
	CONNECT_END_ACK MessageType = 131 // 10000011
	ERROR           MessageType = 255 // 11111111
)

type Batch struct {
	Header   HeaderBatch
	Messages []Message
}

type HeaderBatch struct {
	Type      MessageType
	SizeBatch uint16
}

type HeaderMessage struct {
	Type        MessageType
	SeqNum      uint8
	AckNum      uint8
	AgencyId    uint32
	SizePayload uint16
	SizeName    uint8
	SizeSurName uint8
}

type BodyMessage struct {
	Dni     uint32
	Bet     uint32
	Year    uint16
	Month   uint8
	Day     uint8
	Name    string // max 50 bytes
	SurName string // max 50 bytes
}

type Message struct {
	Header HeaderMessage
	Body   *BodyMessage
}

func Read(socket io.Reader) ([]Message, error) {
	header_data, err := safe_socket.RecvAll(socket, BATCH_HEADER_SIZE)
	if err != nil {
		logger.Warn("read-message-header", logger.Fail)
		return nil, err
	}
	header, err := deserialize_batch_header(header_data)
	if err != nil {
		logger.Warn("deserialize-message-header", logger.Fail)
		return nil, err
	}

	var messages []Message

	for i := 0; i < int(header.SizeBatch); i++ {
		message, err := read_message(socket)
		if err != nil {
			logger.Warn("reading-messages", logger.Fail)
			return nil, err
		}
		messages = append(messages, message)
	}

	return messages, nil

}

func Build_batch(messages []Message) Batch {
	return Batch{
		Header: HeaderBatch{
			Type:      BATCH,
			SizeBatch: uint16(len(messages)),
		},
		Messages: messages,
	}
}

func Build_batch_buffer(batch_size int) []byte {
	size_message := MESSAGE_HEADER_SIZE + BODY_MIN_SIZE + MAX_NAME_LEN*2
	size_batch := BATCH_HEADER_SIZE + batch_size*size_message
	return make([]byte, BATCH_HEADER_SIZE, size_batch)
}

func Set_batch_size(data []byte, batch_size uint16) {
	data[0] = byte(BATCH)
	binary.BigEndian.PutUint16(data[1:3], batch_size)
}

func Append_bet(data []byte, seq uint8, id uint32, info_body string) ([]byte, error) {
	body, err := build_body(info_body)
	if err != nil {
		return data, err
	}

	header := build_header(BET, seq, 0, id, &body)
	message := Message{
		Header: header,
		Body:   &body,
	}

	return serialize_into(data, message), nil
}

func Serialize_batch(batch Batch) []byte {
	size_batch := BATCH_HEADER_SIZE

	for _, message := range batch.Messages {
		size_batch += serialized_message_size(message)
	}

	data := make([]byte, BATCH_HEADER_SIZE, size_batch)

	data[0] = byte(batch.Header.Type)
	binary.BigEndian.PutUint16(data[1:3], batch.Header.SizeBatch)

	for _, message := range batch.Messages {
		data = serialize_into(data, message)
	}

	return data
}

func deserialize_batch_header(data []byte) (HeaderBatch, error) {

	if len(data) < BATCH_HEADER_SIZE {
		logger.Warn("deserialize-message-header", logger.Fail)
		return HeaderBatch{}, errors.New("message is smaller than header")
	}

	header := HeaderBatch{
		Type:      MessageType(data[0]),
		SizeBatch: binary.BigEndian.Uint16(data[1:3]),
	}
	return header, nil
}

func read_message(socket io.Reader) (Message, error) {
	header_data, err := safe_socket.RecvAll(socket, MESSAGE_HEADER_SIZE)
	if err != nil {
		logger.Warn("read-message-header", logger.Fail)
		return Message{}, err
	}
	header, err := deserialize_header(header_data)
	if err != nil {
		logger.Warn("deserialize-message-header", logger.Fail)
		return Message{}, err
	}

	var body *BodyMessage

	if header.SizePayload > 0 {
		body_data, err := safe_socket.RecvAll(socket, int(header.SizePayload))
		if err != nil {
			logger.Warn("read-message-body", logger.Fail)
			return Message{}, err
		}
		decoded, err := deserialize_body(body_data, header)
		if err != nil {
			logger.Warn("deserialize-message-body", logger.Fail)
			return Message{}, err
		}
		body = &decoded
	}

	return Message{
		Header: header,
		Body:   body,
	}, nil

}

func Parse_string_uint(text string) (uint64, error) {
	value, err := strconv.ParseUint(text, 10, UINT)
	if err != nil {
		logger.Warn("parse-string-uint64", logger.Fail, text)
		return 0, errors.New("Error parsing string to uint")
	}
	return value, nil
}

func truncateUTF8Bytes(name string, maxBytes int) string {
	if len(name) <= maxBytes {
		return name
	}

	end := 0
	for i := range name {
		if i > maxBytes {
			break
		}
		end = i
	}
	return name[:end]
}

func Build_message(kind MessageType, seq uint8, ack uint8, id uint32, info_body string) (Message, error) {
	var (
		body       *BodyMessage
		header     HeaderMessage
		parsedBody BodyMessage
		err        error
	)

	switch kind {
	case BET:
		parsedBody, err = build_body(info_body)
		if err != nil {
			return Message{}, err
		}
		body = &parsedBody
	default:
		body = nil
	}

	header = build_header(kind, seq, ack, id, body)

	message := Message{
		Header: header,
		Body:   body,
	}

	return message, nil
}

func build_header(kind MessageType, seq uint8, ack uint8, id uint32, body *BodyMessage) HeaderMessage {
	var (
		sizePayload uint16
		sizeName    uint8
		sizeSurname uint8
	)

	if body != nil {
		sizeName = uint8(len(body.Name))
		sizeSurname = uint8(len(body.SurName))
		sizePayload = BODY_MIN_SIZE + uint16(sizeName) + uint16(sizeSurname)
	}
	header := HeaderMessage{
		Type:        kind,
		SeqNum:      seq,
		AckNum:      ack,
		AgencyId:    id,
		SizePayload: sizePayload,
		SizeName:    sizeName,
		SizeSurName: sizeSurname,
	}
	return header
}

func build_body(line string) (BodyMessage, error) {
	fields := strings.Split(line, ",")
	if len(fields) != ITEMS_PER_LINE {
		logger.Warn("serialize-line", logger.Fail, line)
		return BodyMessage{}, errors.New("Not enough items per line")
	}

	var (
		name    string
		surname string
		dni     uint32
		year    uint16
		month   uint8
		day     uint8
		bet     uint32
		errs    []error
	)

	for index, field := range fields {
		switch index {
		case NameField:
			name = truncateUTF8Bytes(field, MAX_NAME_LEN)

		case SurnameField:
			surname = truncateUTF8Bytes(field, MAX_NAME_LEN)

		case DniField:
			value, err := Parse_string_uint(field)
			if err != nil {
				logger.Warn("parse-dni-uint64", logger.Fail, value)
				errs = append(errs, errors.New("error parsing dni"))
			} else {
				dni = uint32(value)
			}

		case DateField:
			date := strings.Split(field, "-")
			if len(date) != ITEMS_PER_DATE {
				logger.Warn("split-date", logger.Fail, field)
				errs = append(errs, errors.New("error splitting date with -"))
				break
			}

			value, err := Parse_string_uint(date[0])
			if err != nil {
				logger.Warn("parse-year", logger.Fail, date[0])
				errs = append(errs, errors.New("error parsing year"))
			} else {
				year = uint16(value)
			}

			value, err = Parse_string_uint(date[1])
			if err != nil {
				logger.Warn("parse-month", logger.Fail, date[1])
				errs = append(errs, errors.New("error parsing month"))
			} else {
				month = uint8(value)
			}

			value, err = Parse_string_uint(date[2])
			if err != nil {
				logger.Warn("parse-month", logger.Fail, date[2])
				errs = append(errs, errors.New("error parsing day"))
			} else {
				day = uint8(value)
			}

		case BetField:
			value, err := Parse_string_uint(field)
			if err != nil {
				logger.Warn("parse-bet", logger.Fail, field)
				errs = append(errs, errors.New("error parsing bet amount"))
			} else {
				bet = uint32(value)
			}
		}
	}

	if len(errs) > 0 {
		return BodyMessage{}, errors.Join(errs...)
	}

	body := BodyMessage{
		Dni:     dni,
		Bet:     bet,
		Year:    year,
		Month:   month,
		Day:     day,
		Name:    name,
		SurName: surname,
	}

	return body, nil
}

func serialized_message_size(message Message) int {
	size_message := MESSAGE_HEADER_SIZE

	if message.Body != nil {
		size_message += int(message.Header.SizePayload)
	}

	return size_message
}

func serialize_into(data []byte, message Message) []byte {
	header := message.Header
	offset := len(data)
	data = data[:offset+serialized_message_size(message)]
	serialized_message := data[offset:]
	clear(serialized_message)

	serialized_message[0] = byte(header.Type)
	serialized_message[1] = header.SeqNum
	serialized_message[2] = header.AckNum
	binary.BigEndian.PutUint32(serialized_message[3:7], header.AgencyId)
	binary.BigEndian.PutUint16(serialized_message[7:9], header.SizePayload)
	serialized_message[9] = header.SizeName
	serialized_message[10] = header.SizeSurName

	if message.Body != nil {
		body := message.Body
		body_offset := MESSAGE_HEADER_SIZE

		binary.BigEndian.PutUint32(serialized_message[body_offset:body_offset+4], body.Dni)
		body_offset += 4
		binary.BigEndian.PutUint32(serialized_message[body_offset:body_offset+4], body.Bet)
		body_offset += 4
		binary.BigEndian.PutUint16(serialized_message[body_offset:body_offset+2], body.Year)
		body_offset += 2
		serialized_message[body_offset] = body.Month
		body_offset++
		serialized_message[body_offset] = body.Day
		body_offset++

		body_offset += copy(serialized_message[body_offset:], body.Name)
		copy(serialized_message[body_offset:], body.SurName)
	}

	return data
}

func Serialize(message Message) []byte {
	data := make([]byte, 0, serialized_message_size(message))
	return serialize_into(data, message)
}

func deserialize_header(data []byte) (HeaderMessage, error) {

	if len(data) < MESSAGE_HEADER_SIZE {
		logger.Warn("deserialize-message-header", logger.Fail)
		return HeaderMessage{}, errors.New("message is smaller than header")
	}

	header := HeaderMessage{
		Type:        MessageType(data[0]),
		SeqNum:      data[1],
		AckNum:      data[2],
		AgencyId:    binary.BigEndian.Uint32(data[3:7]),
		SizePayload: binary.BigEndian.Uint16(data[7:9]),
		SizeName:    data[9],
		SizeSurName: data[10],
	}
	return header, nil
}

func deserialize_body(data []byte, header HeaderMessage) (BodyMessage, error) {
	payloadSize := int(header.SizePayload)
	nameSize := int(header.SizeName)
	surnameSize := int(header.SizeSurName)

	if payloadSize < BODY_MIN_SIZE {
		logger.Warn("deserialize-message-payload", logger.Fail)
		return BodyMessage{}, errors.New("invalid payload size")
	}

	if payloadSize != BODY_MIN_SIZE+nameSize+surnameSize {
		logger.Warn("deserialize-message-payload", logger.Fail)
		return BodyMessage{}, errors.New("payload size does not match name sizes")
	}

	if len(data) != payloadSize {
		logger.Warn("deserialize-message-size", logger.Fail)
		return BodyMessage{}, errors.New("incomplete or oversized message")
	}

	offset := 0
	body := &BodyMessage{
		Dni: binary.BigEndian.Uint32(data[offset : offset+4]),
	}
	offset += 4

	body.Bet = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	body.Year = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	body.Month = data[offset]
	offset++

	body.Day = data[offset]
	offset++

	body.Name = string(data[offset : offset+nameSize])
	offset += nameSize

	body.SurName = string(data[offset : offset+surnameSize])
	return *body, nil
}

func Deserialize(data []byte) (Message, error) {
	header, err := deserialize_header(data)
	if err != nil {
		return Message{}, err
	}

	if header.SizePayload == 0 {
		return Message{
			Header: header,
			Body:   nil,
		}, nil
	}

	body, err := deserialize_body(data[MESSAGE_HEADER_SIZE:], header)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Header: header,
		Body:   &body,
	}, nil
}
