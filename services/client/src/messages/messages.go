package messages

import (
	"errors"
	"strconv"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

type MessageType uint8

const (
	ITEMS_PER_DATE = 3
	ITEMS_PER_LINE = 5
	MAX_NAME_LEN   = 50
	UINT           = 64
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
	BET             MessageType = 64  // 01000000
	BET_ACK         MessageType = 192 // 11000000
	BET_END         MessageType = 66  // 01000010
	WINNER          MessageType = 32  // 00100000
	CONNECT_END     MessageType = 3   // 00000011
	CONNECT_END_ACK MessageType = 131 // 10000011
	ERROR           MessageType = 255 // 11111111
)

type HeaderMessage struct {
	SizePayload uint16
	Type        MessageType
	AgencyId    uint32
	SizeName    uint8
	SizeSurName uint8
	SeqNum      uint8
	AckNum      uint8
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

func parse_string_uint(text string) (uint64, error) {
	value, err := strconv.ParseUint(text, 10, UINT)
	if err != nil {
		logger.Warn("parse-string-uint64", logger.Fail, text)
		return 0, errors.New("Error parsing string to uint")
	}
	return value, nil
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
			name = field
			if len(field) > MAX_NAME_LEN {
				name = field[:MAX_NAME_LEN]
			}

		case SurnameField:
			surname = field
			if len(field) > MAX_NAME_LEN {
				surname = field[:MAX_NAME_LEN]
			}

		case DniField:
			value, err := parse_string_uint(field)
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

			value, err := parse_string_uint(date[0])
			if err != nil {
				logger.Warn("parse-year", logger.Fail, date[0])
				errs = append(errs, errors.New("error parsing year"))
			} else {
				year = uint16(value)
			}

			value, err = parse_string_uint(date[1])
			if err != nil {
				logger.Warn("parse-month", logger.Fail, date[1])
				errs = append(errs, errors.New("error parsing month"))
			} else {
				month = uint8(value)
			}

			value, err = parse_string_uint(date[2])
			if err != nil {
				logger.Warn("parse-month", logger.Fail, date[2])
				errs = append(errs, errors.New("error parsing day"))
			} else {
				day = uint8(value)
			}

		case BetField:
			value, err := parse_string_uint(field)
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

func serialize(msgtype MessageType, info string) (Message, error) {
	switch msgtype {
	case BET:
		body, err := build_body(info)
	}
}
