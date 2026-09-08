
from dataclasses import dataclass
import string
import safe_socket
from lottery.bet import Bet

BODY_MIN_SIZE  = 12 # en bytes
HEADER_SIZE    = 11 # en bytes

NAME_FIELD      = 0
SURNAME_FIELD   = 1
DNI_FIELD       = 2
DATE_FIELD      = 3
BET_FIELD       = 4

CONNECT          = 1   # 00000001
CONNECT_ACK      = 129 # 10000001
BET              = 64  # 01000000
BET_ACK          = 192 # 11000000
BET_END          = 66  # 01000010
WINNER           = 32  # 00100000
WINNER_ACK       = 160 # 10100000
CONNECT_END      = 3   # 00000011
CONNECT_END_ACK  = 131 # 10000011
ERROR            = 255 # 11111111

@dataclass
class HeaderMessage:
	Type:        int
	SeqNum:      int
	AckNum:      int
	AgencyId:    int
	SizePayload: int
	SizeName:    int
	SizeSurName: int

@dataclass
class BodyMessage:
	Dni:     int
	Bet:     int
	Year:    int
	Month:   int
	Day:     int
	Name:    string # max 50 bytes
	SurName: string # max 50 bytes

@dataclass
class Message:
	Header: HeaderMessage
	Body:   BodyMessage

def read(socket) -> Message:
	header_data = safe_socket.recv_all(socket, HEADER_SIZE)
	header = deserialize_head(header_data)

	body = None

	if header.SizePayload > 0:
		body_data = safe_socket.recv_all(socket, header.SizePayload)
		body = deserialize_body(body_data, header)

	return Message(
		Header=header,
		Body=body,
	)

def message_to_bet(message: Message) -> Bet:
	if message.Header.Type != BET:
		raise ValueError("message is not a BET message")

	if message.Body is None:
		raise ValueError("BET message has no body")

	body = message.Body

	return Bet(
		agency_id=message.Header.AgencyId,
		first_name=body.Name,
		last_name=body.SurName,
		document=body.Dni,
		birthdate=f"{body.Year:04d}-{body.Month:02d}-{body.Day:02d}",
		number=body.Bet,
	)

def serialize(message: Message) -> bytes:
	data = serialize_head(message.Header)

	if message.Body is not None:
		data += serialize_body(message.Body)

	return data

def serialize_head(header: HeaderMessage) -> bytes:
	data = bytearray(HEADER_SIZE)

	data[0] = header.Type
	data[1] = header.SeqNum
	data[2] = header.AckNum
	data[3:7] = header.AgencyId.to_bytes(4, byteorder="big")
	data[7:9] = header.SizePayload.to_bytes(2, byteorder="big")
	data[9] = header.SizeName
	data[10] = header.SizeSurName

	return bytes(data)

def serialize_body(body: BodyMessage) -> bytes:
	name = body.Name.encode("utf-8")
	surname = body.SurName.encode("utf-8")

	data = bytearray(BODY_MIN_SIZE + len(name) + len(surname))
	offset = 0

	data[offset:offset + 4] = body.Dni.to_bytes(4, byteorder="big")
	offset += 4

	data[offset:offset + 4] = body.Bet.to_bytes(4, byteorder="big")
	offset += 4

	data[offset:offset + 2] = body.Year.to_bytes(2, byteorder="big")
	offset += 2

	data[offset] = body.Month
	offset += 1

	data[offset] = body.Day
	offset += 1

	data[offset:offset + len(name)] = name
	offset += len(name)

	data[offset:offset + len(surname)] = surname

	return bytes(data)

def deserialize(data: bytes) -> Message:
	header = deserialize_head(data)

	if header.SizePayload == 0:
		return Message(
			Header=header,
			Body=None,
		)

	body = deserialize_body(data[HEADER_SIZE:], header)

	return Message(
		Header=header,
		Body=body,
	)

def deserialize_head(data: bytes) -> HeaderMessage:
	if len(data) < HEADER_SIZE:
		raise ValueError("header too small")
	
	return HeaderMessage(
		Type=data[0],
		SeqNum=data[1],
		AckNum=data[2],
		AgencyId=int.from_bytes(data[3:7], byteorder="big"),
		SizePayload=int.from_bytes(data[7:9], byteorder="big"),
		SizeName=data[9],
		SizeSurName=data[10],
	)

def deserialize_body(data: bytes, header: HeaderMessage) -> BodyMessage:
	payload_size = header.SizePayload
	name_size = header.SizeName
	surname_size = header.SizeSurName

	if payload_size < BODY_MIN_SIZE:
		raise ValueError("payload size is too short")

	if payload_size != BODY_MIN_SIZE + name_size + surname_size:
		raise ValueError("payload does not match with name sizes")

	if len(data) != payload_size:
		raise ValueError("message size does not match payload size")

	offset = 0
	dni = int.from_bytes(data[offset:offset+4], byteorder="big")
	offset += 4

	bet = int.from_bytes(data[offset:offset+4], byteorder="big")
	offset += 4

	year = int.from_bytes(data[offset:offset+2], byteorder="big")
	offset += 2

	month = data[offset]
	offset += 1

	day = data[offset]
	offset += 1

	name = data[offset:offset + name_size].decode("utf-8")
	offset += name_size

	surname = data[offset:offset + surname_size].decode("utf-8")

	return BodyMessage(
		Dni=dni,
		Bet=bet,
		Year=year,
		Month=month,
		Day=day,
		Name=name,
		SurName=surname,
	)
