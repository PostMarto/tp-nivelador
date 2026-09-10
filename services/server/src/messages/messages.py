
from dataclasses import dataclass
import socket
import string
import safe_socket
from lottery.bet import Bet

BODY_MIN_SIZE = 12
MESSAGE_HEADER_SIZE = 11
BATCH_HEADER_SIZE = 3

NAME_FIELD      = 0
SURNAME_FIELD   = 1
DNI_FIELD       = 2
DATE_FIELD      = 3
BET_FIELD       = 4

CONNECT          = 1
CONNECT_ACK      = 129
BATCH            = 16
BATCH_ACK        = 144
BET              = 64
BET_ACK          = 192
BET_END          = 66
WINNER           = 32
WINNER_ACK       = 160
CONNECT_END      = 3
CONNECT_END_ACK  = 131
ERROR            = 255

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
	Name:    str # type: ignore # max 50 bytes
	SurName: str # type: ignore # max 50 bytes

@dataclass
class Message:
	Header: HeaderMessage
	Body:   BodyMessage | None

@dataclass
class HeaderBatch:
	Type: int
	SizeBatch: int

@dataclass
class Batch:
	Header: HeaderBatch
	Messages: list[Message]

def read(socket) -> Message | Batch:
	header_data = safe_socket.recv_all(socket, BATCH_HEADER_SIZE)

	if header_data[0] == BATCH:
		header = deserialize_batch_header(header_data)
		if header.SizeBatch == 0:
			raise ValueError("batch cannot be empty")

		messages_in_batch = []
		for _ in range(header.SizeBatch):
			messages_in_batch.append(read_message(socket))

		return Batch(
			Header=header,
			Messages=messages_in_batch,
		)

	return read_message(socket, header_data)

def read_message(socket, header_data: bytes = b"") -> Message:
	if len(header_data) > MESSAGE_HEADER_SIZE:
		raise ValueError("message header prefix is too large")

	header_data += safe_socket.recv_all(socket, MESSAGE_HEADER_SIZE - len(header_data))
	header = deserialize_head(header_data)
	body = None

	if header.SizePayload > 0:
		body_data = safe_socket.recv_all(socket, header.SizePayload)
		body = deserialize_body(body_data, header)

	return Message(
		Header=header,
		Body=body,
	)

def send(socket: socket.socket, message: Message):
	batch = build_batch([message])
	binary_batch = serialize_batch(batch)
	safe_socket.send_all(socket, binary_batch)

def build_batch(messages_in_batch: list[Message]) -> Batch:
	return Batch(
		Header=HeaderBatch(
			Type=BATCH,
			SizeBatch=len(messages_in_batch),
		),
		Messages=messages_in_batch,
	)

def serialize_batch(batch: Batch) -> bytes:
	if batch.Header.SizeBatch != len(batch.Messages):
		raise ValueError("batch size does not match message count")

	if batch.Header.SizeBatch > 0xFFFF:
		raise ValueError("batch contains too many messages")

	data = bytearray(BATCH_HEADER_SIZE)
	data[0] = batch.Header.Type
	data[1:3] = batch.Header.SizeBatch.to_bytes(2, byteorder="big")

	for message in batch.Messages:
		data.extend(serialize(message))

	return bytes(data)

def deserialize_batch_header(data: bytes) -> HeaderBatch:
	if len(data) != BATCH_HEADER_SIZE:
		raise ValueError("invalid batch header size")

	if data[0] != BATCH:
		raise ValueError("invalid batch message type")

	return HeaderBatch(
		Type=data[0],
		SizeBatch=int.from_bytes(data[1:3], byteorder="big"),
	)
	

def build_message(kind: int, seq: int, ack: int, id: int, body: BodyMessage | None) -> Message:
	payload_size = 0
	name_size = 0
	surname_size = 0
	
	if body is not None:
		name_size = len(body.Name.encode("utf-8"))
		surname_size = len(body.SurName.encode("utf-8"))
		payload_size = BODY_MIN_SIZE + name_size + surname_size
	
	header = HeaderMessage(
		Type=kind,
		SeqNum=seq,
		AckNum=ack,
		AgencyId=id,
		SizePayload=payload_size,
		SizeName=name_size,
		SizeSurName=surname_size,
	)

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
	data = bytearray(MESSAGE_HEADER_SIZE)

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

	body = deserialize_body(data[MESSAGE_HEADER_SIZE:], header)

	return Message(
		Header=header,
		Body=body,
	)

def deserialize_head(data: bytes) -> HeaderMessage:
	if len(data) < MESSAGE_HEADER_SIZE:
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
