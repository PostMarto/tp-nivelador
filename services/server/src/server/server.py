from collections.abc import Iterator
from dataclasses import dataclass
import queue
import socket
import threading
import logger
from lottery.lottery import Lottery
import safe_socket
import messages.messages as messages
from pathlib import Path

LOTTERY_PATH = "/output/lottery.csv"

CONNECTING      = 1   # 00000001
SENDING         = 2   # 00000010
WAITING_WINNER  = 4   # 00000100
WAITING_CLOSE   = 8   # 00001000
CLOSING         = 128 # 10000000

@dataclass
class ClientConnection:
    socket: socket.socket
    status: int
    agency_id: int
    winners: Iterator[messages.Message] | None = None
     
class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        Path(LOTTERY_PATH).parent.mkdir(parents=True, exist_ok=True)
        Path(LOTTERY_PATH).touch(exist_ok=True)
        self.lottery = Lottery(LOTTERY_PATH)
        self.server_host = server_host
        self.server_port = server_port
        self.threads = []
        self.finished_agencies = set()
        self.agency_quorum_min = agency_quorum_min
        self.lottery_condition = threading.Condition()
        self.lottery_error: Exception | None = None

    def _handle_client(self, client_socket: socket.socket, queue: queue.Queue):
        action = "handle-client"
        message_amount = 0
        client_connection = ClientConnection(client_socket, CONNECTING, 0)
        try:
            logger.info(action, logger.LogResult.in_progress) # type: ignore
            while client_connection.status != CLOSING:
                msg = messages.read(client_socket)
                self.process_message(msg, client_connection, queue)
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount, "err", e
            )
            try:
                error = messages.build_message(messages.ERROR, 0, 0, client_connection.agency_id, None)
                messages.send(client_socket, error)
            except OSError:
                pass  # The peer may already have disconnected.
        finally:
            client_socket.close()

    def _handle_lottery(self, lottery_queue: queue.Queue):
        while True:
            request = lottery_queue.get()
            try:
                if request is None:
                    return
                bet, result_queue = request
                with self.lottery_condition:
                    if self.lottery_error is None:
                        try:
                            self.lottery.store_bets([bet])
                        except Exception as error:
                            self.lottery_error = error
                            self.lottery_condition.notify_all()
                    result = self.lottery_error
                result_queue.put(result)
            finally:
                lottery_queue.task_done()

    def run(self):
        action = "accept-connection"
        lottery_queue = queue.Queue()
        lottery_thread = threading.Thread(target= self._handle_lottery, args=(lottery_queue,))
        lottery_thread.start()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                client_thread = threading.Thread(target= self._handle_client, args=(client_socket, lottery_queue,))
                client_thread.start()
                self.threads.append(client_thread)

    def process_message(self, message: messages.Message, client_connection: ClientConnection, queue: queue.Queue):
        match message.Header.Type:
            case messages.CONNECT:
                if client_connection.status != CONNECTING:
                    logger.error("process-message", logger.LogResult.fail, "invalid-client-status", client_connection.status)
                    return
                client_connection.agency_id = message.Header.AgencyId
                self.process_connect(message, client_connection)

            case messages.BET:
                if client_connection.status != SENDING or client_connection.agency_id != message.Header.AgencyId:
                    logger.error("process-message", logger.LogResult.fail, "invalid-client-status", client_connection.status)
                    return
                self.process_bet(message, client_connection, queue)

            case messages.BET_END:
                if client_connection.status != SENDING or client_connection.agency_id != message.Header.AgencyId:
                    logger.error("process-message", logger.LogResult.fail, "invalid-client-status", client_connection.status)
                    return
                client_connection.status = WAITING_WINNER
                self.process_bet_end(message, client_connection, queue)

            case messages.WINNER_ACK:
                if client_connection.status != WAITING_WINNER or message.Header.AgencyId != client_connection.agency_id or client_connection.winners is None:
                    logger.error("process-message", logger.LogResult.fail, "invalid-client-status", client_connection.status)
                    return
                self.process_winner_ack(message, client_connection)

            case messages.CONNECT_END_ACK:
                if client_connection.status != WAITING_CLOSE or message.Header.AgencyId != client_connection.agency_id:
                    logger.error("process-message", logger.LogResult.fail, "invalid-client-status", client_connection.status)
                    return
                self.process_connect_end_ack(message, client_connection)

            case messages.ERROR:
                self.process_error(message, client_connection)

            case _:
                logger.error("process-message", logger.LogResult.fail, "unknown-message-type", message.Header.Type)
                return

    def process_connect(self, message: messages.Message, client_connection: ClientConnection):
        ack = messages.build_message(messages.CONNECT_ACK, 0, 0, message.Header.AgencyId, None)
        messages.send(client_connection.socket, ack)
        client_connection.status = SENDING

    def process_bet(self, message: messages.Message, client_connection: ClientConnection, lottery_queue: queue.Queue):
        bet = messages.message_to_bet(message)
        result_queue = queue.Queue(maxsize=1)
        lottery_queue.put((bet, result_queue))
        error = result_queue.get()
        if error is not None:
            raise error
        ack = messages.build_message(messages.BET_ACK, message.Header.SeqNum, message.Header.SeqNum, message.Header.AgencyId, message.Body)
        messages.send(client_connection.socket, ack)

    def process_bet_end(self, message: messages.Message, client_connection: ClientConnection, queue: queue.Queue):
        queue.join()
        with self.lottery_condition:
            self.finished_agencies.add(client_connection.agency_id)
            self.lottery_condition.notify_all()
            self.lottery_condition.wait_for(
                lambda: self.lottery_error is not None or len(self.finished_agencies) >= self.agency_quorum_min
            )
            if self.lottery_error is not None:
                raise self.lottery_error
            winners = [bet for bet in self.lottery.load_bets() if bet.agency_id == client_connection.agency_id and self.lottery.has_won(bet)]
            winner_messages = []
            for bet in winners:
                year, month, day = map(int, bet.birthdate.split("-"))
                body = messages.BodyMessage(
                    Dni=bet.document,
                    Bet=bet.number,
                    Year=year,
                    Month=month,
                    Day=day,
                    Name=bet.first_name,
                    SurName=bet.last_name,
                )
                winner_messages.append(messages.build_message(messages.WINNER, 0, 0, client_connection.agency_id, body))

            winner_iterator = iter(winner_messages)
            client_connection.winners = winner_iterator
            outgoing = next(winner_iterator, None)

            if outgoing is None:
                client_connection.winners = None
                client_connection.status = WAITING_CLOSE
                outgoing = messages.build_message(
                    messages.CONNECT_END, 0, 0, client_connection.agency_id, None
                )

        messages.send(client_connection.socket, outgoing)

    def process_winner_ack(self, message: messages.Message, client_connection: ClientConnection):
        winners = client_connection.winners
        if winners is None:
            raise ValueError("Winner delivery has not started")

        outgoing = next(winners, None)
        if outgoing is None:
            client_connection.winners = None
            client_connection.status = WAITING_CLOSE
            outgoing = messages.build_message(
                messages.CONNECT_END, 0, 0, client_connection.agency_id, None
            )
        messages.send(client_connection.socket, outgoing)

    def process_connect_end_ack(self, message: messages.Message, client_connection: ClientConnection):
        client_connection.status = CLOSING

    def process_error(self, message: messages.Message, client_connection: ClientConnection):
        logger.error("client-error", logger.LogResult.fail, "agency-id", client_connection.agency_id, "ack-num", message.Header.AckNum)
        client_connection.winners = None
        client_connection.status = CLOSING
