from collections.abc import Iterator
from dataclasses import dataclass
import queue
import signal
import socket
import threading
import logger
from lottery.lottery import Lottery
import safe_socket
import messages.messages as messages
from pathlib import Path

LOTTERY_PATH = "/output/lottery.csv"

RUNNING         = 1   # 00000001
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
        self.status = RUNNING
        self.threads = []
        self.finished_agencies = set()
        self.agency_quorum_min = agency_quorum_min
        self.lottery_condition = threading.Condition()
        self.lottery_error: Exception | None = None
        self.lottery_thread: threading.Thread | None = None
        self.lottery_queue: queue.Queue | None = None
        self.sockets = []
        self.server_socket: socket.socket | None = None

    def _handle_client(self, client_socket: socket.socket, lottery_queue: queue.Queue):
        action = "handle-client"
        message_amount = 0
        client_connection = ClientConnection(client_socket, CONNECTING, 0)
        try:
            logger.info(action, logger.LogResult.in_progress)
            while client_connection.status != CLOSING and self.status != CLOSING:
                incoming = messages.read(client_socket)

                if self.status == CLOSING:
                    break

                if isinstance(incoming, messages.Batch):
                    message_amount += len(incoming.Messages)
                    try:
                        self.process_batch(incoming, client_connection, lottery_queue)
                    except Exception as error:
                        logger.error("process-batch", logger.LogResult.fail, "err", error)
                        error_message = messages.build_message(messages.ERROR, 0, 0, client_connection.agency_id, None)
                        messages.send(client_socket, error_message)
                else:
                    message_amount += 1
                    self.process_message(incoming, client_connection, lottery_queue)

        except Exception as error:
            logger.error(action, logger.LogResult.fail, "messages-amount", message_amount, "err", error)
            try:
                error_message = messages.build_message(messages.ERROR, 0, 0, client_connection.agency_id, None)
                messages.send(client_socket, error_message)
            except OSError:
                pass
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
        self.sigterm_listener()
        self.lottery_queue = queue.Queue()
        self.lottery_thread = threading.Thread(target= self._handle_lottery, args=(self.lottery_queue,))
        self.lottery_thread.start()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.bind((self.server_host, self.server_port))
                server_socket.listen()
                self.server_socket = server_socket
                while self.status != CLOSING:
                    try:
                        logger.info(action, logger.LogResult.in_progress)
                        client_socket, _ = server_socket.accept()
                        self.sockets.append(client_socket)
                        if self.status == CLOSING:
                            break
                    except OSError:
                        if self.status == CLOSING:
                            break
                        logger.error(action, logger.LogResult.fail)
                        raise
                    logger.info(action, logger.LogResult.success)

                    client_thread = threading.Thread(target= self._handle_client, args=(client_socket, self.lottery_queue,))
                    client_thread.start()
                    self.threads.append(client_thread)
        finally:
            self.shutdown()

    def sigterm_listener(self):
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def sigterm_handler(self, signum, frame):
        self.status = CLOSING
        if self.server_socket is not None:
            self.server_socket.close()

    def shutdown(self):
        self.status = CLOSING
        for conn in self.sockets:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()

        with self.lottery_condition:
            self.lottery_error = Exception("server is shutting down")
            self.lottery_condition.notify_all()

        for thread in self.threads:
            thread.join()

        if self.lottery_queue is not None:
            self.lottery_queue.put(None)

        if self.lottery_thread is not None:
            self.lottery_thread.join()
        
        logger.info("server-shutdown", logger.LogResult.success)

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

    def process_batch(self, batch: messages.Batch, client_connection: ClientConnection, lottery_queue: queue.Queue):
        if len(batch.Messages) == 0:
            raise ValueError("received an empty batch")

        if client_connection.status != SENDING:
            raise ValueError("client is not in SENDING status")

        agency_id = batch.Messages[0].Header.AgencyId

        for message in batch.Messages:
            if message.Header.Type != messages.BET:
                raise ValueError("batch contains a non-BET message")

            if message.Header.AgencyId != agency_id:
                raise ValueError("batch contains different agency IDs")

            if message.Header.AgencyId != client_connection.agency_id:
                raise ValueError("batch agency ID does not match client")

            if message.Body is None:
                raise ValueError("BET message has no body")

        for message in batch.Messages:
            self.process_bet(message, client_connection, lottery_queue, False)

        batch_ack = messages.build_message(messages.BATCH_ACK, 0, 0, agency_id, None)
        messages.send(client_connection.socket, batch_ack)

    def process_bet(self, message: messages.Message, client_connection: ClientConnection, lottery_queue: queue.Queue, send_ack: bool = True):
        bet = messages.message_to_bet(message)
        result_queue = queue.Queue(maxsize=1)
        lottery_queue.put((bet, result_queue))
        error = result_queue.get()

        if error is not None:
            raise error

        if send_ack:
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
