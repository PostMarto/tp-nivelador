from dataclasses import dataclass
import socket
import logger
import safe_socket
import messages.messages as messages

_ECHO_SERVER_MESSAGE_SIZE = 1024

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
     
class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.connections = {}

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                client_message = safe_socket.recv_all(
                    client_socket, _ECHO_SERVER_MESSAGE_SIZE
                )
                if not client_message:
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount,
                    )
                    return
                message_amount += 1
                safe_socket.send_all(client_socket, client_message)
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

    def run(self):
        action = "accept-connection"
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

                self._handle_client(client_socket)

    def process_message(self, message: messages.Message):
        match message.Header.Type:
            case messages.CONNECT:
                self.process_connect(message)

            case messages.BET:
                self.process_bet(message)

            case messages.BET_END:
                self.process_bet_end(message)

            case messages.WINNER_ACK:
                self.process_winner_ack(message)

            case messages.CONNECT_END_ACK:
                self.process_connect_end_ack(message)

            case messages.ERROR:
                self.process_connect_end_ack(message)

    def process_connect(self, message: messages.Message):
        if message.Header.AgencyId in self.connections:
            ack = messages.build_message(messages.CONNECT_ACK, 0, 0, message.Header.AgencyId, None)
            self.send(ack)
        else:
            self.create_connection(message.Header.)
        return

    def process_bet(self, message: messages.Message):
            return

    def process_bet_end(self, message: messages.Message):
            return

    def process_winner_ack(self, message: messages.Message):
            return

    def process_connect_end_ack(self, message: messages.Message):
            return

    def process_error(self, message: messages.Message):
            return
