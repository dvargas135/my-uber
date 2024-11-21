import zmq
import time
from src.config import DISPATCHER_IP, HEARTBEAT_PORT, BACKUP_REP_PORT
from src.utils.rich_utils import RichConsoleUtils

class HeartbeatService:
    def __init__(self, dispatcher_ip, heartbeat_port, backup_rep_port):
        self.dispatcher_ip = dispatcher_ip
        self.heartbeat_port = heartbeat_port
        self.backup_rep_port = backup_rep_port
        self.console_utils = RichConsoleUtils()
        self.context = zmq.Context()
        self.heartbeat_socket = self.context.socket(zmq.REQ)
        self.heartbeat_socket.connect(f"tcp://{self.dispatcher_ip}:{self.heartbeat_port}")
        self.backup_socket = self.context.socket(zmq.PUB)
        self.backup_socket.bind(f"tcp://*:{self.backup_rep_port}")  # Backup listens on its REP_PORT

    def send_heartbeat(self):
        while True:
            try:
                self.heartbeat_socket.send_string("heartbeat")
                if self.heartbeat_socket.poll(1000):  # Wait for 1 second
                    response = self.heartbeat_socket.recv_string()
                    if response == "heartbeat_ack":
                        self.console_utils.print("Heartbeat successful: Dispatcher is active.", 2)
                else:
                    self.console_utils.print("Heartbeat failed: Dispatcher is inactive.", 3)
                    self.signal_backup()
            except zmq.ZMQError as e:
                self.console_utils.print(f"Heartbeat error: {e}", 3)
                self.signal_backup()
            time.sleep(5)  # Send heartbeat every 5 seconds

    def signal_backup(self):
        # Publish a message to the backup dispatcher to activate
        self.backup_socket.send_string("activate_backup")
        self.console_utils.print("Signaled backup dispatcher to activate.", 2)

    def run(self):
        self.send_heartbeat()

if __name__ == "__main__":
    heartbeat_service = HeartbeatService(DISPATCHER_IP, HEARTBEAT_PORT, BACKUP_REP_PORT)
    heartbeat_service.run()
