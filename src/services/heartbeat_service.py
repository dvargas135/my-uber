# src/services/heartbeat_service.py

import zmq
import time
from src.config import (
    DISPATCHER_IP,
    BACKUP_DISPATCHER_IP,  # This will no longer be used for activation signals
    HEARTBEAT_PORT,
    BACKUP_ACTIVATION_PORT
)
from src.utils.rich_utils import RichConsoleUtils

class HeartbeatService:
    def __init__(self, dispatcher_ip, backup_activation_port):
        """
        Initializes the HeartbeatService.

        :param dispatcher_ip: IP address of the main dispatcher.
        :param backup_activation_port: Port on which the backup dispatcher listens for activation signals.
        """
        self.dispatcher_ip = dispatcher_ip
        self.heartbeat_port = HEARTBEAT_PORT
        self.backup_activation_port = backup_activation_port
        self.console_utils = RichConsoleUtils()
        self.context = zmq.Context()
        
        # Initialize heartbeat socket (REQ) to communicate with the main dispatcher
        self.heartbeat_socket = self.context.socket(zmq.REQ)
        self.heartbeat_socket.connect(f"tcp://{self.dispatcher_ip}:{self.heartbeat_port}")
        
        # Initialize activation socket (PUB) to send signals to the backup dispatcher
        self.backup_socket = self.context.socket(zmq.PUB)
        self.backup_socket.bind(f"tcp://*:{self.backup_activation_port}")  # Bind PUB socket

    def send_heartbeat(self):
        """
        Continuously sends heartbeat messages to the main dispatcher.
        If the dispatcher does not respond, signals the backup dispatcher to activate.
        """
        while True:
            try:
                self.heartbeat_socket.send_string("heartbeat")
                if self.heartbeat_socket.poll(1000):  # Wait for 1 second for a response
                    response = self.heartbeat_socket.recv_string()
                    if response == "heartbeat_ack":
                        self.console_utils.print("Heartbeat successful: Dispatcher is active.", level=2)
                else:
                    self.console_utils.print("Heartbeat failed: Dispatcher is inactive.", level=3)
                    self.signal_backup()
            except zmq.ZMQError as e:
                self.console_utils.print(f"Heartbeat error: {e}", level=3)
                self.signal_backup()
            except Exception as e:
                self.console_utils.print(f"Unexpected error in heartbeat: {e}", level=3)
                self.signal_backup()
            time.sleep(5)  # Send heartbeat every 5 seconds

    def signal_backup(self):
        """
        Publishes an activation signal to the backup dispatcher to take over tasks.
        """
        try:
            self.backup_socket.send_string("activate_backup")
            self.console_utils.print("Signaled backup dispatcher to activate.", level=2)
        except zmq.ZMQError as e:
            self.console_utils.print(f"Error signaling backup dispatcher: {e}", level=3)

    def run(self):
        """
        Starts the heartbeat monitoring process.
        """
        self.send_heartbeat()

if __name__ == "__main__":
    heartbeat_service = HeartbeatService(
        dispatcher_ip=DISPATCHER_IP,
        backup_activation_port=BACKUP_ACTIVATION_PORT
    )
    heartbeat_service.run()
