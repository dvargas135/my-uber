import zmq
import time
from src.config import (
    DISPATCHER_IP,
    BACKUP_DISPATCHER_IP,
    HEARTBEAT_3_PORT,
    BACKUP_ACTIVATION_PORT,
    HEARTBEAT_2_PORT
)
from src.utils.rich_utils import RichConsoleUtils

class HeartbeatService:
    def __init__(self, dispatcher_ip, backup_dispatcher_ip, heartbeat_port, backup_activation_port):
        self.dispatcher_ip = dispatcher_ip
        self.backup_dispatcher_ip = backup_dispatcher_ip
        self.heartbeat_port = heartbeat_port
        self.backup_activation_port = backup_activation_port
        self.console_utils = RichConsoleUtils()
        self.context = zmq.Context()
        self.heartbeat_2_port = HEARTBEAT_2_PORT
        
        self.heartbeat_socket = self.context.socket(zmq.REQ)
        self.heartbeat_socket.connect(f"tcp://{self.dispatcher_ip}:{self.heartbeat_port}")
        
        self.backup_socket = self.context.socket(zmq.PUSH)
        self.backup_socket.connect(f"tcp://{self.backup_dispatcher_ip}:{self.backup_activation_port}")

        self.main_active = True

    def send_heartbeat(self):
        """
        Sends heartbeats to the dispatcher and signals the backup dispatcher
        based on the main dispatcher's status.
        """
        try:
            deactivate_socket = self.context.socket(zmq.PUSH)
            deactivate_socket.connect(f"tcp://{self.backup_dispatcher_ip}:{self.heartbeat_2_port}")

            while True:
                try:
                    self.heartbeat_socket.send_string("heartbeat_srv")
                    if self.heartbeat_socket.poll(1000):  # Wait for 1 second for a response
                        response = self.heartbeat_socket.recv_string()
                        if response == "heartbeat_ack":
                            if not self.main_active:
                                print("Dispatcher is back online. Sending deactivate signal to backup.")
                                self.console_utils.print("Heartbeat successful: Dispatcher is active again.", level=2)
                                self.main_active = True
                                # Send deactivate signal via heartbeat_2_port
                                deactivate_socket.send_string("deactivate_backup")
                            else:
                                self.console_utils.print("Heartbeat successful: Dispatcher is active.", level=2)
                        else:
                            print(f"Unexpected response: {response}")
                            if self.main_active:
                                print("Unexpected response; marking dispatcher as inactive.")
                                self.main_active = False
                                # Send activate signal via backup_activation_port
                                self.signal_backup("activate_backup")
                    else:
                        if self.main_active:
                            self.console_utils.print("Heartbeat failed: Dispatcher is inactive.", level=3)
                            self.main_active = False
                            # Send activate signal via backup_activation_port
                            self.signal_backup("activate_backup")
                except zmq.ZMQError as e:
                    if self.main_active:
                        self.console_utils.print(f"Heartbeat error: {e}", level=3)
                        self.main_active = False
                        # Send activate signal via backup_activation_port
                        self.signal_backup("activate_backup")
                    self.heartbeat_socket.close()
                    self.heartbeat_socket = self.context.socket(zmq.REQ)
                    self.heartbeat_socket.connect(f"tcp://{self.dispatcher_ip}:{self.heartbeat_port}")
                except Exception as e:
                    print(f"Unexpected error: {e}")
                    if self.main_active:
                        self.console_utils.print(f"Unexpected error in heartbeat: {e}", level=3)
                        self.main_active = False
                        # Send activate signal via backup_activation_port
                        self.signal_backup("activate_backup")
                finally:
                    time.sleep(5)
        finally:
            deactivate_socket.close()

    def signal_backup(self, signal_type):
        """
        Sends signals to the backup dispatcher through the appropriate port.
        """
        try:
            if signal_type == "activate_backup":
                self.console_utils.print(f"Sending {signal_type} signal on backup_activation_port.", level=2)
                self.backup_socket.send_string(signal_type)  # Sent via backup_activation_port
                self.console_utils.print("Signaled backup dispatcher to activate.", level=2)
            elif signal_type == "deactivate_backup":
                self.console_utils.print(f"Sending {signal_type} signal on heartbeat_2_port.", level=2)
                # This is handled in `send_heartbeat` directly via deactivate_socket
            else:
                self.console_utils.print(f"Unknown signal type: {signal_type}", level=3)
        except zmq.ZMQError as e:
            self.console_utils.print(f"Error signaling backup dispatcher: {e}", level=3)



    def run(self):
        self.send_heartbeat()

if __name__ == "__main__":
    heartbeat_service = HeartbeatService(
        dispatcher_ip=DISPATCHER_IP,
        backup_dispatcher_ip=BACKUP_DISPATCHER_IP,
        heartbeat_port=HEARTBEAT_3_PORT,
        backup_activation_port=BACKUP_ACTIVATION_PORT
    )
    heartbeat_service.run()
