import zmq
import time
from src.config import (
    DISPATCHER_IP,
    BACKUP_DISPATCHER_IP,
    HEARTBEAT_3_PORT,
    BACKUP_ACTIVATION_PORT
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
        
        self.heartbeat_socket = self.context.socket(zmq.REQ)
        self.heartbeat_socket.connect(f"tcp://{self.dispatcher_ip}:{self.heartbeat_port}")
        
        self.backup_socket = self.context.socket(zmq.PUSH)
        self.backup_socket.connect(f"tcp://{self.backup_dispatcher_ip}:{self.backup_activation_port}")

        self.main_active = True

    def send_heartbeat(self):
        while True:
            # print(f"Main dispatcher active state: {self.main_active}")  # Debug print
            try:
                # print("Sending heartbeat...")
                self.heartbeat_socket.send_string("heartbeat_srv")
                if self.heartbeat_socket.poll(1000):  # Wait for response
                    # print("Received response from dispatcher")
                    response = self.heartbeat_socket.recv_string()
                    if response == "heartbeat_ack":
                        # print("Dispatcher acknowledged heartbeat")
                        if not self.main_active:  # Transition back to active
                            print("Dispatcher is back online. Sending deactivate signal to backup.")
                            self.console_utils.print("Heartbeat successful: Dispatcher is active again.", level=2)
                            self.main_active = True  # Set to active
                            self.signal_backup("deactivate_backup")
                        else:
                            self.console_utils.print("Heartbeat successful: Dispatcher is active.", level=2)
                    else:
                        print(f"Unexpected response: {response}")
                        if self.main_active:
                            print("Unexpected response; marking dispatcher as inactive.")
                            self.main_active = False
                            self.signal_backup("activate_backup")
                else:
                    # print("No response received; marking dispatcher as inactive.")
                    if self.main_active:
                        self.console_utils.print("Heartbeat failed: Dispatcher is inactive.", level=3)
                        self.main_active = False
                        self.signal_backup("activate_backup")
            except zmq.ZMQError as e:
                # print(f"ZMQError: {e}")
                if self.main_active:
                    self.console_utils.print(f"Heartbeat error: {e}", level=3)
                    self.main_active = False
                    self.signal_backup("activate_backup")
                # Close and reconnect the socket to reset state
                # print("Reinitializing heartbeat socket...")
                self.heartbeat_socket.close()
                self.heartbeat_socket = self.context.socket(zmq.REQ)
                self.heartbeat_socket.connect(f"tcp://{self.dispatcher_ip}:{self.heartbeat_port}")
            except Exception as e:
                print(f"Unexpected error: {e}")
                if self.main_active:
                    self.console_utils.print(f"Unexpected error in heartbeat: {e}", level=3)
                    self.main_active = False
                    self.signal_backup("activate_backup")
            finally:
                # print("Waiting 5 seconds before next heartbeat...")
                time.sleep(5)

    def signal_backup(self, signal_type):
        try:
            self.backup_socket.send_string(signal_type)
            if signal_type == "activate_backup":
                self.console_utils.print("Signaled backup dispatcher to activate.", level=2)
                # print(f"{self.main_active}")
            elif signal_type == "deactivate_backup":
                self.console_utils.print("Signaled backup dispatcher to deactivate.", level=2)
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
