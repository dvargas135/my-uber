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
            try:
                self.heartbeat_socket.send_string("heartbeat_srv")
                if self.heartbeat_socket.poll(1000):
                    response = self.heartbeat_socket.recv_string()
                    if response == "heartbeat_ack":
                        if not self.main_active:
                            self.console_utils.print("Heartbeat successful: Dispatcher is active again.", level=2)
                            self.signal_backup("deactivate_backup")
                            self.main_active = True
                        else:
                            # self.console_utils.print("Heartbeat successful: Dispatcher is active.", level=2)
                    else:
                        if self.main_active:
                            self.console_utils.print(f"Unexpected response from dispatcher: {response}", level=3)
                            self.signal_backup("activate_backup")
                            self.main_active = False
                else:
                    if self.main_active:
                        self.console_utils.print("Heartbeat failed: Dispatcher is inactive.", level=3)
                        self.signal_backup("activate_backup")
                        self.main_active = False
            except zmq.ZMQError as e:
                if self.main_active:
                    self.console_utils.print(f"Heartbeat error: {e}", level=3)
                    self.signal_backup("activate_backup")
                    self.main_active = False
            except Exception as e:
                if self.main_active:
                    self.console_utils.print(f"Unexpected error in heartbeat: {e}", level=3)
                    self.signal_backup("activate_backup")
                    self.main_active = False
            time.sleep(5)


    def signal_backup(self, signal_type):
        try:
            self.backup_socket.send_string(signal_type)
            if signal_type == "activate_backup":
                self.console_utils.print("Signaled backup dispatcher to activate.", level=2)
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
