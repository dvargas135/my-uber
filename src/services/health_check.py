# src/utils/health_check.py

import zmq
import threading
import time

class HealthCheck:
    def __init__(self, role, dispatchers, heartbeat_port, system=None, console_utils=None):
        self.role = role
        self.dispatchers = dispatchers
        self.heartbeat_port = heartbeat_port
        self.system = system
        self.console_utils = console_utils
        self.stop_event = threading.Event()

        self.context = zmq.Context()
        if self.role == "dispatcher":
            # Dispatcher uses PULL socket to receive heartbeats
            self.heartbeat_puller = self.context.socket(zmq.PULL)
            self.heartbeat_puller.bind(f"tcp://*:{self.heartbeat_port}")
        elif self.role == "taxi":
            # Taxi uses PUSH sockets to send heartbeats to dispatchers
            self.heartbeat_pushers = {
                dispatcher_ip: self.context.socket(zmq.PUSH) for dispatcher_ip in self.dispatchers
            }
            for dispatcher_ip in self.dispatchers:
                self.heartbeat_pushers[dispatcher_ip].connect(f"tcp://{dispatcher_ip}:{self.heartbeat_port}")
            self.current_dispatcher = self.dispatchers[0]  # Start with main dispatcher
        else:
            raise ValueError("Invalid role for HealthCheck. Must be 'dispatcher' or 'taxi'.")

    # -------------------------
    # Dispatcher Role Methods
    # -------------------------
    def receive_heartbeat(self):
        while not self.stop_event.is_set():
            try:
                message = self.heartbeat_puller.recv_string(flags=zmq.NOBLOCK)
                if message.startswith("heartbeat"):
                    _, taxi_id = message.split()
                    taxi_id = int(taxi_id)
                    # Update the last heartbeat timestamp
                    with self.system.heartbeat_lock:
                        self.system.heartbeat_timestamps[taxi_id] = time.time()
                        # Mark the taxi as connected
                        if taxi_id in self.system.taxis:
                            self.system.taxis[taxi_id].connected = True
                            self.console_utils.print(f"Received heartbeat from Taxi {taxi_id}", show_level=False)
                    # Optionally, refresh the table
                    self.system.dispatcher_service.refresh_table()
            except zmq.Again:
                time.sleep(0.1)  # No message received, sleep briefly
            except zmq.ZMQError as e:
                if not self.context.closed:
                    self.console_utils.print(f"Error while receiving heartbeat: {e}", 3)
                break
            except Exception as e:
                self.console_utils.print(f"Unexpected error in receive_heartbeat: {e}", 3)

    def monitor_heartbeats(self, interval=5, timeout=15):
        while not self.stop_event.is_set():
            current_time = time.time()
            with self.system.heartbeat_lock:
                for taxi_id, last_hb in list(self.system.heartbeat_timestamps.items()):
                    if current_time - last_hb > timeout:
                        if taxi_id in self.system.taxis:
                            self.system.taxis[taxi_id].connected = False
                            self.console_utils.print(f"Taxi {taxi_id} disconnected due to missed heartbeats.", 3)
                        del self.system.heartbeat_timestamps[taxi_id]
            time.sleep(interval)

    def start_dispatcher_health_check(self):
        if self.role != "dispatcher":
            raise ValueError("start_dispatcher_health_check can only be called for dispatcher role.")
        
        self.receive_thread = threading.Thread(target=self.receive_heartbeat, name="HeartbeatReceiver")
        self.monitor_thread = threading.Thread(target=self.monitor_heartbeats, name="HeartbeatMonitor")
        self.receive_thread.start()
        self.monitor_thread.start()

    # -------------------------
    # Taxi Role Methods
    # -------------------------
    def send_heartbeat(self, taxi_id, interval=5):
        while not self.stop_event.is_set():
            try:
                heartbeat_msg = f"heartbeat {taxi_id}"
                self.heartbeat_pushers[self.current_dispatcher].send_string(heartbeat_msg)
                self.console_utils.print(f"Sent heartbeat to {self.current_dispatcher} from Taxi {taxi_id}", show_level=False)
                time.sleep(interval)
            except zmq.ZMQError as e:
                self.console_utils.print(f"Error sending heartbeat to {self.current_dispatcher}: {e}", 3)
                self.switch_to_backup()
                time.sleep(interval)
            except Exception as e:
                self.console_utils.print(f"Unexpected error in send_heartbeat: {e}", 3)
                time.sleep(interval)

    def switch_to_backup(self):
        if self.current_dispatcher != self.dispatchers[-1]:
            backup_dispatcher = self.dispatchers[-1]
            self.current_dispatcher = backup_dispatcher
            self.console_utils.print(f"Switched to backup dispatcher: {backup_dispatcher}", 2)
        else:
            self.console_utils.print("Already using the backup dispatcher.", 3)

    def monitor_dispatcher_availability(self, check_interval=10, reconnection_interval=15):
        main_dispatcher = self.dispatchers[0]
        while not self.stop_event.is_set():
            if self.current_dispatcher != main_dispatcher:
                # Attempt to reconnect to main dispatcher
                try:
                    # Test connection by sending a test heartbeat
                    test_msg = "heartbeat_test"
                    self.heartbeat_pushers[main_dispatcher].send_string(test_msg, zmq.NOBLOCK)
                    self.console_utils.print(f"Reconnected to main dispatcher: {main_dispatcher}", 2)
                    self.current_dispatcher = main_dispatcher
                except zmq.ZMQError:
                    self.console_utils.print(f"Main dispatcher {main_dispatcher} still unavailable.", 3)
            time.sleep(check_interval)

    def start_taxi_health_check(self, taxi_id, interval=5):
        if self.role != "taxi":
            raise ValueError("start_taxi_health_check can only be called for taxi role.")
        
        self.send_thread = threading.Thread(target=self.send_heartbeat, args=(taxi_id, interval), name="HeartbeatSender")
        self.monitor_thread = threading.Thread(target=self.monitor_dispatcher_availability, name="DispatcherMonitor")
        self.send_thread.start()
        self.monitor_thread.start()

    def stop(self):
        self.stop_event.set()
        if self.role == "dispatcher":
            self.receive_thread.join()
            self.monitor_thread.join()
            self.heartbeat_puller.close()
        elif self.role == "taxi":
            self.send_thread.join()
            self.monitor_thread.join()
            for pusher in self.heartbeat_pushers.values():
                pusher.close()
        self.context.term()
