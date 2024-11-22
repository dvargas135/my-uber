import zmq
import threading
import time
import csv
from threading import Thread, Event
from src.config import DISPATCHER_IP, USER_REQ_PORT
from src.utils.rich_utils import RichConsoleUtils

class UserThread(Thread):
    def __init__(self, user_id, pos_x, pos_y, waiting_time, dispatcher_ip, backup_dispatcher_ip, user_req_port, backup_user_req_port, console_utils, stop_event):
        super().__init__()
        self.user_id = user_id
        self.pos_x = pos_x
        self.pos_y = pos_y
        self.waiting_time = waiting_time
        self.dispatcher_ip = dispatcher_ip
        self.backup_dispatcher_ip = backup_dispatcher_ip
        self.user_req_port = user_req_port
        self.backup_user_req_port = backup_user_req_port
        self.console_utils = console_utils
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.stop_event = stop_event
        self.use_backup = False  # Track if using backup dispatcher
        self.connect_to_dispatcher()

    def connect_to_dispatcher(self):
        self.socket.close()  # Close any existing socket
        self.socket = self.context.socket(zmq.REQ)
        dispatcher_ip = self.backup_dispatcher_ip if self.use_backup else self.dispatcher_ip
        user_req_port = self.backup_user_req_port if self.use_backup else self.user_req_port
        self.socket.connect(f"tcp://{dispatcher_ip}:{user_req_port}")
        self.console_utils.print(f"User {self.user_id} connected to {'backup' if self.use_backup else 'main'} dispatcher.", 2)

    def switch_to_backup(self):
        self.use_backup = True
        self.console_utils.print(f"User {self.user_id} switching to backup dispatcher.", 3)
        self.connect_to_dispatcher()

    def run(self):
        try:
            self.console_utils.print(f"User {self.user_id} at ({self.pos_x}, {self.pos_y}) will request a taxi in {self.waiting_time} minutes.", 2)
            
            if self.stop_event.wait(timeout=self.waiting_time):
                self.console_utils.print(f"User {self.user_id} was interrupted before sending request.", 2)
                return

            request_message = f"user_request {self.user_id} {self.pos_x} {self.pos_y}"
            start_time = time.time()
            self.socket.send_string(request_message)
            self.console_utils.print(f"User {self.user_id} sent request to dispatcher.", 2)

            poller = zmq.Poller()
            poller.register(self.socket, zmq.POLLIN)
            socks = dict(poller.poll(30000))

            if socks.get(self.socket) == zmq.POLLIN:
                reply = self.socket.recv_string()
                end_time = time.time()
                response_time = end_time - start_time
                if reply.startswith("assign_taxi"):
                    _, taxi_id = reply.split()
                    self.console_utils.print(f"User {self.user_id} assigned to Taxi {taxi_id}. Response time: {response_time:.2f} seconds.", 2)
                elif reply.startswith("no_taxi_available"):
                    self.console_utils.print(f"User {self.user_id} could not be assigned a taxi. Reason: {reply}", 3)
                else:
                    self.console_utils.print(f"User {self.user_id} received unexpected reply: {reply}", 3)
            else:
                self.console_utils.print(f"User {self.user_id} request timed out after 5 seconds. Switching to backup dispatcher.", 3)
                self.switch_to_backup()
                self.run()  # Retry with backup dispatcher
        except Exception as e:
            self.console_utils.print(f"Error in User {self.user_id}: {e}", 3)
        finally:
            self.socket.close()
            self.context.term()


class UserService:
    def __init__(self, users_file, dispatcher_ip, backup_dispatcher_ip, user_req_port, backup_user_req_port):
        self.users_file = users_file
        self.dispatcher_ip = dispatcher_ip
        self.backup_dispatcher_ip = backup_dispatcher_ip
        self.user_req_port = user_req_port
        self.backup_user_req_port = backup_user_req_port
        self.console_utils = RichConsoleUtils()
        self.stop_event = Event()

    def load_users(self):
        users = []
        try:
            with open(self.users_file, 'r') as file:
                reader = csv.DictReader(file, fieldnames=['id', 'x', 'y', 'waiting_time'])
                for row in reader:
                    try:
                        user_id = int(row['id'])
                        pos_x = int(row['x'])
                        pos_y = int(row['y'])
                        waiting_time = int(row['waiting_time'])
                        users.append((user_id, pos_x, pos_y, waiting_time))
                    except ValueError as ve:
                        self.console_utils.print(f"Invalid data format in row: {row}. Error: {ve}", 3)
        except Exception as e:
            self.console_utils.print(f"Error reading users file: {e}", 3)
        return users

    def run(self):
        users = self.load_users()
        threads = []
        try:
            for user in users:
                user_thread = UserThread(
                    user_id=user[0],
                    pos_x=user[1],
                    pos_y=user[2],
                    waiting_time=user[3],
                    dispatcher_ip=self.dispatcher_ip,
                    backup_dispatcher_ip=self.backup_dispatcher_ip,
                    user_req_port=self.user_req_port,
                    backup_user_req_port=self.backup_user_req_port,
                    console_utils=self.console_utils,
                    stop_event=self.stop_event
                )
                user_thread.start()
                threads.append(user_thread)
            
            for thread in threads:
                thread.join()
            
            self.console_utils.print("All user requests have been processed.", 2)
        except KeyboardInterrupt:
            self.console_utils.print("UserService interrupted by user. Terminating user requests...", 2)
            self.stop_event.set()
        finally:
            self.console_utils.print("Cleaning up user service resources...", 2)
            for thread in threads:
                if thread.is_alive():
                    thread.join(timeout=1)
            self.console_utils.print("UserService process ended and resources cleaned up.", 4)
