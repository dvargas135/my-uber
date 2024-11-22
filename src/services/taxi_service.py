import zmq
import time
import os
import random
from threading import Event, Thread, Lock, Condition
from src.config import DISPATCHER_IP, PUB_PORT, SUB_PORT, REP_PORT, PULL_PORT, HEARTBEAT_PORT, BACKUP_DISPATCHER_IP, HEARTBEAT_2_PORT
from src.models.taxi_model import Taxi
from src.utils.rich_utils import RichConsoleUtils
from src.models.grid_model import Grid
from src.utils.validation_utils import validate_grid, validate_initial_position, validate_speed
from src.utils.zmq_utils import ZMQUtils
from src.config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

class TaxiService:
    def __init__(self, taxi_id, pos_x, pos_y, speed, N, M, status):
        self.grid = Grid(N, M)
        self.taxi = Taxi(taxi_id, self.grid.rows, self.grid.cols, pos_x, pos_y, speed, status)
        self.dispatcher_ip = DISPATCHER_IP
        self.backup_dispatcher_ip = BACKUP_DISPATCHER_IP

        self.console_utils = RichConsoleUtils()
        self.zmq_utils = ZMQUtils(DISPATCHER_IP, PUB_PORT, SUB_PORT, REP_PORT, PULL_PORT, HEARTBEAT_PORT, HEARTBEAT_2_PORT)
        self.stop_event = Event()
        self.msg = f"{self.taxi.taxi_id} {self.taxi.pos_x} {self.taxi.pos_y} {self.taxi.speed} {self.taxi.status}"

        self.heartbeat_pusher = self.zmq_utils.connect_push_heartbeat()
        self.socket_lock = Lock()
        self.socket_ready = Condition()
        self.socket_initialized = False
        self.connected = False
        self.main_dispatcher_offline = False
        self.pub_port = PUB_PORT
    
    def connect_to_backup_dispatcher(self, reconnect=False):
        self.console_utils.print("Main dispatcher is offline, connecting to backup server")
        self.zmq_utils.dispatcher_ip = BACKUP_DISPATCHER_IP
        # connected = False
        retry_count = 0

        if reconnect:
            self.console_utils.print("Backup dispatcher inactive, attempting to reconnect...")
        else:
            self.console_utils.print("Connecting to Backup Dispatcher...")
        
        while not self.connected and not self.stop_event.is_set():
            try:
                with self.socket_lock:  # Synchronize socket recreation
                    if reconnect:
                        #self.console_utils.print("Recreating all sockets for reconnection...", 2)
                        with self.socket_ready:
                            self.socket_initialized = False
                        self.zmq_utils.recreate_all_sockets(taxi_id=self.taxi.taxi_id, topic=str(self.taxi.taxi_id))

                    self.zmq_utils.connect_push()
                    self.zmq_utils.connect_sub(topic=str(self.taxi.taxi_id))

                    requester = self.zmq_utils.connect_req()
                    requester.send_string(f"connect_request {self.msg}")

                if requester.poll(1000):
                    response = requester.recv_string()
                    if response == f"connect_ack {self.taxi.taxi_id}":
                        if reconnect:
                            self.console_utils.print(f"Successfully reconnected to Backup Dispatcher.", 4)
                            with self.socket_lock:
                                requester.send_string(f"{self.msg}")
                            self.console_utils.print(f"Last position sent successfully: {self.taxi.pos_x} {self.taxi.pos_y}.", 4)
                            self.connected = True
                        else:
                            self.console_utils.print(f"Successfully connected to Backup Dispatcher as Taxi {self.taxi.taxi_id}", 4)
                        self.connected = True
                    else:
                        self.console_utils.print(f"Unexpected response from backup dispatcher: {response}", 3)
                else:
                    retry_count += 1
                    self.console_utils.print(
                        f"{'Re' if reconnect else ''}Connection attempt failed, retrying... [{retry_count}]",
                        3, end="\r"
                    )
                    self.connect_to_dispatcher(reconnect=True)
                    time.sleep(2)

                requester.close()

            except zmq.ZMQError as e:
                retry_count += 1
                self.console_utils.print(
                    f"Connection error on attempt {retry_count}: {e}. "
                    f"Socket state: PUSH={self.zmq_utils.pusher}, "
                    f"REQ={self.zmq_utils.requester}, "
                    f"SUB={self.zmq_utils.subscriber}",
                    3
                )
                time.sleep(2)
            except Exception as e:
                self.console_utils.print(
                    f"Unexpected error during connection attempt: {e}", 3
                )
                time.sleep(2)

    def connect_to_dispatcher(self, reconnect=False):
        self.zmq_utils.dispatcher_ip = DISPATCHER_IP
        # connected = False
        retry_count = 0

        if reconnect:
            self.console_utils.print("Dispatcher inactive, attempting to reconnect...")
            self.connected = False
        else:
            self.console_utils.print("Connecting to Dispatcher...")
        
        while not self.connected and not self.stop_event.is_set():
            try:
                with self.socket_lock:  # Synchronize socket recreation
                    if reconnect:
                        self.console_utils.print("Recreating all sockets for reconnection...", 2)
                        with self.socket_ready:
                            self.socket_initialized = False
                        self.zmq_utils.recreate_all_sockets(taxi_id=self.taxi.taxi_id, topic=str(self.taxi.taxi_id))

                    self.zmq_utils.connect_push()
                    self.zmq_utils.connect_sub(topic=str(self.taxi.taxi_id))

                    requester = self.zmq_utils.connect_req()
                    requester.send_string(f"connect_request {self.msg}")

                if requester.poll(1000):
                    response = requester.recv_string()
                    if response == f"connect_ack {self.taxi.taxi_id}":
                        if reconnect:
                            self.console_utils.print(f"Successfully reconnected to Dispatcher.", 4)
                            with self.socket_lock:
                                requester.send_string(f"{self.msg}")
                            self.console_utils.print(f"Last position sent successfully: {self.taxi.pos_x} {self.taxi.pos_y}.", 4)
                            self.connected = True
                        else:
                            self.console_utils.print(f"Successfully connected to Dispatcher as Taxi {self.taxi.taxi_id}", 4)
                        self.connected = True
                    else:
                        self.console_utils.print(f"Unexpected response from dispatcher: {response}", 3)
                else:
                    retry_count += 1
                    self.console_utils.print(
                        f"{'Re' if reconnect else ''}Connection attempt failed, retrying... [{retry_count}]",
                        3, end="\r"
                    )
                    time.sleep(2)

                    if retry_count == 5:
                        self.main_dispatcher_offline = True
                        self.connect_to_backup_dispatcher()

                requester.close()

            except zmq.ZMQError as e:
                retry_count += 1
                self.console_utils.print(
                    f"Connection error on attempt {retry_count}: {e}. "
                    f"Socket state: PUSH={self.zmq_utils.pusher}, "
                    f"REQ={self.zmq_utils.requester}, "
                    f"SUB={self.zmq_utils.subscriber}",
                    3
                )
                time.sleep(2)
            except Exception as e:
                self.console_utils.print(
                    f"Unexpected error during connection attempt: {e}", 3
                )
                time.sleep(2)
    
    def dispatcher_active(self):
        try:
            temp_requester = self.zmq_utils.connect_req()
            temp_requester.send_string(f"connect_request {self.msg}")
            if temp_requester.poll(1000):  # Wait for a response
                response = temp_requester.recv_string()
                if response == f"connect_ack {self.taxi.taxi_id}":
                    temp_requester.close()
                    return True
            temp_requester.close()
        except zmq.ZMQError as e:
            self.console_utils.print(f"Dispatcher check error: {e}", 3)
        # self.connected = False
        return False
    
    def publish_position(self):
        try:
            time.sleep(5)
            while not self.stop_event.is_set() and not self.taxi.stopped:
                # if not self.dispatcher_active():
                #     print("send_heartbeat not self.publish_position()")
                #     self.connect_to_dispatcher(reconnect=True)
                try:
                    self.taxi.move_counter += 1

                    if self.taxi.speed == 4:
                        cells_to_move = 2
                    elif self.taxi.speed == 2:
                        cells_to_move = 1
                    elif self.taxi.speed == 1:
                        cells_to_move = 1 if self.taxi.move_counter % 2 == 0 else 0
                    else:
                        cells_to_move = 0

                    if cells_to_move > 0:
                        directions = ['NORTH', 'SOUTH', 'EAST', 'WEST']
                        valid_directions = []

                        for direction in directions:
                            if self.taxi.can_move(direction):
                                valid_directions.append(direction)

                        if not valid_directions:
                            self.console_utils.print(f"Taxi {self.taxi.taxi_id} cannot move, stopping.", level=3)
                            self.taxi.stopped = True
                            break

                        direction = random.choice(valid_directions)
                        self.taxi.move(direction, cells_to_move)

                        if not self.dispatcher_active():
                            # self.console_utils.print("Dispatcher inactive. Attempting to reconnect.", 3)
                            self.connect_to_dispatcher(reconnect=True)
                            continue

                        self.msg = f"{self.taxi.taxi_id} {self.taxi.pos_x} {self.taxi.pos_y} {self.taxi.speed} {self.taxi.status}"
                        self.zmq_utils.pusher.send_string(f"{self.msg}")

                        if self.taxi.stopped:
                            self.console_utils.print(
                                f"Taxi {self.taxi.taxi_id} moved {direction} to ({self.taxi.pos_x}, {self.taxi.pos_y}) and has stopped moving.",
                                level=2
                            )
                        else:
                            self.console_utils.print(
                                f"Taxi {self.taxi.taxi_id} moved {direction} to ({self.taxi.pos_x}, {self.taxi.pos_y})",
                                level=2
                            )
                    else:
                        self.console_utils.print(f"Taxi {self.taxi.taxi_id} did not move this interval.", level=2)

                    time.sleep(5)
                except zmq.ZMQError as e:
                    self.console_utils.print(f"Error publishing position: {e}", level=3)
                    self.console_utils.print("Attempting to reconnect to dispatcher...", level=1)
                    self.connect_to_dispatcher(reconnect=True)
                except Exception as e:
                    self.console_utils.print(f"Unexpected error in publish_position: {e}", level=3)
                    time.sleep(2)
        except Exception as e:
            self.console_utils.print(f"Fatal error in publish_position: {e}", level=3)
            self.stop_event.set()

    def receive_commands(self):
        try:
            while not self.stop_event.is_set():
                try:
                    # Wait for sockets to be ready
                    with self.socket_ready:
                        while not self.socket_initialized:
                            self.socket_ready.wait()

                    if self.zmq_utils.subscriber and self.zmq_utils.subscriber.poll(1000):  # Check for None
                        message = self.zmq_utils.subscriber.recv_string()
                        self.console_utils.print(f"Received message for Taxi {self.taxi.taxi_id}: {message}", show_level=False)
                except zmq.ZMQError as e:
                    self.console_utils.print(f"Error receiving message: {e}", 3, end="\r")
        except zmq.ZMQError as e:
            self.console_utils.print(f"Error receiving message: {e}", 3, end="\r")

    def subscribe_to_assignments(self):
        if hasattr(self, "subscriber") and self.subscriber:  # Check if the subscriber already exists
            self.subscriber.close()
        subscriber = self.zmq_utils.context.socket(zmq.SUB)
        subscriber.connect(f"tcp://{self.dispatcher_ip}:{self.pub_port}")
        subscriber.setsockopt_string(zmq.SUBSCRIBE, f"assign {self.taxi.taxi_id}")
        while not self.stop_event.is_set():
            try:
                message = subscriber.recv_string(flags=zmq.NOBLOCK)
                if message.startswith(f"assign_taxi {self.taxi.taxi_id}"):
                    _, taxi_id, user_id = message.split()
                    self.handle_assignment(user_id)
            except zmq.Again:
                time.sleep(0.1)
            except Exception as e:
                self.console_utils.print(f"Error in subscribing to assignments: {e}", 3)
        subscriber.close()

    def handle_assignment(self, user_id):
        self.console_utils.print(f"Taxi {self.taxi.taxi_id} assigned to User {user_id}", 2)
    
    def send_heartbeat(self):
        while not self.stop_event.is_set():
            try:
                heartbeat_msg = f"heartbeat {self.taxi.taxi_id}"
                self.heartbeat_pusher.send_string(heartbeat_msg)
                # self.console_utils.print(f"Sent heartbeat from Taxi {self.taxi.taxi_id}", show_level=False)
                time.sleep(5)
            except zmq.ZMQError as e:
                self.console_utils.print(f"Error sending heartbeat: {e}", 3)
                time.sleep(5)
            except Exception as e:
                self.console_utils.print(f"Unexpected error in send_heartbeat: {e}", 3)
                time.sleep(5)

    def run(self):
        if not validate_grid(self.grid.rows, self.grid.cols, self.console_utils) or not validate_initial_position(self.taxi.pos_x, self.taxi.pos_y, self.taxi.grid.rows, self.taxi.grid.cols, self.taxi.taxi_id, self.console_utils) or not validate_speed(self.taxi.speed, self.taxi.taxi_id, self.console_utils):
            self.console_utils.print(f"Taxi {self.taxi.taxi_id} failed to start due to invalid parameters.", 3)
            return

        try:
            self.connect_to_dispatcher()
            
            publish_position_thread = Thread(target=self.publish_position)
            receive_commands_thread = Thread(target=self.receive_commands)
            heartbeat_thread = Thread(target=self.send_heartbeat)
            subscribe_to_assignments_thread = Thread(target=self.subscribe_to_assignments)

            publish_position_thread.daemon = False
            receive_commands_thread.daemon = False
            heartbeat_thread.daemon = False
            subscribe_to_assignments_thread.daemon = False

            publish_position_thread.start()
            receive_commands_thread.start()
            heartbeat_thread.start()
            subscribe_to_assignments_thread.start()

            while not self.stop_event.is_set():
                publish_position_thread.join(timeout=1)
                receive_commands_thread.join(timeout=1)
                heartbeat_thread.join(timeout=1)
                subscribe_to_assignments_thread.join(timeout=1)
            
        except KeyboardInterrupt:
            self.console_utils.print(f"Taxi {self.taxi.taxi_id} process interrupted by user, terminating process...", 2)
            self.stop_event.set()

        finally:
            self.console_utils.print(f"Taxi {self.taxi.taxi_id} process ended and resources cleaned up.", 4)
            os._exit(0)
