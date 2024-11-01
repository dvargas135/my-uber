import zmq
import time
import os
import random
from threading import Event, Thread
from src.config import DISPATCHER_IP, PUB_PORT, SUB_PORT, REP_PORT, PULL_PORT
from src.models.taxi_model import Taxi
from src.utils.rich_utils import RichConsoleUtils
from src.models.grid_model import Grid
from src.utils.validation_utils import validate_grid, validate_initial_position, validate_speed
from src.utils.zmq_utils import ZMQUtils

class TaxiService:
    def __init__(self, taxi_id, pos_x, pos_y, speed, N, M, status):
        self.grid = Grid(N, M)
        self.taxi = Taxi(taxi_id, self.grid.rows, self.grid.cols, pos_x, pos_y, speed, status)

        self.console_utils = RichConsoleUtils()
        self.zmq_utils = ZMQUtils(DISPATCHER_IP, PUB_PORT, SUB_PORT, REP_PORT, PULL_PORT)
        self.stop_event = Event()
        self.msg = f"{self.taxi.taxi_id} {self.taxi.pos_x} {self.taxi.pos_y} {self.taxi.speed} {self.taxi.status}"

    def connect_to_dispatcher(self, reconnect=False):
        connected = False
        retry_count = 0

        if reconnect:
            self.console_utils.print("Dispatcher inactive, attempting to reconnect...")
        else:
            self.console_utils.print("Connecting to Dispatcher...")
        
        while not connected and not self.stop_event.is_set():
            try:
                if reconnect:
                    self.zmq_utils.disconnect_pub()
                    self.zmq_utils.disconnect_sub()

                #self.zmq_utils.connect_pub()
                self.zmq_utils.connect_push()
                self.zmq_utils.connect_sub(topic=str(self.taxi.taxi_id))

                requester = self.zmq_utils.connect_req()
                requester.send_string(f"connect_request {self.msg}")

                if requester.poll(1000):
                    response = requester.recv_string()
                    if response == f"connect_ack {self.taxi.taxi_id}":
                        if reconnect:
                            self.console_utils.print(f"Successfully reconnected to Dispatcher.", 4)
                            requester.send_string(f"{self.msg}")
                            self.console_utils.print(f"Last position sent successfully: {self.taxi.pos_x} {self.taxi.pos_y}.", 4)
                        else:
                            self.console_utils.print(f"Successfully connected to Dispatcher as Taxi {self.taxi.taxi_id}", 4)
                        connected = True
                    else:
                        self.console_utils.print(f"Unexpected response from dispatcher: {response}", 3)
                else:
                    retry_count += 1
                    if reconnect:
                        self.console_utils.print(f"Reconnection attempt failed, retrying... [{retry_count}]", 3, end="\r")
                    else:
                        self.console_utils.print(f"Connection attempt failed, retrying... [{retry_count}]", 3, end="\r")
                    time.sleep(2)

                requester.close()
                
                if not connected:
                    self.zmq_utils.disconnect_pub()
                    self.zmq_utils.disconnect_sub()
                    time.sleep(2)

            except zmq.ZMQError as e:
                retry_count += 1
                self.console_utils.print(f"Connection error: {e}, retrying... [{retry_count}]", 3, end="\r")
                time.sleep(2)
    
    def dispatcher_active(self):
        try:
            temp_requester = self.zmq_utils.connect_req()
            temp_requester.send_string(f"connect_request {self.msg}")
            if temp_requester.poll(1000):
                response = temp_requester.recv_string()
                if response == "connect_ack {self.taxi.taxi_id}":
                    temp_requester.close()
                    return True
            temp_requester.close()
        except zmq.ZMQError:
            pass
        return False
    
    def publish_position(self):
        try:
            time.sleep(30)
            while not self.stop_event.is_set() and not self.taxi.stopped:
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
                            self.console_utils.print(f"Taxi {self.taxi.taxi_id} cannot move, stopping.")
                            self.taxi.stopped = True
                            break

                        direction = random.choice(valid_directions)
                        self.taxi.move(direction, cells_to_move)

                        self.msg = f"{self.taxi.taxi_id} {self.taxi.pos_x} {self.taxi.pos_y} {self.taxi.speed} {self.taxi.status}"
                        self.zmq_utils.pusher.send_string(f"{self.msg}")

                        if self.taxi.stopped:
                            self.console_utils.print(f"Taxi {self.taxi.taxi_id} moved {direction} to ({self.taxi.pos_x}, {self.taxi.pos_y}) and has stopped moving.")
                        else:
                            self.console_utils.print(f"Taxi {self.taxi.taxi_id} moved {direction} to ({self.taxi.pos_x}, {self.taxi.pos_y})")
                    else:
                        self.console_utils.print(f"Taxi {self.taxi.taxi_id} did not move this interval.")

                except zmq.ZMQError as e:
                    self.console_utils.print(f"Error publishing position: {e}", 3)
                    self.console_utils.print("Attempting to reconnect to dispatcher...", 1)
                    self.connect_to_dispatcher(reconnect=True)
                except Exception as e:
                    self.console_utils.print(f"Unexpected error in publish_position: {e}", 3)
                    time.sleep(2)
        except Exception as e:
            self.console_utils.print(f"Fatal error in publish_position: {e}", 3)
            self.stop_event.set()

    def receive_commands(self):
        try:
            while not self.stop_event.is_set():
                try:
                    if self.zmq_utils.subscriber.poll(1000):
                        message = self.zmq_utils.subscriber.recv_string()
                        self.console_utils.print(f"Received message for Taxi {self.taxi.taxi_id}: {message}", show_level=False)
                except zmq.ZMQError as e:
                    self.console_utils.print(f"Error receiving message: {e}", 3, end="\r")
        except zmq.ZMQError as e:
            self.console_utils.print(f"Error receiving message: {e}", 3, end="\r")

    def run(self):
        if not validate_grid(self.grid.rows, self.grid.cols, self.console_utils) or not validate_initial_position(self.taxi.pos_x, self.taxi.pos_y, self.taxi.grid.rows, self.taxi.grid.cols, self.taxi.taxi_id, self.console_utils) or not validate_speed(self.taxi.speed, self.taxi.taxi_id, self.console_utils):
            self.console_utils.print(f"Taxi {self.taxi.taxi_id} failed to start due to invalid parameters.", 3)
            return

        try:
            self.connect_to_dispatcher()
            
            publish_position_thread = Thread(target=self.publish_position)
            receive_commands_thread = Thread(target=self.receive_commands)

            publish_position_thread.daemon = False
            receive_commands_thread.daemon = False

            publish_position_thread.start()
            receive_commands_thread.start()

            while not self.stop_event.is_set():
                publish_position_thread.join(timeout=1)
                receive_commands_thread.join(timeout=1)
            
        except KeyboardInterrupt:
            self.console_utils.print(f"Taxi {self.taxi.taxi_id} process interrupted by user, terminating process...", 2)
            self.stop_event.set()

        finally:
            self.console_utils.print(f"Taxi {self.taxi.taxi_id} process ended and resources cleaned up.", 4)
            os._exit(0)
