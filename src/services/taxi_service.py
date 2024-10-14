import zmq
import time
import os
from  threading import Event, Thread
from src.config import DISPATCHER_IP, PUB_PORT, SUB_PORT, REP_PORT
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
        self.zmq_utils = ZMQUtils(DISPATCHER_IP, PUB_PORT, SUB_PORT, REP_PORT)
        self.stop_event = Event()

    def connect_to_dispatcher(self, reconnect=False):
        connected = False
        retry_count = 0

        if reconnect:
            self.console_utils.print("Dispatcher inactive, attempting to reconnect...")
        else:
            self.console_utils.print("Connecting to Dispatcher...")
        
        while not connected:
            try:
                if reconnect:
                    self.zmq_utils.disconnect_pub()
                    self.zmq_utils.disconnect_sub()

                publisher = self.zmq_utils.connect_pub()
                self.zmq_utils.connect_sub(topic=str(self.taxi.taxi_id))

                requester = self.zmq_utils.connect_req()
                requester.send_string(f"connect_request {self.taxi.taxi_id} {self.taxi.pos_x} {self.taxi.pos_y} {self.taxi.speed} {self.taxi.status}")

                if requester.poll(1000):
                    response = requester.recv_string()
                    if response == "connect_ack":
                        if reconnect:
                            self.console_utils.print(f"Successfully reconnected to Dispatcher.", 4)
                            publisher.send_string(f"{self.taxi.taxi_id} {self.taxi.pos_x} {self.taxi.pos_y}")
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
            temp_requester.send_string(f"connect_request {self.taxi.taxi_id} {self.taxi.pos_x} {self.taxi.pos_y}")
            if temp_requester.poll(1000):
                response = temp_requester.recv_string()
                if response == "connect_ack":
                    temp_requester.close()
                    return True
            temp_requester.close()
        except zmq.ZMQError:
            pass
        return False

    def publish_position(self):
        self.zmq_utils.publisher.send_string(f"{self.taxi.taxi_id} {self.taxi.pos_x} {self.taxi.pos_y}")
        self.console_utils.print(f"Taxi {self.taxi.taxi_id} position published: ({self.taxi.pos_x}, {self.taxi.pos_y})", show_level=False)

    def receive_commands(self):
        try:
            while True:
                message = self.zmq_utils.subscriber.recv_string()
                self.console_utils.print(f"Received message for Taxi {self.taxi.taxi_id}: {message}", show_level=False)

        except zmq.ZMQError as e:
            self.console_utils.print(f"Error receiving message: {e}", 3)

    def run(self):
        if not validate_grid(self.grid.rows, self.grid.cols, self.console_utils) or not validate_initial_position(self.taxi.pos_x, self.taxi.pos_y, self.taxi.grid.rows, self.taxi.grid.cols, self.taxi.taxi_id, self.console_utils) or not validate_speed(self.taxi.speed, self.taxi.taxi_id, self.console_utils):
            self.console_utils.print(f"Taxi {self.taxi.taxi_id} failed to start due to invalid parameters.", 3)
            return

        try:
            self.connect_to_dispatcher()
            '''
            connect_to_dispatcher_thread = Thread(target=self.connect_to_dispatcher)
            publish_position_thread = Thread(target=self.publish_position)
            receive_commands_thread = Thread(target=self.receive_commands)

            connect_to_dispatcher_thread.daemon = False
            publish_position_thread.daemon = False
            receive_commands_thread.daemon = False

            connect_to_dispatcher_thread.start()
            publish_position_thread.start()
            receive_commands_thread.start()

            while not self.stop_event.is_set():
                connect_to_dispatcher_thread.join(timeout=1)
                publis_position_thread.join(timeout=1)
                receive_commands_thread.join(timeout=1)'''

        except KeyboardInterrupt:
            self.console_utils.print("Central Dispatcher process interrupted by user, terminating process...", 2)

        finally:
            self.console_utils.print(f"Taxi {self.taxi.taxi_id} process ended and resources cleaned up.", 4)
            os._exit(0)
