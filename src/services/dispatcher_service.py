import zmq
import threading
from threading import Thread, Event
from src.models.system_model import System
from src.models.taxi_model import Taxi
from src.config import PUB_PORT, SUB_PORT, REP_PORT, DISPATCHER_IP, PULL_PORT
from src.utils.rich_utils import RichConsoleUtils
from src.utils.validation_utils import validate_grid
from src.utils.zmq_utils import ZMQUtils

class DispatcherService:
    def __init__(self, N, M):
        self.console_utils = RichConsoleUtils()
        self.system = System(N, M)
        self.zmq_utils = ZMQUtils(DISPATCHER_IP, PUB_PORT, SUB_PORT, REP_PORT, PULL_PORT)

        columns = ["Taxi ID", "Position X", "Position Y", "Speed", "Status", "Connected"]
        self.table = self.console_utils.create_table("Taxi Positions", columns)

        self.stop_event = Event()

    def handle_connection_request(self):
        responder = self.zmq_utils.bind_rep_socket()
        try:
            while not self.stop_event.is_set():
                try:
                    if responder.poll(100):
                        message = responder.recv_string()
                        if message.startswith("connect_request"):
                            _, taxi_id, pos_x, pos_y, speed, status = message.split()

                            if taxi_id not in self.system.taxis:
                                taxi_id = int(taxi_id)
                                pos_x = int(pos_x)
                                pos_y = int(pos_y)
                                speed = int(speed)
                                status = status

                                taxi = Taxi(taxi_id, self.system.grid.rows, self.system.grid.cols, pos_x, pos_y, speed, status, True)
                                taxi.initial_pos_x = pos_x
                                taxi.initial_pos_y = pos_y
                                self.system.register_taxi(taxi)
                                responder.send_string(f"connect_ack {taxi_id}")
                                print(f"Taxi {taxi_id} connected at ({pos_x}, {pos_y}) with speed {speed}.")
                            else:
                                taxi = self.system.taxis[taxi_id]
                                taxi.pos_x = pos_x
                                taxi.pos_y = pos_y
                                taxi.speed = speed
                                taxi.status = status
                                responder.send_string(f"connect_ack {taxi_id}")
                                print(f"Taxi {taxi_id} reconnected and updated.")

                            self.refresh_table()

                except zmq.Again:
                    pass
                except zmq.ZMQError as e:
                    if self.stop_event.is_set():
                        break
                    if not self.zmq_utils.context.closed:
                        self.console_utils.print(f"Error while handling connection request: {e}", 3)
        except zmq.ZMQError as e:
            if not self.zmq_utils.context.closed:
                self.console_utils.print(f"Error while handling connection request: {e}", 3)
        finally:
            if responder:
                responder.close()

    def receive_position_updates(self):
        try:
            puller = self.zmq_utils.bind_pull_socket()
            #subscriber = self.zmq_utils.connect_sub()
            while not self.stop_event.is_set():
                try:
                    message = puller.recv_string(zmq.NOBLOCK)
                    if message:
                        taxi_id, pos_x, pos_y, _, _ = message.split()
                        taxi_id = int(taxi_id)
                        pos_x = int(pos_x)
                        pos_y = int(pos_y)

                        if taxi_id in self.system.taxis:
                            self.system.update_taxi_position(taxi_id, pos_x, pos_y)
                            print(f"Updated Taxi {taxi_id} position to ({pos_x}, {pos_y})")
                        else:
                            print(f"Taxi {taxi_id} not found, cannot update position")

                        self.refresh_table()
                except zmq.Again:
                    pass
                except zmq.ZMQError as e:
                    if not self.zmq_utils.context.closed:
                        self.console_utils.print(f"Error while receiving position updates: {e}", 3)
        finally:
            if puller:
                puller.close()

    def refresh_table(self):
        taxi_data = [
            [
                taxi_id,
                taxi.pos_x,
                taxi.pos_y,
                taxi.speed,
                taxi.status,
                taxi.connected,
            ]
            for taxi_id, taxi in self.system.taxis.items()
        ]
        table = self.console_utils.generate_table("Taxi Positions", ["Taxi ID", "Position X", "Position Y", "Speed", "Status", "Connected"], taxi_data)
        self.live.update(table)

    def run(self):
        if not validate_grid(self.system.grid.rows, self.system.grid.cols, self.console_utils):
            self.console_utils.print(f"Dispatcher failed to start due to invalid parameters.", 3)
            return
        
        try:
            with self.console_utils.start_live_display(self.table) as live:
                self.live = live

                connection_thread = Thread(target=self.handle_connection_request)
                updates_thread = Thread(target=self.receive_position_updates)

                connection_thread.daemon = False
                updates_thread.daemon = False

                connection_thread.start()
                updates_thread.start()

                while not self.stop_event.is_set():
                    connection_thread.join(timeout=1)
                    updates_thread.join(timeout=1)

        except KeyboardInterrupt:
            self.console_utils.print("Central Dispatcher process interrupted by user.", 2)
            self.stop_event.set()

        finally:
            self.console_utils.print("Cleaning up dispatcher resources...", 2)
            connection_thread.join()
            updates_thread.join()
            self.zmq_utils.close()
            self.console_utils.print("Central Dispatcher process ended and resources cleaned up.", 4)
