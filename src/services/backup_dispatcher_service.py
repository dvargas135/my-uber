import zmq
import threading
import time
from threading import Thread, Event, Lock
from src.models.system_model import System
from src.models.taxi_model import Taxi
from src.config import PUB_PORT, SUB_PORT, REP_PORT, BACKUP_DISPATCHER_IP, PULL_PORT, HEARTBEAT_PORT, BACKUP_USER_REQ_PORT, DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, BACKUP_ACTIVATION_PORT, HEARTBEAT_2_PORT
from src.utils.rich_utils import RichConsoleUtils
from src.utils.validation_utils import validate_grid
from src.utils.zmq_utils import ZMQUtils
from src.utils.db_handler import DatabaseHandler
from src.services.database_service import DatabaseService
from src.config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

class BackupDispatcherService:
    def __init__(self, N, M):
        self.console_utils = RichConsoleUtils()
        self.system = System(N, M)  # Ensure System class is defined
        self.zmq_utils = ZMQUtils(BACKUP_DISPATCHER_IP, PUB_PORT, SUB_PORT, REP_PORT, PULL_PORT, HEARTBEAT_PORT, HEARTBEAT_2_PORT)

        columns = ["Taxi ID", "Position X", "Position Y", "Speed", "Status", "Connected"]
        self.table = self.console_utils.create_table("Taxi Positions", columns)

        self.stop_event = Event()
        self.heartbeat_lock = Lock()
        self.heartbeat_timestamps = {}

        # Initialize activation socket as PULL to receive signals from HeartbeatService
        self.activation_socket = self.zmq_utils.context.socket(zmq.PULL)
        self.activation_socket.bind(f"tcp://*:{BACKUP_ACTIVATION_PORT}")

        self.user_req_socket = self.zmq_utils.bind_rep_user_request_socket(BACKUP_USER_REQ_PORT)

        self.assignment_lock = Lock()

        self.db_handler = DatabaseHandler(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)

        self.main_dispatcher_offline = False
        self.heartbeat_2_port = HEARTBEAT_2_PORT
        self.backup_activation_port = BACKUP_ACTIVATION_PORT

    def handle_taxi_requests(self):
        responder = self.zmq_utils.bind_rep_socket()
        try:
            while not self.stop_event.is_set():
                try:
                    if responder.poll(100):
                        message = responder.recv_string()
                        if message.startswith("connect_request"):
                            parts = message.split()
                            if len(parts) < 6:
                                self.console_utils.print(f"Invalid connect_request message: {message}", 3)
                                responder.send_string("invalid_request")
                                continue
                            _, taxi_id, pos_x, pos_y, speed, status = parts

                            try:
                                taxi_id = int(taxi_id)
                                pos_x = int(pos_x)
                                pos_y = int(pos_y)
                                speed = int(speed)
                            except ValueError:
                                self.console_utils.print(f"Invalid data types in connect_request message: {message}", 3)
                                responder.send_string("invalid_request")
                                continue

                            # if taxi_id not in self.system.taxis:
                            if not self.db_handler.taxi_exists(taxi_id):
                                # Register new taxi in-memory
                                taxi = Taxi(taxi_id, self.system.grid.rows, self.system.grid.cols, pos_x, pos_y, speed, status, True)
                                taxi.initial_pos_x = pos_x
                                taxi.initial_pos_y = pos_y
                                # self.system.register_taxi(taxi)
                                
                                # Add taxi to the database
                                self.db_handler.add_taxi(
                                    taxi_id=taxi_id,
                                    pos_x=pos_x,
                                    pos_y=pos_y,
                                    speed=speed,
                                    status=status
                                )
                                
                                responder.send_string(f"connect_ack {taxi_id}")
                                self.console_utils.print(f"Taxi {taxi_id} connected at ({pos_x}, {pos_y}) with speed {speed}.")
                            else:
                                # Update existing taxi in-memory
                                # taxi = self.system.taxis[taxi_id]
                                # taxi.pos_x = pos_x
                                # taxi.pos_y = pos_y
                                # taxi.speed = speed
                                # taxi.status = status
                                # taxi.connected = True  # Mark as connected upon reconnection
                                
                                # Update taxi in the database
                                self.db_handler.update_taxi_position(taxi_id, pos_x, pos_y)
                                self.db_handler.set_taxi_status(taxi_id, status)
                                self.db_handler.update_taxi_connected_status(taxi_id, connected=True)
                                
                                responder.send_string(f"connect_ack {taxi_id}")
                                # self.console_utils.print(f"Taxi {taxi_id} reconnected and updated.")

                            # Update Heartbeat Timestamp
                            with self.heartbeat_lock:
                                self.heartbeat_timestamps[taxi_id] = time.time()
                                self.db_handler.record_heartbeat(taxi_id)

                            self.refresh_table()

                except zmq.Again:
                    pass
                except zmq.ZMQError as e:
                    if self.stop_event.is_set():
                        break
                    if not self.zmq_utils.context.closed:
                        self.console_utils.print(f"Error while handling taxi request: {e}", 3)
        except zmq.ZMQError as e:
            if not self.zmq_utils.context.closed:
                self.console_utils.print(f"Error in handle_taxi_requests: {e}", 3)
        finally:
            if responder:
                responder.close()


    def handle_user_requests(self):
        responder = self.user_req_socket
        try:
            while not self.stop_event.is_set():
                try:
                    if responder.poll(100):
                        message = responder.recv_string()
                        if message.startswith("user_request"):
                            parts = message.split()
                            if len(parts) != 4:
                                self.console_utils.print(f"Invalid user_request message: {message}", 3)
                                responder.send_string("invalid_request")
                                continue
                            _, user_id, user_x, user_y = parts
                            try:
                                user_id = int(user_id)
                                user_x = int(user_x)
                                user_y = int(user_y)
                            except ValueError:
                                self.console_utils.print(f"Invalid data types in user_request message: {message}", 3)
                                responder.send_string("invalid_request")
                                continue

                            self.console_utils.print(f"Received ride request from User {user_id} at ({user_x}, {user_y})", 2)

                            # Log user request in the database
                            self.db_handler.add_user_request(user_id, user_x, user_y, waiting_time=30)  # Example waiting_time

                            # Find the nearest available taxi
                            assigned_taxi = self.find_nearest_available_taxi(user_x, user_y)

                            if assigned_taxi:
                                with self.assignment_lock:
                                    # Double-check if the taxi is still available
                                    if assigned_taxi.connected and assigned_taxi.status.lower() == "available":
                                        # Assign the taxi in the database
                                        self.db_handler.assign_taxi_to_user(user_id, assigned_taxi.taxi_id)

                                        # Update in-memory taxi status
                                        assigned_taxi.connected = False  # Mark as occupied
                                        assigned_taxi.status = "unavailable"

                                        self.console_utils.print(f"Assigned Taxi {assigned_taxi.taxi_id} to User {user_id}", 2)
                                        responder.send_string(f"assign_taxi {assigned_taxi.taxi_id}")

                                        # Optionally, notify the assigned taxi about the assignment
                                        self.zmq_utils.publish_assignment(f"assign {assigned_taxi.taxi_id} {user_id}")

                                        # Simulate the taxi being busy with the service
                                        service_thread = Thread(target=self.simulate_service, args=(assigned_taxi.taxi_id, user_id, 5), daemon=True)
                                        service_thread.start()
                                    else:
                                        self.console_utils.print(f"Taxi {assigned_taxi.taxi_id} became unavailable during assignment.", 3)
                                        responder.send_string("no_taxi_available")
                            else:
                                self.console_utils.print(f"No available taxis for User {user_id}", 3)
                                responder.send_string("no_taxi_available")

                            self.refresh_table()
                except zmq.Again:
                    pass
                except zmq.ZMQError as e:
                    if self.stop_event.is_set():
                        break
                    if not self.zmq_utils.context.closed:
                        self.console_utils.print(f"Error while handling user request: {e}", 3)
                except Exception as e:
                    self.console_utils.print(f"Unexpected error in handle_user_requests: {e}", 3)
        finally:
            if responder:
                responder.close()

    def find_nearest_available_taxi(self, user_x, user_y):
        with self.assignment_lock:
            # Fetch available taxis from the database
            available_taxis = self.db_handler.get_available_taxis()
            if not available_taxis:
                return None
            # Calculate Manhattan distance and sort
            available_taxis.sort(key=lambda taxi: (abs(taxi.pos_x - user_x) + abs(taxi.pos_y - user_y), taxi.taxi_id))
            nearest_taxi = available_taxis[0]  # Get the closest taxi
            return nearest_taxi


    def simulate_service(self, taxi_id, user_id, duration):
        self.console_utils.print(f"Taxi {taxi_id} is servicing User {user_id} for {duration} seconds.", 2)
        time.sleep(duration)
        # After service completion, mark the taxi as available and reset position
        # if taxi_id in self.system.taxis:
        if self.db_handler.taxi_exists(taxi_id):
            # taxi = self.system.taxis[taxi_id]
            taxi = self.db_handler.get_taxi_by_id(taxi_id)
            taxi.status = "available"
            taxi.connected = True
            taxi.pos_x = taxi.initial_pos_x
            taxi.pos_y = taxi.initial_pos_y
            with self.heartbeat_lock:
                self.heartbeat_timestamps[taxi_id] = time.time()
            self.refresh_table()
            self.console_utils.print(f"Taxi {taxi_id} has completed service for User {user_id} and is now available at ({taxi.pos_x}, {taxi.pos_y}).", 2)
            
            # Update the database to mark the taxi as available and reset position
            self.db_handler.mark_taxi_available(taxi_id)
            self.db_handler.update_taxi_position(taxi_id, taxi.pos_x, taxi.pos_y)
        else:
            self.console_utils.print(f"Taxi {taxi_id} not found during service simulation.", 3)


    def receive_position_updates(self):
        puller = self.zmq_utils.bind_pull_socket()
        try:
            while not self.stop_event.is_set():
                try:
                    message = puller.recv_string(zmq.NOBLOCK)
                    if message:
                        parts = message.split()
                        if len(parts) < 5:
                            self.console_utils.print(f"Invalid position update message: {message}", 3)
                            continue
                        taxi_id, pos_x, pos_y, _, _ = parts
                        try:
                            taxi_id = int(taxi_id)
                            pos_x = int(pos_x)
                            pos_y = int(pos_y)
                        except ValueError:
                            self.console_utils.print(f"Invalid data types in position update message: {message}", 3)
                            continue

                        # if taxi_id in self.system.taxis:
                        if self.db_handler.taxi_exists(taxi_id):
                            # Update in-memory position
                            # self.system.update_taxi_position(taxi_id, pos_x, pos_y)
                            
                            # Update position in the database
                            self.db_handler.update_taxi_position(taxi_id, pos_x, pos_y)
                            
                            # Optionally, record a heartbeat for the taxi
                            self.db_handler.record_heartbeat(taxi_id)
                        else:
                            self.console_utils.print(f"Taxi {taxi_id} not found, cannot update position", 3)

                        self.refresh_table()
                except zmq.Again:
                    pass
                except zmq.ZMQError as e:
                    if not self.zmq_utils.context.closed:
                        self.console_utils.print(f"Error while receiving position updates: {e}", 3)
                except Exception as e:
                    self.console_utils.print(f"Unexpected error in receive_position_updates: {e}", 3)
        finally:
            if puller:
                puller.close()

    def refresh_table(self):
        taxi_data = []
        taxis = self.db_handler.get_all_taxis()
        # for taxi_id, taxi in self.system.taxis.items():
        for taxi in taxis:
            taxi_id = taxi[0]
            pos_x = taxi[1]
            pos_y = taxi[2]
            speed = taxi[3]
            status = taxi[4]
            connected = taxi[5]

            if isinstance(status, str):
                status_lower = status.lower()
                if status_lower == "available":
                    status_str = "[light_green]Available[/light_green]"
                elif status_lower == "unavailable":
                    status_str = "[red]Unavailable[/red]"
                else:
                    status_str = f"[yellow]{status}[/yellow]"
            else:
                status_str = f"[yellow]{status}[/yellow]"

            if isinstance(connected, bool):
                if connected == 1:
                    connected_str = "[light_green]True[/light_green]"
                else:
                    connected_str = "[red]False[/red]"
            else:
                connected_str = f"[yellow]{connected}[/yellow]"

            # Optional: Commented out to reduce console clutter
            # self.console_utils.print(f"Styled Status: {status_str}, Styled Connected: {connected_str}", show_level=False)

            taxi_data.append([
                str(taxi_id),
                str(pos_x),
                str(pos_y),
                str(speed),
                status_str,
                connected_str,
            ])

        table = self.console_utils.generate_table(
            "Taxi Positions",
            ["Taxi ID", "Position X", "Position Y", "Speed", "Status", "Connected"],
            taxi_data
        )

        self.live.update(table)
    
    def receive_heartbeat(self):
        try:
            heartbeat_puller = self.zmq_utils.bind_pull_heartbeat_socket()
            while not self.stop_event.is_set():
                try:
                    message = heartbeat_puller.recv_string(zmq.NOBLOCK)
                    if message:
                        parts = message.split()
                        if len(parts) != 2 or parts[0] != "heartbeat":
                            self.console_utils.print(f"Invalid heartbeat message: {message}", 3)
                            continue
                        _, taxi_id = parts
                        try:
                            taxi_id = int(taxi_id)
                        except ValueError:
                            self.console_utils.print(f"Invalid taxi_id in heartbeat message: {message}", 3)
                            continue
                            
                        with self.heartbeat_lock:
                            self.heartbeat_timestamps[taxi_id] = time.time()
                            if self.db_handler.taxi_exists(taxi_id):
                                self.db_handler.update_taxi_connected_status(taxi_id, True)
                                # self.console_utils.print(f"Received heartbeat from Taxi {taxi_id}", show_level=False)
                            else:
                                self.console_utils.print(f"Heartbeat from unknown Taxi {taxi_id}", 3)
                        self.refresh_table()
                except zmq.Again:
                    pass
                except zmq.ZMQError as e:
                    if not self.zmq_utils.context.closed:
                        self.console_utils.print(f"Error while receiving heartbeat: {e}", 3)
                except Exception as e:
                    self.console_utils.print(f"Unexpected error in receive_heartbeat: {e}", 3)
        finally:
            if heartbeat_puller:
                heartbeat_puller.close()
    
    def receive_heartbeat_from_heartbeat_server(self):
        try:
            activation_puller = self.zmq_utils.context.socket(zmq.PULL)
            activation_puller.bind(f"tcp://*:{self.heartbeat_2_port}")

            self.console_utils.print(f"Listening for activation signals on port {self.heartbeat_2_port}.", 2)

            poller = zmq.Poller()
            poller.register(activation_puller, zmq.POLLIN)  # Poll for incoming messages

            while not self.stop_event.is_set():
                socks = dict(poller.poll(1000))  # Wait for 1 second
                if activation_puller in socks:
                    message = activation_puller.recv_string()
                    self.console_utils.print(f"Received signal: {message}", 2)

                    if message == "activate_backup":
                        if not self.main_dispatcher_offline:
                            self.main_dispatcher_offline = True
                            self.console_utils.print("Received activate signal. Taking over tasks as Backup Dispatcher.", 2)
                            self.start_dispatcher_tasks()

                    elif message == "deactivate_backup":
                        if self.main_dispatcher_offline:
                            self.main_dispatcher_offline = False
                            self.console_utils.print("Received deactivate signal. Pausing Backup Dispatcher tasks.", 2)
                            self.stop_dispatcher_tasks()

                else:
                    time.sleep(0.1)  # Keep the loop non-blocking

        except zmq.ZMQError as e:
            self.console_utils.print(f"Error receiving heartbeat signal: {e}", 3)

        finally:
            activation_puller.close()


    def monitor_heartbeats(self):
        HEARTBEAT_INTERVAL = 5
        TIMEOUT = 15

        while not self.stop_event.is_set():
            current_time = time.time()
            with self.heartbeat_lock:
                for taxi_id, last_hb in list(self.heartbeat_timestamps.items()):
                    if current_time - last_hb > TIMEOUT:
                        # if taxi_id in self.system.taxis:
                        if self.db_handler.taxi_exists(taxi_id):
                            # self.system.taxis[taxi_id].connected = False
                            self.db_handler.update_taxi_connected_status(taxi_id, connected=False)
                            # self.console_utils.print(f"Taxi {taxi_id} disconnected due to missed heartbeats.", 3)
                            self.refresh_table()
                        del self.heartbeat_timestamps[taxi_id]
            time.sleep(HEARTBEAT_INTERVAL)
    
    def activate(self):
        self.console_utils.print("Backup dispatcher active... Waiting for heartbeat signal from heartbeat server.")
        while True:
            try:
                if self.activation_socket.poll(1000):
                    message = self.activation_socket.recv_string()
                    if message == "activate_backup":
                        self.console_utils.print("Backup Dispatcher activated. Taking over tasks.", 2)
                        # Initialize backup dispatcher tasks here
                        self.main_dispatcher_offline = True
                        return
            except zmq.ZMQError as e:
                self.console_utils.print(f"Backup activation error: {e}", 3)

    def run(self):
        if not validate_grid(self.system.grid.rows, self.system.grid.cols, self.console_utils):
            self.console_utils.print(f"Dispatcher failed to start due to invalid parameters.", 3)
            return
        try:
            activate_thread = Thread(target=self.activate, name="ActivationHandler")
            activate_thread.daemon = False
            activate_thread.start()
        finally:
            activate_thread.join()

        while self.main_dispatcher_offline:
            try:
                with self.console_utils.start_live_display(self.table) as live:
                    self.live = live

                    taxi_thread = Thread(target=self.handle_taxi_requests, name="ConnectionHandler")
                    updates_thread = Thread(target=self.receive_position_updates, name="PositionUpdater")
                    heartbeat_thread = Thread(target=self.receive_heartbeat, name="HeartbeatReceiver")
                    monitor_thread = Thread(target=self.monitor_heartbeats, name="HeartbeatMonitor")
                    user_thread = Thread(target=self.handle_user_requests, name="UserRequestHandler")
                    receive_heartbeat_from_heartbeat_server_thread = Thread(target=self.receive_heartbeat_from_heartbeat_server, name="HeartbeatServerReceiver")

                    taxi_thread.daemon = False
                    updates_thread.daemon = False
                    heartbeat_thread.daemon = False
                    monitor_thread.daemon = False
                    user_thread.daemon = False
                    receive_heartbeat_from_heartbeat_server_thread.daemon = False

                    taxi_thread.start()
                    updates_thread.start()
                    heartbeat_thread.start()
                    monitor_thread.start()
                    user_thread.start()
                    receive_heartbeat_from_heartbeat_server_thread.start()

                    while not self.stop_event.is_set():
                        taxi_thread.join(timeout=1)
                        updates_thread.join(timeout=1)
                        heartbeat_thread.join(timeout=1)
                        monitor_thread.join(timeout=1)
                        user_thread.join(timeout=1)
                        activate_thread.join(timeout=1)
                        receive_heartbeat_from_heartbeat_server_thread.join(timeout=1)

            except KeyboardInterrupt:
                self.console_utils.print("Backup Dispatcher process interrupted by user.", 2)
                self.stop_event.set()

            finally:
                self.console_utils.print("Cleaning up backup dispatcher resources...", 2)
                taxi_thread.join()
                updates_thread.join()
                heartbeat_thread.join()
                monitor_thread.join()
                user_thread.join()
                activate_thread.join()
                receive_heartbeat_from_heartbeat_server_thread.join()
                self.zmq_utils.close()
                self.db_handler.close()
                self.console_utils.print("Backup Dispatcher process ended and resources cleaned up.", 4)
