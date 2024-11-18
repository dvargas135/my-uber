import zmq
import threading
import time
from threading import Thread, Event, Lock
from src.models.system_model import System
from src.models.taxi_model import Taxi
from src.config import BACKUP_PUB_PORT, BACKUP_SUB_PORT, BACKUP_REP_PORT, BACKUP_DISPATCHER_IP, BACKUP_PULL_PORT, BACKUP_HEARTBEAT_PORT, BACKUP_USER_REQ_PORT
from src.utils.rich_utils import RichConsoleUtils
from src.utils.validation_utils import validate_grid
from src.utils.zmq_utils import ZMQUtils
from src.services.database_service import DatabaseService
from src.config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

class BackupDispatcherService:
    def __init__(self, N, M):
        self.console_utils = RichConsoleUtils()
        self.system = System(N, M)
        self.zmq_utils = ZMQUtils(BACKUP_DISPATCHER_IP, BACKUP_PUB_PORT, BACKUP_SUB_PORT, BACKUP_REP_PORT, BACKUP_PULL_PORT, BACKUP_HEARTBEAT_PORT)

        columns = ["Taxi ID", "Position X", "Position Y", "Speed", "Status", "Connected"]
        self.table = self.console_utils.create_table("Taxi Positions", columns)

        self.stop_event = Event()
        self.heartbeat_lock = Lock()
        self.heartbeat_timestamps = {}

        self.user_req_socket = self.zmq_utils.bind_rep_user_request_socket(USER_REQ_PORT)

        self.assignment_lock = Lock()

        db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        self.db_service = DatabaseService(db_url)

        self.initialize_dispatcher_state()
        self.main_dispatcher_alive = True

        self.taxi_thread = None
        self.user_thread = None
        self.position_thread = None
    
        def monitor_main_dispatcher(self):
            HEARTBEAT_THRESHOLD = 30
            MONITOR_INTERVAL = 10
            while not self.stop_event.is_set():
                try:
                    # Example: Check a specific heartbeat table or a ping endpoint
                    last_heartbeat = self.db_service.get_last_heartbeat(main_dispatcher_id)
                    current_time = time.time()
                    if current_time - last_heartbeat > HEARTBEAT_THRESHOLD:
                        if self.main_dispatcher_alive:
                            self.console_utils.print("Main dispatcher is down. Switching to backup mode.", 2)
                            self.main_dispatcher_alive = False
                            self.activate_backup_dispatcher()
                    else:
                        if not self.main_dispatcher_alive:
                            self.console_utils.print("Main dispatcher is back online. Deactivating backup mode.", 2)
                            self.main_dispatcher_alive = True
                            self.deactivate_backup_dispatcher()
                except Exception as e:
                    self.console_utils.print(f"Error in monitoring main dispatcher: {e}", 3)
                time.sleep(MONITOR_INTERVAL)

    def run_backup_dispatcher():
        backup_dispatcher = BackupDispatcherService(...)
        
        # Activate the backup dispatcher
        backup_dispatcher.activate_backup_dispatcher()
        
        try:
            while True:
                # Implement your failover detection logic here
                # For simplicity, we'll keep the backup dispatcher active indefinitely
                time.sleep(1)
        except KeyboardInterrupt:
            # Deactivate the backup dispatcher on interruption
            backup_dispatcher.deactivate_backup_dispatcher()

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

                            if taxi_id not in self.system.taxis:
                                # Register new taxi in-memory
                                taxi = Taxi(taxi_id, self.system.grid.rows, self.system.grid.cols, pos_x, pos_y, speed, status, True)
                                taxi.initial_pos_x = pos_x
                                taxi.initial_pos_y = pos_y
                                self.system.register_taxi(taxi)
                                
                                # Add taxi to the database
                                self.db_service.add_taxi(
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
                                taxi = self.system.taxis[taxi_id]
                                taxi.pos_x = pos_x
                                taxi.pos_y = pos_y
                                taxi.speed = speed
                                taxi.status = status
                                taxi.connected = True  # Mark as connected upon reconnection
                                
                                # Update taxi in the database
                                self.db_service.update_taxi_position(taxi_id, pos_x, pos_y)
                                self.db_service.set_taxi_status(taxi_id, status)
                                self.db_service.update_taxi_connected_status(taxi_id, connected=True)
                                
                                responder.send_string(f"connect_ack {taxi_id}")
                                self.console_utils.print(f"Taxi {taxi_id} reconnected and updated.")

                            # Update Heartbeat Timestamp
                            with self.heartbeat_lock:
                                self.heartbeat_timestamps[taxi_id] = time.time()

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
                            self.db_service.add_user_request(user_id, user_x, user_y, waiting_time=30)  # Example waiting_time

                            # Find the nearest available taxi
                            assigned_taxi = self.find_nearest_available_taxi(user_x, user_y)

                            if assigned_taxi:
                                with self.assignment_lock:
                                    # Double-check if the taxi is still available
                                    if assigned_taxi.connected and assigned_taxi.status.lower() == "available":
                                        # Assign the taxi in the database
                                        self.db_service.assign_taxi_to_user(user_id, assigned_taxi.taxi_id)

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
            available_taxis = self.db_service.get_available_taxis()
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
        if taxi_id in self.system.taxis:
            taxi = self.system.taxis[taxi_id]
            taxi.status = "available"
            taxi.connected = True
            taxi.pos_x = taxi.initial_pos_x
            taxi.pos_y = taxi.initial_pos_y
            with self.heartbeat_lock:
                self.heartbeat_timestamps[taxi_id] = time.time()
            self.refresh_table()
            self.console_utils.print(f"Taxi {taxi_id} has completed service for User {user_id} and is now available at ({taxi.pos_x}, {taxi.pos_y}).", 2)
            
            # Update the database to mark the taxi as available and reset position
            self.db_service.mark_taxi_available(taxi_id)
            self.db_service.update_taxi_position(taxi_id, taxi.pos_x, taxi.pos_y)
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

                        if taxi_id in self.system.taxis:
                            # Update in-memory position
                            self.system.update_taxi_position(taxi_id, pos_x, pos_y)
                            
                            # Update position in the database
                            self.db_service.update_taxi_position(taxi_id, pos_x, pos_y)
                            
                            # Optionally, record a heartbeat for the taxi
                            self.db_service.record_heartbeat(taxi_id)
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
        for taxi_id, taxi in self.system.taxis.items():

            if isinstance(taxi.status, str):
                status_lower = taxi.status.lower()
                if status_lower == "available":
                    status_str = "[light_green]Available[/light_green]"
                elif status_lower == "unavailable":
                    status_str = "[red]Unavailable[/red]"
                else:
                    status_str = f"[yellow]{taxi.status}[/yellow]"
            else:
                status_str = f"[yellow]{taxi.status}[/yellow]"

            if isinstance(taxi.connected, bool):
                if taxi.connected:
                    connected_str = "[light_green]True[/light_green]"
                else:
                    connected_str = "[red]False[/red]"
            else:
                connected_str = f"[yellow]{taxi.connected}[/yellow]"

            # Optional: Commented out to reduce console clutter
            # self.console_utils.print(f"Styled Status: {status_str}, Styled Connected: {connected_str}", show_level=False)

            taxi_data.append([
                str(taxi_id),
                str(taxi.pos_x),
                str(taxi.pos_y),
                str(taxi.speed),
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
                            if taxi_id in self.system.taxis:
                                self.system.taxis[taxi_id].connected = True
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

    def monitor_heartbeats(self):
        HEARTBEAT_INTERVAL = 5
        TIMEOUT = 15

        while not self.stop_event.is_set():
            current_time = time.time()
            with self.heartbeat_lock:
                for taxi_id, last_hb in list(self.heartbeat_timestamps.items()):
                    if current_time - last_hb > TIMEOUT:
                        if taxi_id in self.system.taxis:
                            self.system.taxis[taxi_id].connected = False
                            # self.console_utils.print(f"Taxi {taxi_id} disconnected due to missed heartbeats.", 3)
                            self.refresh_table()
                        del self.heartbeat_timestamps[taxi_id]
            time.sleep(HEARTBEAT_INTERVAL)
    
    def initialize_dispatcher_state(self):
        # Fetch all taxis from the database and populate the in-memory system
        session = self.db_service.get_session()
        try:
            taxis = session.query(Taxi).all()
            for taxi in taxis:
                self.system.register_taxi(taxi)
            self.console_utils.print("Dispatcher state initialized from the database.", 2)
        except Exception as e:
            self.console_utils.print(f"Error initializing dispatcher state: {e}", 3)
        finally:
            session.close()

    def activate_backup_dispatcher(self):
        # Clear the stop event in case it was previously set
        self.stop_event.clear()
        
        # Initialize and start threads
        self.taxi_thread = Thread(target=self.handle_taxi_requests, name="ConnectionHandler")
        self.user_thread = Thread(target=self.handle_user_requests, name="UserRequestHandler")
        self.position_thread = Thread(target=self.receive_position_updates, name="PositionUpdater")
        self.heartbeat_thread = Thread(target=self.receive_heartbeat, name="HeartbeatReceiver")
        self.monitor_thread = Thread(target=self.monitor_heartbeats, name="HeartbeatMonitor")
        self.health_monitor_thread = Thread(target=self.monitor_main_dispatcher, name="HealthMonitorHandler")
        
        # Set daemon to False to ensure threads are managed properly
        self.taxi_thread.daemon = False
        self.user_thread.daemon = False
        self.position_thread.daemon = False
        self.heartbeat_thread.daemon = False
        self.monitor_thread.daemon = False
        self.health_monitor_thread.daemon = False
        
        # Start all threads
        self.taxi_thread.start()
        self.user_thread.start()
        self.position_thread.start()
        self.heartbeat_thread.start()
        self.monitor_thread.start()
        self.health_monitor_thread.start()
        
        # Optional: Log activation
        self.console_utils.print("Backup dispatcher activated and is now handling requests.", 2)
    
    def deactivate_backup_dispatcher(self):
        # Signal threads to stop
        self.stop_event.set()
        
        # Wait for threads to finish execution
        if self.taxi_thread is not None:
            self.taxi_thread.join()
        if self.user_thread is not None:
            self.user_thread.join()
        if self.position_thread is not None:
            self.position_thread.join()
        if self.heartbeat_thread is not None:
            self.heartbeat_thread.join()
        if self.monitor_thread is not None:
            self.monitor_thread.join()
        if self.health_monitor_thread is not None:
            self.health_monitor_thread.join()
        
        # Optional: Log deactivation
        self.console_utils.print("Backup dispatcher deactivated and has stopped handling requests.", 2)
    
    def run(self):
        if not validate_grid(self.system.grid.rows, self.system.grid.cols, self.console_utils):
            self.console_utils.print(f"Dispatcher failed to start due to invalid parameters.", 3)
            return
        
        try:
            with self.console_utils.start_live_display(self.table) as live:
                self.live = live

                # Activate the backup dispatcher (starts all threads)
                self.activate_backup_dispatcher()
    
                while not self.stop_event.is_set():
                    time.sleep(1)  # Keep the main thread alive
    
        except KeyboardInterrupt:
            self.console_utils.print("Backup Dispatcher process interrupted by user.", 2)
            self.stop_event.set()
    
        finally:
            # Deactivate the backup dispatcher (signals threads to stop and joins them)
            self.deactivate_backup_dispatcher()
            self.console_utils.print("Cleaning up dispatcher resources...", 2)
            self.zmq_utils.close()
            self.console_utils.print("Backup Dispatcher process ended and resources cleaned up.", 4)