import zmq
import threading
import time

class User(threading.Thread):
    def __init__(self, user_id, pos_x, pos_y, wait_time, dispatcher_address, timeout=5):
        """
        Initialize the User thread.

        :param user_id: Unique identifier for the user.
        :param pos_x: X-coordinate of the user's position.
        :param pos_y: Y-coordinate of the user's position.
        :param wait_time: Time in seconds before requesting a taxi.
        :param dispatcher_address: Address of the dispatcher (e.g., "tcp://localhost:5555").
        :param timeout: Time in seconds to wait for a dispatcher response.
        """
        super().__init__()
        self.user_id = user_id
        self.pos_x = pos_x
        self.pos_y = pos_y
        self.wait_time = wait_time
        self.dispatcher_address = dispatcher_address
        self.timeout = timeout
        self.success = False
        self.response_time = None

    def run(self):
        try:
            # Sleep for the specified waiting time
            time.sleep(self.wait_time)

            # Initialize ZeroMQ context and REQ socket
            context = zmq.Context()
            socket = context.socket(zmq.REQ)
            socket.connect(self.dispatcher_address)

            # Prepare the request message
            request = f"request_taxi {self.user_id} {self.pos_x} {self.pos_y}"

            # Record the start time
            start_time = time.time()

            # Send the request
            socket.send_string(request)

            # Poll the socket for a response with timeout
            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)
            socks = dict(poller.poll(self.timeout * 1000))  # Timeout in milliseconds

            if socks.get(socket) == zmq.POLLIN:
                # Receive the response
                response = socket.recv_string()
                end_time = time.time()
                self.response_time = end_time - start_time

                if response.startswith("assign_taxi"):
                    _, taxi_id = response.split()
                    self.success = True
                    print(f"User {self.user_id} assigned to Taxi {taxi_id} in {self.response_time:.2f} seconds.")
                else:
                    self.success = False
                    print(f"User {self.user_id} received invalid response: {response}")
            else:
                # Timeout occurred
                self.success = False
                self.response_time = self.timeout
                print(f"User {self.user_id} timed out after {self.timeout} seconds waiting for a taxi.")

            # Clean up
            socket.close()
            context.term()

        except Exception as e:
            self.success = False
            self.response_time = self.timeout
            print(f"User {self.user_id} encountered an error: {e}")
