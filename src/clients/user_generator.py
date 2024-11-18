import threading
from src.clients.user import User

class UserGenerator:
    def __init__(self, user_file, dispatcher_address, timeout=5):
        """
        Initialize the UserGenerator.

        :param user_file: Path to the text file containing user specifications.
        :param dispatcher_address: Address of the dispatcher (e.g., "tcp://localhost:5555").
        :param timeout: Time in seconds to wait for a dispatcher response.
        """
        self.user_file = user_file
        self.dispatcher_address = dispatcher_address
        self.timeout = timeout
        self.users = []

    def read_user_file(self):
        """
        Read the user specifications from the text file.

        Expected file format (one user per line):
        user_id pos_x pos_y wait_time
        Example:
        1 2 3 10
        2 5 5 20
        """
        with open(self.user_file, 'r') as file:
            for line in file:
                parts = line.strip().split()
                if len(parts) != 4:
                    print(f"Invalid user specification: {line}")
                    continue
                user_id, pos_x, pos_y, wait_time = parts
                try:
                    user_id = int(user_id)
                    pos_x = int(pos_x)
                    pos_y = int(pos_y)
                    wait_time = int(wait_time)
                    user = User(user_id, pos_x, pos_y, wait_time, self.dispatcher_address, self.timeout)
                    self.users.append(user)
                except ValueError:
                    print(f"Invalid data types in line: {line}")
                    continue

    def start_users(self):
        """
        Start all user threads.
        """
        for user in self.users:
            user.start()

    def wait_for_completion(self):
        """
        Wait for all user threads to complete.
        """
        for user in self.users:
            user.join()

    def run(self):
        """
        Execute the UserGenerator process.
        """
        self.read_user_file()
        self.start_users()
        self.wait_for_completion()
        self.generate_report()

    def generate_report(self):
        """
        Generate a summary report of user requests.
        """
        total_users = len(self.users)
        successful_requests = sum(user.success for user in self.users)
        failed_requests = total_users - successful_requests
        average_response_time = sum(user.response_time for user in self.users if user.success) / successful_requests if successful_requests > 0 else 0

        print("\n--- User Request Report ---")
        print(f"Total Users: {total_users}")
        print(f"Successful Requests: {successful_requests}")
        print(f"Failed Requests (No Taxi/Timeout): {failed_requests}")
        print(f"Average Response Time: {average_response_time:.2f} seconds")
