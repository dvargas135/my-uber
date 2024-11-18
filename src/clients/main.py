from src.clients.user_generator import UserGenerator

if __name__ == "__main__":
    USER_FILE = "users.txt"  # Path to your user specification file
    DISPATCHER_ADDRESS = "tcp://localhost:5555"  # Replace with your dispatcher's address
    TIMEOUT = 5  # Seconds to wait for dispatcher response

    generator = UserGenerator(USER_FILE, DISPATCHER_ADDRESS, TIMEOUT)
    generator.run()