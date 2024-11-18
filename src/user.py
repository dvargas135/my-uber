import sys
from src.services.user_service import UserService

def main():
    if len(sys.argv) != 4:
        print("Usage: python run_user_service.py <users_file> <dispatcher_ip> <user_req_port>")
        sys.exit(1)
    
    users_file = sys.argv[1]
    dispatcher_ip = sys.argv[2]
    user_req_port = int(sys.argv[3])
    
    user_service = UserService(users_file, dispatcher_ip, user_req_port)
    user_service.run()

if __name__ == "__main__":
    main()
