import sys
from src.services.user_service import UserService
from src.config import BACKUP_DISPATCHER_IP, SUB_PORT, REP_PORT, DISPATCHER_IP, PULL_PORT, HEARTBEAT_PORT, USER_REQ_PORT, BACKUP_USER_REQ_PORT, DB_PASSWORD, DB_HOST, DB_NAME, HEARTBEAT_2_PORT, HEARTBEAT_3_PORT

def main():
    if len(sys.argv) != 2:
        print("Usage: python run_user_service.py <users_file>")
        sys.exit(1)
    
    users_file = sys.argv[1]
    dispatcher_ip = DISPATCHER_IP
    backup_dispatcher_ip = BACKUP_DISPATCHER_IP
    user_req_port = USER_REQ_PORT
    backup_user_req_port = BACKUP_USER_REQ_PORT
    
    user_service = UserService(users_file, dispatcher_ip, backup_dispatcher_ip, user_req_port, backup_user_req_port)
    user_service.run()

if __name__ == "__main__":
    main()
