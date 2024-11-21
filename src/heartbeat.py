import sys
from src.services.heartbeat_service import HeartbeatService
from src.config import DISPATCHER_IP, BACKUP_DISPATCHER_IP, HEARTBEAT_PORT, BACKUP_ACTIVATION_PORT

def main():
    heartbeat_service = HeartbeatService(DISPATCHER_IP, BACKUP_DISPATCHER_IP, HEARTBEAT_PORT, BACKUP_ACTIVATION_PORT)
    heartbeat_service.run()

if __name__ == "__main__":
    main()
