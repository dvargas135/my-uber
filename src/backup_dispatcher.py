import sys
from src.services.backup_dispatcher_service import BackupDispatcherService
from src.config import MAX_N, MAX_M

def main():
    if len(sys.argv) != 3:
        print("Usage: python backup_dispatcher.py <N> <M>")
        sys.exit(1)

    N = int(sys.argv[1])
    M = int(sys.argv[2])

    backup_dispatcher = BackupDispatcherService(N, M)
    backup_dispatcher.run()

if __name__ == "__main__":
    main()
