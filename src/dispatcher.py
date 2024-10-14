import sys
from src.services.dispatcher_service import DispatcherService
from src.config import MAX_N, MAX_M

def main():
    if len(sys.argv) != 3:
        print("Usage: python dispatcher.py <N> <M>")
        sys.exit(1)

    N = int(sys.argv[1])
    M = int(sys.argv[2])

    dispatcher = DispatcherService(N, M)
    dispatcher.run()

if __name__ == "__main__":
    main()
