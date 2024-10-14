import sys
from src.services.taxi_service import TaxiService

def main():
    if len(sys.argv) != 7:
        print("Usage: python taxi.py <taxi_id> <N> <M> <pos_x> <pos_y> <speed>")
        sys.exit(1)

    taxi_id = int(sys.argv[1])
    N = int(sys.argv[2])
    M = int(sys.argv[3])
    pos_x = int(sys.argv[4])
    pos_y = int(sys.argv[5])
    speed = int(sys.argv[6])

    taxi_service = TaxiService(taxi_id, pos_x, pos_y, speed, N, M, "Available")
    taxi_service.run()

if __name__ == "__main__":
    main()