from src.config import MAX_N, MAX_M, VALID_SPEEDS

def validate_grid(rows, cols, console_utils):
    if rows > MAX_N or cols > MAX_M and (rows >= 0 and cols >= 0):
        console_utils.print(f"Error: Grid dimensions must be between 0x0 and {MAX_N}x{MAX_M}.", 3)
        return False
    return True

def validate_initial_position(pos_x, pos_y, N, M, taxi_id, console_utils):
    if not (0 <= pos_x < N) or not (0 <= pos_y < M):
        console_utils.print(f"Invalid initial position for Taxi {taxi_id}: ({pos_x}, {pos_y})", 3)
        return False
    return True

def validate_speed(speed, taxi_id, console_utils):
    if speed not in VALID_SPEEDS:
        console_utils.print(f"Invalid speed for Taxi {taxi_id}: {speed} km/h. Must be 1, 2, or 4 km/h", 3)
        return False
    return True