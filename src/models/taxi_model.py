from src.models.grid_model import Grid

class Taxi:
    def __init__(self, taxi_id, N, M, pos_x, pos_y, speed, status, connected=False):
        self.taxi_id = taxi_id
        self.initial_pos_x = None
        self.initial_pos_y = None
        self.pos_x = pos_x
        self.pos_y = pos_y
        self.speed = speed
        self.grid = Grid(N, M)
        self.status = status
        self.connected = connected

    def move(self, direction, cells_to_move):
        if direction == "NORTH":
            self.pos_y -= cells_to_move
        elif direction == "SOUTH":
            self.pos_y += cells_to_move
        elif direction == "EAST":
            self.pos_x += cells_to_move
        elif direction == "WEST":
            self.pos_x -= cells_to_move

        if (self.pos_x == 0 or self.pos_x == self.M - 1) and (self.pos_y == 0 or self.pos_y == self.N - 1):
            self.stopped = True
