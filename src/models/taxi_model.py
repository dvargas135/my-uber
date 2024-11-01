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
        self.N = N
        self.M = M
        self.status = status
        self.connected = connected
        self.stopped = False
        self.move_counter = 0
        self.initial_borders = set()

        if self.pos_x == 0:
            self.initial_borders.add('x=0')
        if self.pos_x == self.M - 1:
            self.initial_borders.add(f'x={self.M -1}')
        if self.pos_y == 0:
            self.initial_borders.add('y=0')
        if self.pos_y == self.N -1:
            self.initial_borders.add(f'y={self.N -1}')

        self.has_left_initial_borders = False

    def move(self, direction, cells_to_move):
        if self.stopped:
            return

        if direction == "NORTH":
            max_possible_move = self.N - self.pos_y
            actual_move = min(cells_to_move, max_possible_move)
            new_pos_y = self.pos_y + actual_move
            new_pos_x = self.pos_x
        elif direction == "SOUTH":
            max_possible_move = self.pos_y - 0
            actual_move = min(cells_to_move, max_possible_move)
            new_pos_y = self.pos_y - actual_move
            new_pos_x = self.pos_x
        elif direction == "EAST":
            max_possible_move = self.M - self.pos_x
            actual_move = min(cells_to_move, max_possible_move)
            new_pos_x = self.pos_x + actual_move
            new_pos_y = self.pos_y
        elif direction == "WEST":
            max_possible_move = self.pos_x - 0
            actual_move = min(cells_to_move, max_possible_move)
            new_pos_x = self.pos_x - actual_move
            new_pos_y = self.pos_y
        else:
            raise ValueError(f"Invalid direction: {direction}")

        self.pos_x = new_pos_x
        self.pos_y = new_pos_y

        current_borders = set()
        
        if self.pos_x == 0:
            current_borders.add('x=0')
        if self.pos_x == self.M:
            current_borders.add(f'x={self.M}')
        if self.pos_y == 0:
            current_borders.add('y=0')
        if self.pos_y == self.N:
            current_borders.add(f'y={self.N}')

        if not self.has_left_initial_borders:
            if current_borders == self.initial_borders:
                pass
            else:
                self.has_left_initial_borders = True
                if current_borders:
                    self.stopped = True
                    print(f"Taxi {self.taxi_id} has stopped moving at ({self.pos_x}, {self.pos_y}).")
                    return
        else:
            if current_borders:
                self.stopped = True
                print(f"Taxi {self.taxi_id} has stopped moving at ({self.pos_x}, {self.pos_y}).")
                return

    def can_move(self, direction):
        if direction == "NORTH":
            return self.pos_y < self.N
        elif direction == "SOUTH":
            return self.pos_y > 0
        elif direction == "EAST":
            return self.pos_x < self.M
        elif direction == "WEST":
            return self.pos_x > 0
        else:
            return False
