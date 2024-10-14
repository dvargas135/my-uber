from src.models.grid_model import Grid

class System:
    def __init__(self, N, M):
        self.taxis = {}
        self.grid = Grid(N, M)

    def register_taxi(self, taxi):
        taxi_id = taxi.taxi_id
        self.taxis[taxi_id] = taxi

    def update_taxi_position(self, taxi_id, new_pos_x, new_pos_y):
        if taxi_id in self.taxis:
            taxi = self.taxis[taxi_id]
            taxi.pos_x = new_pos_x
            taxi.pos_y = new_pos_y
