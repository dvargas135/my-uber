from src.models.grid_model import Grid
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, TIMESTAMP
from sqlalchemy.orm import relationship
from src.services.database_service import Base

class Taxi(Base):
    __tablename__ = 'taxis'  # Name of the table in the database

    id = Column(Integer, primary_key=True, autoincrement=True)
    taxi_id = Column(String(50), unique=True, nullable=False)
    pos_x = Column(Integer, nullable=False)
    pos_y = Column(Integer, nullable=False)
    speed = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False)
    connected = Column(Boolean, default=False)

    assignments = relationship("Assignment", back_populates="taxi")
    heartbeats = relationship("Heartbeat", back_populates="taxi")

    def __init__(self, taxi_id, N, M, pos_x, pos_y, speed, status, connected=False):
        self.taxi_id = taxi_id
        self.initial_pos_x = pos_x
        self.initial_pos_y = pos_y
        self.pos_x = pos_x
        self.pos_y = pos_y
        self.speed = speed
        self.grid = Grid(N, M)
        self.N = N  # Grid rows (Y-axis)
        self.M = M  # Grid columns (X-axis)
        self.status = status
        self.connected = connected
        self.stopped = False
        self.move_counter = 0
        self.was_off_borders = False  # Tracks if the taxi has moved off all borders

    def move(self, direction, cells_to_move):
        if self.stopped:
            return

        # Determine new position based on direction and speed
        if direction == "NORTH":
            max_possible_move = self.N - self.pos_y  # Up to pos_y=10
            actual_move = min(cells_to_move, max_possible_move)
            new_pos_y = self.pos_y + actual_move
            new_pos_x = self.pos_x
        elif direction == "SOUTH":
            max_possible_move = self.pos_y - 0  # Down to pos_y=0
            actual_move = min(cells_to_move, max_possible_move)
            new_pos_y = self.pos_y - actual_move
            new_pos_x = self.pos_x
        elif direction == "EAST":
            max_possible_move = self.M - self.pos_x  # East to pos_x=10
            actual_move = min(cells_to_move, max_possible_move)
            new_pos_x = self.pos_x + actual_move
            new_pos_y = self.pos_y
        elif direction == "WEST":
            max_possible_move = self.pos_x - 0  # West to pos_x=0
            actual_move = min(cells_to_move, max_possible_move)
            new_pos_x = self.pos_x - actual_move
            new_pos_y = self.pos_y
        else:
            raise ValueError(f"Invalid direction: {direction}")

        # Update position
        self.pos_x = new_pos_x
        self.pos_y = new_pos_y

        # Check if the taxi is currently on any border
        on_border = (
            self.pos_x == 0 or self.pos_x == self.M or
            self.pos_y == 0 or self.pos_y == self.N
        )

        # Debugging Output (Can be removed or replaced with proper logging)
        # print(
        #     f"Taxi {self.taxi_id} moved {direction} to ({self.pos_x}, {self.pos_y}). "
        #     f"On border: {on_border}. Was off borders: {self.was_off_borders}"
        # )

        if on_border:
            if self.was_off_borders:
                self.stopped = True
                print(f"Taxi {self.taxi_id} has stopped moving at ({self.pos_x}, {self.pos_y}).")
                return  # Exit the move function
            # If still on borders and hasn't moved off yet, continue moving
        else:
            if not self.was_off_borders:
                self.was_off_borders = True
                print(f"Taxi {self.taxi_id} has moved off the borders.")

    def can_move(self, direction):
        if direction == "NORTH":
            return self.pos_y < self.N  # Can move NORTH if not at pos_y=10
        elif direction == "SOUTH":
            return self.pos_y > 0      # Can move SOUTH if not at pos_y=0
        elif direction == "EAST":
            return self.pos_x < self.M  # Can move EAST if not at pos_x=10
        elif direction == "WEST":
            return self.pos_x > 0      # Can move WEST if not at pos_x=0
        else:
            return False
