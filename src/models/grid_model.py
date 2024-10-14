class Grid:
    def __init__(self, rows, cols):
        self.rows = rows
        self.cols = cols

    def is_within_bounds(self, x, y):
        return 0 <= x < self.rows and 0 <= y < self.cols
