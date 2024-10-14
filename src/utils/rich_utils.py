from rich.console import Console
from rich.table import Table
from rich.live import Live

class RichConsoleUtils:
    def __init__(self):
        self.console = Console()

    def print(self, message, level=1, show_level=True, end="\n"):
        levels = {
            1: ("INFO", "bold blue"),
            2: ("WARNING", "bold yellow"),
            3: ("ERROR", "bold red"),
            4: ("SUCCESS", "bold green"),
        }

        level_text, color = levels.get(level, ("INFO", "bold blue"))

        if show_level:
            formatted_message = f"[{color}]{level_text}:[/{color}] {message}"
        else:
            formatted_message = f"[{color}]{message}[/{color}]"

        self.console.print(formatted_message, end=end, highlight=False)
        """
        if end == "\r":
            self.console.print(formatted_message, end="", highlight=False)
            self.console.print(end, end="", highlight=False)
        else:
            self.console.print(formatted_message, end=end, highlight=False)"""

    def create_table(self, title, columns):
        table = Table(title=title)
        for column in columns:
            table.add_column(column, justify="center")
        return table

    def update_live_table(self, table, data, live_display):
        existing_rows = {row.cells[0].text: row for row in table.rows}

        for row_data in data:
            taxi_id = str(row_data[0])
            pos_x = str(row_data[1])
            pos_y = str(row_data[2])
            speed = str(row_data[3])
            status = str(row_data[4])
            connected = str(row_data[5])

            if taxi_id in existing_rows:
                row_index = table.rows.index(existing_rows[taxi_id])
                table.rows[row_index].cells[1].text = pos_x
                table.rows[row_index].cells[2].text = pos_y
                table.rows[row_index].cells[3].text = speed
                table.rows[row_index].cells[4].text = status
                table.rows[row_index].cells[5].text = connected
            else:
                table.add_row(taxi_id, pos_x, pos_y, speed, status, connected)

        live_display.update(table)

    def start_live_display(self, table, refresh_per_second=2):
        return Live(table, refresh_per_second=refresh_per_second, console=self.console)
