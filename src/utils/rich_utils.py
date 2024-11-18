from rich.console import Console
from rich.table import Table
from rich.live import Live

class RichConsoleUtils:
    def __init__(self):
        self.console = Console()

    def print(self, message, level=1, show_level=False, end="\n"):
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

    def create_table(self, title, columns, styles=None):
        table = Table(title=title)
        for column in columns:
            if styles and column in styles:
                if column not in ["Status", "Connected"]:
                    table.add_column(column, justify="center", style=styles[column])
                else:
                    table.add_column(column, justify="center")
            else:
                table.add_column(column, justify="center")
        return table

    def generate_table(self, title, columns, data, styles=None):
        header_styles = {
            "Taxi ID": "cyan",
            "Position X": "magenta",
            "Position Y": "magenta",
            "Speed": "yellow",
            # Exclude "Status" and "Connected" from header styles
        }

        table = self.create_table(title, columns, styles=header_styles)

        for row_data in data:
            table.add_row(*row_data)

        return table

    def update_live_table(self, table, data, live_display):
        table.rows.clear()

        for row_data in data:
            taxi_id = str(row_data[0])
            pos_x = str(row_data[1])
            pos_y = str(row_data[2])
            speed = str(row_data[3])
            status = row_data[4]
            connected = row_data[5]

            if status.lower() == "available":
                status_colored = "[light_green]active[/light_green]"
            elif connected:
                connected_colored = "[light_green]True[/light_green]"
            elif status.lower() == "unavailable":
                status_colored = "[red]unavailable[/red]"
            elif not connected:
                connected_colored = "[red]False[/red]"
            else:
                status_colored = status
                connected_colored = "True" if connected else "False"

            table.add_row(
                taxi_id,
                pos_x,
                pos_y,
                speed,
                status_colored,
                connected_colored
            )

        live_display.update(table)

    def start_live_display(self, table, refresh_per_second=2):
        return Live(table, refresh_per_second=refresh_per_second, console=self.console)
