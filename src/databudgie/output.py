from rich import console, progress
from rich.table import Table
from rich.theme import Theme
from rich.traceback import Traceback


class Console(console.Console):
    theme = Theme({"trace": "white", "info": "blue", "warn": "yellow", "error": "bold red"})

    def __init__(self, verbosity=0):
        super().__init__(theme=self.theme, log_time=True)
        self.verbosity = verbosity

    def trace(self, message):
        if self.verbosity >= 1:
            return self.log(message, style="trace")
        return None

    def info(self, message):
        return self.log(message, style="info")

    def warn(self, message):
        return self.log(message, style="warn")

    def error(self, message):
        return self.log(message, style="error")

    def exception(self, e):
        tb = Traceback.from_exception(type(e), e, e.__traceback__.tb_next, max_frames=1)
        return self.log(tb, style="error")


class Progress(progress.Progress):
    def __init__(self, console):
        columns = (
            progress.TaskProgressColumn(),
            progress.TimeRemainingColumn(),
            progress.BarColumn(),
            progress.TextColumn("[progress.description]{task.description}"),
        )
        super().__init__(*columns, console=console, transient=True)

    def update(self, task, description, advance=1):
        return super().update(task, description=f"[trace]{description}[/trace]", advance=advance)


default_console = Console()


__all__ = [
    "Console",
    "default_console",
    "Progress",
    "Traceback",
    "Table",
]
