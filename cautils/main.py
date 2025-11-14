from cyclopts import App
from . import data_agent
from . import bq_metadata

remote_app = App()
remote_app.command(data_agent.app)
remote_app.command(bq_metadata.app)


# @remote_app.command
def main(name: str, count: int):
    """Help string for this demo application.

    Parameters
    ----------
    name: str
        Name of the user to be greeted.
    count: int
        Number of times to greet.
    """
    for _ in range(count):
        print(f"Hello {name}!")


remote_app()
