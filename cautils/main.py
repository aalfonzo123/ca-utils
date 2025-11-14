from cyclopts import App
from . import data_agent
from . import bq_metadata
from . import da_lro

app = App()
app.command(data_agent.app)
app.command(bq_metadata.app)
app.command(da_lro.app)

app()
