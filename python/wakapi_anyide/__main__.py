import asyncio
import logging
import sqlite3
import json
import requests
from json import dumps
from pathlib import Path
from typing import Annotated
from typing import TypeAlias

import typer

from wakapi_anyide import __version__
from wakapi_anyide.models.config import WakatimeConfig
from wakapi_anyide.models.environment import Environment
from wakapi_anyide.models.project import Project
from wakapi_anyide.runner import ConfigInvalidatedException
from wakapi_anyide.runner import run

logger = logging.getLogger(__name__)


DEFAULT_IGNOREFILES = [".gitignore"]
TEMPLATE = """
# https://github.com/iamawatermelo/wakapi-anyide v{version}

[meta]
version = 1
watchers = ['files']

[files]
include = {include}  # files to include in tracking
exclude = {exclude}  # files to exclude in tracking
exclude_files = {exclude_files}  # files whose contents will be used to exclude other files from tracking
exclude_binary_files = true  # whether to ignore binary files
# language_mapping = {{".kicad_sch" = "Kicad Schematic"}} # custom language mapping

[project]
name = "{name}"  # your project name
"""

app = typer.Typer(
    pretty_exceptions_enable=False  # they don't report asyncio taskgroup exceptions correctly
)

try:
    from rich.highlighter import Highlighter
    from rich.text import Text
    
    class NoHighlights(Highlighter):
        def highlight(self, text: Text) -> None:
            pass
except ImportError:
    pass


def setup_logging(is_verbose: bool):
    try:
        from rich.logging import RichHandler
        logging.basicConfig(
            level="DEBUG" if is_verbose else "INFO",
            format="[bold magenta]{module}[/bold magenta][bright_black]:{funcName}@{lineno:03}[/bright_black]  {message}",
            style='{',
            datefmt="[%X]",
            handlers=[RichHandler(highlighter=NoHighlights(), rich_tracebacks=True, markup=True, show_time=False, show_path=False)]
        )
    except ImportError:
        logging.basicConfig(
            level="DEBUG" if is_verbose else "INFO",
            format="{levelname} {filename}:{funcName}@{lineno:03} {message}",
            style='{',
            datefmt="[%X]"
        )
        logger.warning("Rich is not available, using basic logging (pip install rich?)")


Verbose: TypeAlias = Annotated[bool, typer.Option("--verbose", callback=setup_logging)]
Polling: TypeAlias = Annotated[bool, typer.Option("--filewatcher-use-polling")]


def start(is_test, is_polling):
    asyncio.run(run(Environment(
        is_test_only=is_test,
        polling_mode=is_polling,
        config=WakatimeConfig(),   # type: ignore
        project=Project()  # type: ignore
    )))


@app.command()
def test(verbose: Verbose = False, polling: Polling = False):
    start(True, polling)


@app.command()
def track(verbose: Verbose = False, polling: Polling = False):
    start(False, polling)

@app.command()
def send_failed_heartbeats(verbose: Verbose = False, polling: Polling = False):
    import base64
    import requests
    
    config = WakatimeConfig()

    con = sqlite3.connect("heartbeats.db")
    cur = con.cursor()

    heartbeats = cur.execute("SELECT data FROM heartbeats").fetchall()

    if not heartbeats:
        logger.info("No failed heartbeats found")
        return
    
    logger.info(f"Found {len(heartbeats)} failed heartbeats, sending them...")

    # Create user agent string
    user_agent = f"wakapi-anyide/{__version__}"

    try:
        data = [
            json.loads(heartbeat[0]) for heartbeat in heartbeats
        ]
        
        response = requests.post(
            f"{config.settings.api_url}/users/current/heartbeats.bulk",
            json=data,
            headers={
                "User-Agent": user_agent,
                "Authorization": f"Basic {base64.b64encode(config.settings.api_key.encode()).decode()}"
            }
        )
        
        if response.status_code == 201:
            logger.info("Successfully sent heartbeat")
            cur.execute("DELETE FROM heartbeats")
            con.commit()
        else:
            last_text = response.text
            logger.error(f"Failed to send heartbeat: {response.status_code} - {last_text}")
    except Exception as e:
        logger.error(f"Failed to decode heartbeat: {e}")
    
    con.close()
    
@app.command()
def version():
    print(f"wakapi-anyide v{__version__}")

def prompt(prompt, default: str | None = None):
    try:
        from rich import get_console
        return get_console().input(f"[bold magenta]{prompt}[/bold magenta] [cyan](default {default})[/cyan]\n[bright_cyan]>>> [bright_cyan]") or default
    except ImportError:
        return input(f"{prompt}\n>>> ") or default


def prompt_choices(prompt, choices, default):
    try:
        from rich import get_console
        response = get_console().input(f"[bold magenta]{prompt} {repr(choices)}[/bold magenta] [cyan](default {default})[/cyan]\n[bright_cyan]>>> [bright_cyan]")
    except ImportError:
        response = input(f"{prompt} {repr(choices)} (default {default})\n>>> ")
    
    if response == "":
        return default
    
    if response in choices:
        return response
    else:
        raise ValueError("response not allowed")


def prompt_yn(prompt, default: bool):
    prompt_str = "[Y/n]" if default else "[y/N]"
    
    try:
        from rich import get_console
        response = get_console().input(f"[bold magenta]{prompt}[/bold magenta] [cyan]\\{prompt_str}: [cyan]")
    except ImportError:
        response = input(f"{prompt} {prompt_str}: ")
        
    if response == "":
        return default
    
    if response.lower() not in ("y", "n"):
        raise ValueError("expected y or n")
    
    return response.lower() == "y"


@app.command()
def setup():
    output = Path("wak.toml")
    if output.exists():
        raise Exception("a wak.toml already exists in this directory")
        
    project_type = prompt_choices("What kind of project do you have?", ["files"], "files")
        
    project_name = prompt("What's your project name?", default=Path("./").absolute().name)
    
    included_paths = list()
    if prompt_yn("Would you like to watch all files in the directory?", True):
        included_paths.append("*")
    elif prompt_yn("Would you like to add include paths?", True):
        while True:
            included_paths.append(prompt("Please enter a path to include in gitignore format (e.g /src)"))
            
            if not prompt_yn("Would you like to add another include path?", True):
                break
    
    excluded_paths = list()
    if prompt_yn("Would you like to add exclude paths?", False):
        while True:
            excluded_paths.append(prompt("Please enter a path to exclude in gitignore format (e.g /node_modules)"))
            
            if not prompt_yn("Would you like to add another exclude path?", False):
                break
    
    exclude_files = []
    for file in DEFAULT_IGNOREFILES:
        if Path(file).exists():
            exclude_files.append(file)
    
    with open(output, 'w') as file:
        file.write(TEMPLATE.format(
            version=__version__,
            include=dumps(included_paths),
            exclude=dumps(excluded_paths),
            exclude_files=dumps(exclude_files),
            name=project_name
        ).strip())


if __name__ == "__main__":
    app()