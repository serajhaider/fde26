import logging
from logging.handlers import TimedRotatingFileHandler  
from pathlib import Path
from datetime import datetime

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

_configured = False

def setup_file_logging(run_name: str = "elt_pipeline", log_level: int = logging.INFO) -> Path:
    """Attach a rotating file handler to Prefect's logger tree.

    Prefect emits flow/task logs through loggers under the "prefect" namespace
    (prefect.flow_runs, prefect.task_runs, etc). Attaching here means every
    @task and @flow log line — including dbt output streamed through
    PrefectDbtRunner — lands in this file automatically, with no changes
    needed inside individual tasks.
    """

    global _configured
    log_file = LOG_DIR / f"log_{run_name}.log"

    if _configured:
        return log_file

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    
    handler = TimedRotatingFileHandler(log_file, when='midnight', backupCount=14)
    handler.setFormatter(formatter)
    handler.setLevel(log_level)

    # 1. Attach handler to ROOT logger (captures custom classes & standard libraries)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)


    prefect_logger = logging.getLogger("prefect")
    prefect_logger.addHandler(handler)
    prefect_logger.setLevel(log_level)
    prefect_logger.propagate = True

    # 3. Optional: Suppress noisy 3rd-party HTTP/SDK logs unless they fail
    for third_party in ["urllib3", "boto3", "botocore", "httpx"]:
        logging.getLogger(third_party).setLevel(logging.WARNING)

    _configured = True
    return log_file