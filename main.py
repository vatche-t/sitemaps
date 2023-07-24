from pathlib import Path
import subprocess
import time
import traceback
import sys
from loguru import logger

import config

logger.add(sink="invoice_crawler.log", rotation="50 MB")

max_process_life_time = int(config.MAX_PROCESS_LIFE_TIME)
process_restart_delay_time = int(config.PROCESS_RESTART_DELAY_TIME)
parent_path = Path(__file__).parent.absolute()

while True:
    try:
        process = subprocess.run(
            [f"{parent_path}/.venv/bin/python3.10", f"{parent_path}/pipeline.py"],
            text=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            timeout=max_process_life_time,
        )
        logger.info(f"process exit status: {process.returncode}")

    except Exception as e:
        logger.info(f"exception on last main.py round \n traceback: {traceback.format_exc()}")

    finally:
        logger.info(f"going for next round after {process_restart_delay_time} sec")
        time.sleep(process_restart_delay_time)
