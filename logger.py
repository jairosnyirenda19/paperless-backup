import logging
import sys
from datetime import date
from config import LOGS_DIR

# creating logger
logger = logging.getLogger()

# creating formatter
formatter = logging.Formatter(fmt="[%(asctime)s] %(name)s.%(levelname)s: %(message)s")

# creating streams
stream_handler = logging.StreamHandler(stream=sys.stdout)

log_file_date  = date.today().strftime("%d_%m_%Y")
log_filename   = f"{LOGS_DIR}/app_{log_file_date}.log"
file_handler   = logging.FileHandler(filename=log_filename)

# set formatters
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# add handlers to the logger instance
logger.handlers = [stream_handler, file_handler]

# set log level
logger.setLevel(logging.INFO)