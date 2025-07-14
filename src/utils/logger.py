import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler


def setup_logger(logs_dir=Path('logs'), log_file='run.log'):
    """Sets up a logger that writes to both console and a file."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / log_file

    logger = logging.getLogger('CinematicDatasetCollector')
    logger.setLevel(logging.INFO)

    # Avoid adding handlers multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler
    fh = RotatingFileHandler(log_path, maxBytes=10*1024*1024, backupCount=5)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

# Create a default logger instance
log = setup_logger()
