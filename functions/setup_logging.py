import logging
from datetime import datetime
import os
from logging.handlers import RotatingFileHandler


def setup_logging():
    # Set up logging
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.setLevel(logging.DEBUG)

        # Directory where log files will be stored
        log_dir = '../logs'  # Adjust this path as needed

        # Create the log directory if it doesn't exist
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Format datetime once to ensure consistency
        formatted_datetime = datetime.now().strftime('(%Y-%m-%d, %H:%M:%S)')

        # Output for errors and warnings to 'DG2RM2_error.log'
        file_handler_error = logging.FileHandler(os.path.join(log_dir, 'DG2RM2_error.log'), encoding='utf-8')
        file_handler_error.setLevel(logging.WARNING)
        file_handler_error.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler_error)

        # Output for info and debug to 'DG2RM.log'
        file_handler_info = RotatingFileHandler(os.path.join(log_dir, 'DG2RM.log'), maxBytes=1000000, backupCount=3,
                                                encoding='utf-8')
        file_handler_info.setLevel(logging.INFO)
        file_handler_info.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler_info)

        # Console output
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(console_handler)

        root_logger.critical(f"\n\n Start of run - {formatted_datetime} \n")

    setup_logging()
