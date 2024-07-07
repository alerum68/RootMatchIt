def setup_logging():
    # Set up logging
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.setLevel(logging.DEBUG)

        # Output for errors and warnings to 'DG2RM2_error.log'
        file_handler_error = logging.FileHandler('DG2RM2_error.log', encoding='utf-8')
        file_handler_error.setLevel(logging.WARNING)
        file_handler_error.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler_error)

        # Output for info and debug to 'DG2RM.log'
        file_handler_info = RotatingFileHandler('DG2RM.log', maxBytes=1000000, backupCount=3, encoding='utf-8')
        file_handler_info.setLevel(logging.INFO)
        file_handler_info.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler_info)

        # Console output
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(console_handler)

        root_logger.info(f"\n\n Start of run - {datetime.now().strftime('(%Y-%m-%d, %H:%M:%S)')} \n")
