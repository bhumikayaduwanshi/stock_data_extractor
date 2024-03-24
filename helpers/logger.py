import logging
import logging.handlers
import os
import time
import functools
from pathlib import Path
from datetime import date
from config.config import CONFIG


def setup_logger(log_file, logger_name):
    # Create a logger
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # Create a file handler and set the log level
    file_handler = logging.FileHandler(
        log_file, mode="a", encoding=None, delay=False)
    file_handler.setLevel(logging.INFO)

    # # create an email handler
    smtp_handler = logging.handlers.SMTPHandler(mailhost=('smtp.gmail.com', 587),
                                                fromaddr=CONFIG.get('smtp').get('email'),
                                                toaddrs=[CONFIG.get('smtp').get('email')],
                                                subject='Error Log Mail',
                                                credentials=(CONFIG.get('smtp').get('email'), CONFIG.get('smtp').get('password')),
        secure=())

    smtp_handler.setLevel(logging.WARNING)
    # Create a formatter and set it on the file handler
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add the file handler to the logger
    logger.addHandler(file_handler)  # file handler
    logger.addHandler(smtp_handler)  # email handler

    return logger


def log_execution_time(logger):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"Executing '{func.__name__}' now!")
            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                logger.exception(
                    f"Exception occurred in function '{func.__name__}': {str(e)}")
                raise
            finally:
                end_time = time.perf_counter()
                elapsed_time = end_time - start_time
                logger.info(
                    f"Function '{func.__name__}' execution time: {elapsed_time:.4f} seconds")
            return result
        return wrapper
    return decorator


script_logger = setup_logger(Path(CONFIG.get('directory').get('log').get(
    'execution')).joinpath(f'execution_time_{date.today()}.log'), 'execution_logger')

error_logger = setup_logger(Path(CONFIG.get('directory').get('log').get(
    'error')).joinpath(f'error_{date.today()}.log'), 'error_logger')
