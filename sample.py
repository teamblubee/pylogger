import os
import logging
import asyncio
import argparse
import sys

from AsyncBufferedRedisHandler import AsyncBufferedRedisHandler, ConditionalFormatter

def initialize_logger(logger_name, log_filename):
    """
    Initializes and returns a logger with the specified name and log filename.

    Args:
        logger_name (str): The name of the logger.
        log_filename (str): The path to the log file.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # Initialize the AsyncBufferedRedisHandler with the provided log filename
    redis_handler = AsyncBufferedRedisHandler(
        buffer_size=10,
        flush_interval=5,
        start_tty_writer=True,  # Starts the TTY writer automatically
        filename=log_filename,  # Use the provided log filename
        max_bytes=10 * 1024 * 1024,  # 10 MB
        backup_count=100
    )
    
    # Define the log message format
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = ConditionalFormatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        optional_fields=['pid', 'ZZZpid']
    )
    redis_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(redis_handler)
    
    return logger

async def main(logger_name, log_filename):
    """
    Main asynchronous function to log messages.

    Args:
        logger_name (str): The name of the logger.
        log_filename (str): The path to the log file.
    """
    # print(f"PRE INIT LOGGER")
    logger = initialize_logger(logger_name, log_filename)
    # print(f"POS INIT LOGGER")

    _ad = False
    # Log messages in an infinite loop (adjust as needed)
    try:
        for i in range(100000000000):
        # for i in range(2):
        # for i in range(1):
        # for i in range(3):
            # logger.info(f"Logging message {i}", extra={'ZZZpid': os.getpid()})
            logger.info(f"Logging message {i}")
            await asyncio.sleep(0.001)
            if i > 100 and not _ad:
                logger.info(f"Logging message {i}", extra={'ZZZpid': os.getpid()})
                _ad = True

    except asyncio.CancelledError: pass
    finally:
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)

def parse_arguments():
    """
    Parses command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments containing the logger name and log filename.
    """
    parser = argparse.ArgumentParser(description="Asynchronous Logging Script")
    
    parser.add_argument(
        '--log_file',
        type=str,
        required=True,
        help='Path to the log file. Example: --log-file /var/log/myapp.log'
    )
    
    parser.add_argument(
        '--logger_name',
        type=str,
        required=True,
        help='Name of the logger. Example: --logger-name my_custom_logger'
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command-line arguments
    args = parse_arguments()
    
    # Validate that both log_file and logger_name are provided
    if not args.log_file:
        print("Error: --log_file argument is required.")
        sys.exit(1)
    
    if not args.logger_name:
        print("Error: --logger_name argument is required.")
        sys.exit(1)
    
    # Run the main asynchronous function with the provided logger name and log filename
    try:
        asyncio.run(main(args.logger_name, args.log_file))
    except KeyboardInterrupt:
        print("Logging interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
