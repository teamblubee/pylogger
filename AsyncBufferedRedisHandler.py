# AsyncBufferedRedisHandler.py

import os
import sys
import time
import uuid
import queue
import signal
import orjson
import logging
import threading
import redis as redis
from pathlib import Path
# from queue import Queue, Empty

from AsyncRedisTTYWriter import AsyncRedisTTYWriter
from AsyncRedisFileWriter import AsyncRedisFileWriter

from multiprocessing import Process, Queue, Event

def generate_uuid_from_filename(filename):
    """
    Generates a deterministic UUID based on the provided filename.
    
    Args:
        filename (str): The filename to generate the UUID from.
    
    Returns:
        str: A string representation of the UUID.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, filename))

class ConditionalFormatter(logging.Formatter):
    """
    Custom formatter to conditionally add parts of the log message
    if specific attributes exist in the log record.
    """
    def __init__(self, fmt=None, datefmt=None, optional_fields=None):
        """
        Initialize the ConditionalFormatter.

        Args:
            fmt (str): Base format string for the log message.
            datefmt (str): Date format string.
            optional_fields (list): List of field names that should be conditionally added.
        """
        self.base_format = fmt
        self.datefmt = datefmt
        self.optional_fields = optional_fields if optional_fields else []
        super().__init__(fmt=self.base_format, datefmt=self.datefmt)

    def format(self, record):
        # Start with the base format
        dynamic_format = self.base_format

        # Check each of the optional fields and add it to the format if it exists
        for field in self.optional_fields:
            if hasattr(record, field):
                dynamic_format += f" - {field}: %({field})s"

        # Set the dynamic format to the formatter
        self._style._fmt = dynamic_format

        # Now format the record with the dynamically created format string
        return super().format(record)

class AsyncBufferedRedisHandler(logging.Handler):
    """
    A custom logging handler that buffers log records before sending them to a Redis stream_name.
    Automatically starts the AsyncRedisTTYWriter and AsyncRedisFileWriter if not already running, using Redis for synchronization.
    Detects the TTY associated with the process when initialized.
    """

    def __init__(self,
                 redis_host='localhost',
                 redis_port=6379,
                 redis_db=0,
                 stream_name=None,
                 buffer_size=1,
                 flush_interval=0.01,
                 start_tty_writer=True,
                 filename=None,
                 max_bytes=10*1024*1024,
                 backup_count=5,
                 lock_timeout=10,
                 retry_attempts=5,
                 retry_delay=1
                 ):
        super().__init__()
        self.init_closed      = False 
        self.stop_event       = Event()
        self.start_tty_writer = start_tty_writer
        self.redis_host       = redis_host
        self.redis_port       = redis_port
        self.redis_db         = redis_db
        self.stream_name      = stream_name if stream_name else f"{self.__class__.__name__}_{uuid.uuid4()}_{os.getpid()}"
        self.buffer_size      = buffer_size
        self.flush_interval   = flush_interval
        self.filename         = filename
        self.max_bytes        = max_bytes
        self.backup_count     = backup_count
        self.lock_timeout     = lock_timeout
        self.retry_attempts   = retry_attempts
        self.retry_delay      = retry_delay

        # Initialize log queue and worker thread
        self.log_queue        = queue.Queue()
        self.worker_thread    = threading.Thread(target=self._process_queue, daemon=True)
        # self.worker_thread    = threading.Thread(target=self._process_queue)
        self.worker_thread.start()

        # Set up signal handlers
        self._setup_signal_handlers()

        # Initialize writers dictionary
        self.writers = {}

        # Initialize Redis client
        self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db)

        # Detect the TTY associated with the process
        self.tty = self._detect_tty()

        # Create writer if configured to do so
        if start_tty_writer:
            self._initialize_tty_writer()

        # Create file writer if filename is provided
        if filename:
            self.filename = self._validate_filename(filename)
            self._initialize_file_writer()


    def _writer_process(self, writer_type=None, stop_event=None):
        """
        Function to initialize and run a writer in a dedicated process.
        The stop_event is used to signal the writer to stop gracefully.
        """
        if writer_type == 'tty':
            # Initialize the TTY writer
            tty_writer = AsyncRedisTTYWriter(
                redis_host=self.redis_host,
                redis_port=self.redis_port,
                redis_db=self.redis_db,
                stream_name=self.stream_name,
                stop_event=stop_event
            )

        elif writer_type == 'file':
            # Initialize the File writer
            file_writer = AsyncRedisFileWriter(
                redis_host=self.redis_host,
                redis_port=self.redis_port,
                redis_db=self.redis_db,
                stream_name=self.stream_name,
                filename=self.filename,
                max_bytes=self.max_bytes,
                backup_count=self.backup_count,
                stop_event=stop_event
            )

    def _initialize_tty_writer(self, daemon:bool=True):
        """
        Initializes a TTY writer and starts it in a dedicated process.
        """
        # Create an event for signaling the writer to stop
        # stop_event = Event()

        # Start the writer process and pass the stop_event
        writer_process = Process(target=self._writer_process, args=('tty', self.stop_event))
        writer_process.daemon = daemon
        writer_process.start()

        # Store the process and stop_event for management
        self.writers['tty'] = {
            'process': writer_process
        }

    def _initialize_file_writer(self, daemon:bool=True):
        """
        Initializes a File writer and starts it in a dedicated process.
        """
        # Create an event for signaling the writer to stop
        # stop_event = Event()

        # Start the writer process and pass the stop_event
        writer_process = Process(target=self._writer_process, args=('file', self.stop_event))
        writer_process.daemon = daemon
        writer_process.start()

        # Store the process and stop_event for management
        self.writers[self.filename] = {
            'process': writer_process
        }


    def _detect_tty(self):
        """
        Detects the TTY associated with the process's stdout or stderr.
        Returns the TTY path or 'no_tty' if not found.
        """
        try:
            if os.isatty(sys.stdout.fileno()):
                return os.ttyname(sys.stdout.fileno())
            elif os.isatty(sys.stderr.fileno()):
                return os.ttyname(sys.stderr.fileno())
            else:
                return 'no_tty'
        except Exception as e:
            print(f"Failed to detect TTY: {e}")
            return 'no_tty'

    def _validate_filename(self, filename):
        """
        Validates the provided filename.

        Args:
            filename (str): Path to the log file.

        Returns:
            str: Absolute path to the log file.

        Raises:
            ValueError: If the filename is invalid.
            FileNotFoundError: If the log directory does not exist.
            PermissionError: If the file cannot be created due to permission issues.
        """
        if not isinstance(filename, str):
            raise ValueError("filename must be a string representing the file path.")

        path = Path(filename).resolve()

        if path.is_dir():
            raise ValueError(f"filename '{filename}' is a directory. Please provide a valid file path.")

        log_dir = path.parent
        if not log_dir.exists():
            raise FileNotFoundError(f"Log directory '{log_dir}' does not exist. Please create it before initializing the writer.")

        if not path.exists():
            try:
                with open(path, 'a'):
                    os.utime(path, None)
                print(f"Created log file '{path}'.")
            except PermissionError as e:
                raise PermissionError(f"Insufficient permissions to create log file '{path}': {e}")
            except Exception as e:
                raise IOError(f"Failed to create log file '{path}': {e}")

        return str(path)



    def emit(self, record):
        """
        Adds the log record to the queue for processing.
        Exits early if stop_event is set.
        """
        if self.init_closed or self.stop_event.is_set():
            sys.exit(0)
            return

        try:
            # Format the record for log message output
            formatted_record = self.format(record)

            # Create the base record dictionary with the standard fields
            record_dict = {
                "formatted_message": formatted_record,
                "level": record.levelname,
                "levelno": record.levelno,
                "name": record.name,
                "pathname": record.pathname,
                "filename": record.filename,
                "module": record.module,
                "lineno": record.lineno,
                "funcName": record.funcName,
                "created": record.created,
                "asctime": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(record.created)),
                "msecs": record.msecs,
                "relativeCreated": record.relativeCreated,
                "thread": record.thread,
                "threadName": record.threadName,
                "process": record.process,
                "message": record.getMessage(),  # The original message with substitutions
                "tty": self.tty,
            }

            # Add any custom 'extra' fields to the record dictionary
            for key, value in record.__dict__.items():
                if key not in record_dict and not key.startswith('_'):
                    record_dict[key] = value

            # Handle known problematic fields like 'args' and other complex types explicitly
            if isinstance(record.args, tuple):
                # Convert the 'args' tuple to a string representation
                record_dict['args'] = str(record.args)

            if record.exc_info:
                # Convert 'exc_info' to a string representation if present (usually traceback info)
                record_dict['exc_info'] = self.formatException(record.exc_info)


            # Ensure all values are Redis-compatible (no None types)
            for key, value in record_dict.items():
                if value is None:
                    record_dict[key] = ''  # Replace None with an empty string

            # print(record_dict)
            self.log_queue.put(record_dict)
        except Exception as e:
            print(f"EXCEPTION::[{str(e)}]")
            self.handleError(record)


    def _setup_signal_handlers(self):
        """
        Set up signal handlers to ensure proper cleanup on termination signals.
        """
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        """
        Handle termination signals to ensure the stream is deleted, and propagate the signal.
        """
        # print(f"Received termination signal: {signum}, closing handler.")
        self.stop_event.set()

    def _process_queue(self):
        """
        Continuously process the log queue and send records to Redis.
        Exits when stop_event is set or a None is placed in the queue.
        """
        try:
            while not self.stop_event.is_set():
                try:
                    record = self.log_queue.get(timeout=1)
                    if record is None:
                        print(f"Received None, breaking out of queue processing.")
                        break
                    self.redis_client.xadd(self.stream_name, record)
                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"Failed to process log record: {e}")

        finally:
            # print(f"BREAK OUT OF PROCESS QUEUE - about to close Redis client")
            self.redis_client.close()  # Ensure Redis client is properly closed in the thread
            # print(f"Redis client closed, thread should be exiting now.")


    def close(self):
        """
        Close the handler, check for active consumers, and flush any remaining logs.
        If there are no active consumers, delete the stream from Redis before exiting.
        """
        if self.init_closed:
            # print("Handler is already closing, skipping.")
            return

        self.init_closed = True
        try:
            self.stop_event.set()
        except Exception as e:
            print(f"close exception::[{str(e)}]")

        if self.redis_client:
            try:
                # Check if there are any consumers reading from the stream
                groups_info = self.redis_client.xinfo_groups(self.stream_name)
                active_consumers = sum(group['consumers'] for group in groups_info)
                if active_consumers == 0:
                    # print(f"No consumers, deleting stream name::[{self.stream_name}]")
                    self.redis_client.delete(self.stream_name)
            except redis.ResponseError as e:
                if "no such key" not in str(e).lower():
                    print(f"Failed to delete stream from Redis: {e}")
            finally:
                self.redis_client.close()

        # print(f"[{self.writers}]")
        for writer_info in self.writers.values():
            process = writer_info['process']

            if process is not None and self.stop_event is not None:
                # print(f"Signaling writer process {process.pid} to stop")
                process.join()

        # print(f"# Stop the logging queue worker thread")
        self.log_queue.put(None)
        # print(f"A")
        self.worker_thread.join()
        # print(f"B")
        super().close()
        # print(f"C")
        sys.exit(0)

