import os
import redis
import logging
import threading
import signal
from logging.handlers import RotatingFileHandler

class AsyncRedisFileWriter:
    """
    A writer that subscribes to a Redis stream and writes log records to a rotating file.
    """

    def __init__(
            self,
            redis_host='localhost',
            redis_port=6379,
            redis_db=0,
            stream_name=None,
            filename=None,
            stop_event=None,
            max_bytes=10*1024*1024,
            backup_count=5,
            last_id='0-0',
            block=None,
            lock_timeout=1  # seconds
            ):
        """
        Initializes the AsyncRedisFileWriter.
        """
        if not filename:
            raise ValueError("No filename provided for AsyncRedisFileWriter. Exiting.")
        
        self.init_closed = False
        self.stop_event = stop_event
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.stream_name = stream_name
        self.filename = os.path.abspath(filename)
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.redis_client = None
        self.running = False
        self.last_id = last_id
        self.block = block
        self.lock_timeout = lock_timeout

        # Initialize Redis client
        self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        self.running = True

        # Initialize the rotating file handler without setting a formatter
        try:
            self.file_handler = RotatingFileHandler(self.filename, maxBytes=self.max_bytes, backupCount=self.backup_count)
            self.file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        except Exception as e:
            raise IOError(f"Failed to initialize RotatingFileHandler for '{self.filename}': {e}")

        # Register the file writer in Redis
        self._register_writer_in_redis()

        # Start a thread for listening to the Redis stream
        self.listener_thread = threading.Thread(target=self._listen)
        self.listener_thread.start()


    def _setup_signal_handlers(self):
        """
        Set up signal handlers to ensure cleanup is called on termination signals.
        """
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        """
        Handle termination signals to ensure the writer is closed and cleaned up.
        """
        if not self.init_closed:
            self.init_closed = True

        if self.init_closed:
            return

        # print(f"Received termination signal: {signum}, closing writer.")
        self.close()
        signal.signal(signum, signal.SIG_DFL)  # Re-register the signal to default behavior
        os.kill(os.getpid(), signum)  # Re-raise the signal to terminate the process


    def _register_writer_in_redis(self):
        """
        Registers this file writer in Redis with necessary metadata.
        """
        registry_key = f"file_writer_registry:{self.filename}"
        lock_key = f"file_writer_lock:{self.filename}"

        try:
            # Acquire Redis lock to ensure this is the only writer initializing the file writer
            with self.redis_client.lock(lock_key, timeout=self.lock_timeout):
                writer_metadata = {
                    "file_path": self.filename
                }
                self.redis_client.hmset(registry_key, writer_metadata)
                # print(f"Registered file writer for '{self.filename}' in Redis.")
        except redis.exceptions.LockError as e:
            raise RuntimeError(f"Failed to acquire lock for file writer '{self.filename}': {e}")


    def _write_to_file(self, message_data):
        """
        Writes the pre-formatted message directly to the file using the file handler.
        """
        try:
            # Decode the message data (which is stored in byte strings)
            log_record = {k.decode('utf-8'): v.decode('utf-8') for k, v in message_data.items()}

            # Fetch the pre-formatted message and write it directly to the file
            formatted_message = log_record.get("formatted_message", "")

            # Write the formatted message directly to the file (manually emitting a log)
            self.file_handler.stream.write(f"{formatted_message}\n")
            self.file_handler.flush()  # Ensure that it's flushed to disk
            # print(f"_WRITE_TO_FILE::[{formatted_message}]")
        except Exception as e:
            print(f"Failed to write log record to file: {e}")


    def _listen(self):
        """
        Listens for messages on the Redis stream and writes them to the file.
        Stops when the `stop_event` is set.
        """
        # print(f"AsyncRedisFileWriter listening to stream '{self.stream_name}'")
        while self.running and not self.stop_event.is_set():
            try:
                response = self.redis_client.xread({self.stream_name: self.last_id}, block=self.block)

                # if not self.running:
                if not self.running or self.stop_event.is_set():
                    break

                for stream, messages in response:
                    for message_id, message_data in messages:
                        self.last_id = message_id
                        self._write_to_file(message_data)
            except Exception as e:
                print(f"Unexpected error in AsyncRedisFileWriter listener: {e}")
                if not self.running or self.stop_event.is_set():
                    break

        self.close()
        print(f"Writer {self.__class__} stopped for '{self.filename}'.")

    def close(self):
        """
        Gracefully closes the writer, stops the Redis client, file handler, and performs registry cleanup.
        """
        if self.init_closed:
            return

        self.init_closed = True
        self.running = False

        print(f"Draining the remaining logs from Redis stream before shutdown")
        self._drain_logs()

        # Close the file handler
        if self.file_handler:
            try:
                self.file_handler.close()
                # print(f"File handler for '{self.filename}' has been closed.")
            except Exception as e:
                print(f"Failed to close file handler for '{self.filename}': {e}")

        # Perform the registry cleanup
        self._cleanup_registry()

        # Close Redis client
        if self.redis_client:
            try:
                self.redis_client.close()
                # print(f"Redis client for '{self.filename}' has been closed.")
            except Exception as e:
                print(f"Error while closing Redis client for '{self.filename}': {e}")

    def _drain_logs(self):
        """
        Drains the remaining logs from the Redis stream to ensure no data is lost before stopping.
        """
        while not self.stop_event.is_set():
            try:
                response = self.redis_client.xread({self.stream_name: self.last_id}, block=self.block)
                if not response:
                    print(f"No more logs in Redis stream '{self.stream_name}' to drain.")
                    break

                for stream, messages in response:
                    for message_id, message_data in messages:
                        self.last_id = message_id
                        self._write_to_file(message_data)

            except Exception as e:
                print(f"Error while draining logs for '{self.filename}': {e}")
                break

    def _cleanup_registry(self):
        """
        Cleans up the Redis registry for the file writer.
        Retries if the lock cannot be acquired immediately.
        """
        registry_key = f"file_writer_registry:{self.filename}"
        lock_key = f"file_writer_lock:{self.filename}"

        # print(f"_cleanup_registry for::[{registry_key}]")

        retries = 5  # Maximum number of retries
        delay = 1  # Initial delay between retries

        while retries > 0:
            try:
                with self.redis_client.lock(lock_key, timeout=self.lock_timeout):
                    # print(f"_cleanup_registry for::[got client lock]")

                    # Check if the registry key exists before trying to delete it
                    if self.redis_client.exists(registry_key):
                        # print(f"Registry key '{registry_key}' exists, attempting to delete.")
                        delete_response = self.redis_client.delete(registry_key)
                        # print(f"delete response::[{delete_response}]")

                        if delete_response == 1:
                            print(f"Successfully deleted registry key: '{registry_key}'")
                        else:
                            print(f"Failed to delete registry key: '{registry_key}', it may not exist.")
                    else:
                        print(f"Registry key '{registry_key}' does not exist, nothing to delete.")
                    break  # Exit the retry loop after successful cleanup

            except redis.exceptions.LockError:
                retries -= 1
                print(f"Failed to acquire lock for '{self.filename}', retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            except Exception as e:
                print(f"Unexpected error while cleaning up registry for '{self.filename}': {e}")
                break

        if retries == 0:
            print(f"Failed to clean up registry for '{self.filename}' after multiple attempts.")

