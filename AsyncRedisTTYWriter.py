import os
import threading
import orjson
import logging
import redis as redis
from multiprocessing import Queue

class AsyncRedisTTYWriter:
    """
    A writer that subscribes to a Redis stream and writes log records to TTY.
    """
    def __init__(
            self,
            redis_host='localhost',
            redis_port=6379,
            redis_db=0,
            stream_name=None,
            last_id='0-0',
            stop_event=None,
            block=None
            ):
        """
        Initializes the AsyncRedisTTYWriter.

        Args:
            redis_host (str): Redis server host.
            redis_port (int): Redis server port.
            redis_db (int): Redis database number.
            stream_name (str): Redis stream to subscribe to.
        """
        self.stop_event = stop_event
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.stream_name = stream_name
        self.redis_client = None
        self.running = False
        self.last_id = last_id
        self.block = block

        if not self.stream_name:
            print("No stream name provided or stream name is None. Exiting.")
            return

        self.redis_client = redis.Redis(
            host=self.redis_host, port=self.redis_port, db=self.redis_db
        )
        self.running = True

        # print(f"AsyncRedisTTYWriter start '{self.stream_name}'")
        self.listener_thread = threading.Thread(target=self._listen)
        self.listener_thread.start()


    def _listen(self):
        """
        Listens for messages on the Redis stream and writes them to the TTY.
        """
        # print(f"AsyncRedisTTYWriter _listen started for stream '{self.stream_name}'")
        while self.running and not self.stop_event.is_set():
            try:
                # Check if the redis_client is None (which could happen if it's closed during shutdown)
                if self.redis_client is None:
                    print("Redis client is None, stopping listener.")
                    break

                # Read from the Redis stream, blocking for up to 'block' milliseconds
                response = self.redis_client.xread(
                    {self.stream_name: self.last_id}, block=self.block
                )

                if not self.running:
                    # Ensure we exit immediately if stop() has been called during the block period
                    break

                try:
                    # Process the messages received from the stream
                    for stream, messages in response:
                        for message_id, message_data in messages:
                            self.last_id = message_id  # Update last_id to continue from here

                            # Output the raw message data directly to TTY
                            message_data_decoded = {k.decode('utf-8'): v.decode('utf-8') if isinstance(v, bytes) else v for k, v in message_data.items()}

                            # print(f"""
                            # AsyncRedisTTYWriter
                            # ---------------------------
                            # Message ID: {message_id}
                            # Data:       {message_data}
                            # Type:       {type(message_data)}
                            # message:    {type(message_data.get(b'formatted_message'))}
                            # decoded:    {message_data_decoded}
                            # """)
                            print(message_data_decoded['formatted_message'])
                except Exception as e:
                    print(f"RESPONSE ERROR::[{str(e)}]")

            except ValueError as e:
                if "I/O operation on closed file" in str(e):
                    # Handle the specific case of a closed file
                    print(f"AsyncRedisTTYWriter listener stopped due to closed resource: {e}")
                    break
            except AttributeError as e:
                if "'NoneType' object has no attribute" in str(e):
                    # Handle case where an attribute is accessed on a None object
                    print(f"AsyncRedisTTYWriter listener stopped due to an AttributeError (resource was None): {e}")
                    break
            except redis.exceptions.ConnectionError as e:
                # Handle connection errors gracefully
                if self.running:
                    print(f"Error in AsyncRedisTTYWriter listener due to connection issue: {e}")
                else:
                    break  # Exit loop if not running anymore
            except Exception as e:
                # Handle other unexpected errors
                print(f"Unexpected error in AsyncRedisTTYWriter listener: {e}")
                if not self.running:
                    break

        self.close()
        print(f"Writer {self.__class__} stopped.")


    def close(self):
        """
        Stops the writer by closing the Redis client and gracefully shutting down threads.
        """
        if not self.running:
            return
        self.running = False

        # Ensure that we do not use the Redis client after it has been closed
        if self.redis_client:
            try:
                self.redis_client.close()
            except Exception as e:
                print(f"Error while closing Redis client: {e}")

        print("AsyncRedisTTYWriter has been stopped.")
