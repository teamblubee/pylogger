# AsyncBufferedRedisHandler: An Advanced Python Logger Using Redis

The **AsyncBufferedRedisHandler** is a Python logging handler that leverages Redis to manage logs in a scalable and reliable way. This tool is ideal for scenarios where multiple processes on the same machine need to write to log files reliably, avoiding memory issues and preventing the creation of multiple redundant log files, since there's no buffering or retry logic outside of directly pushing to Redis. Below is a detailed description of its key features, design, and how it works.

### Overview
The AsyncBufferedRedisHandler provides a unique solution for logging in Python applications by using Redis as a buffer. This handler leverages Python's default logging while avoiding some common pitfalls, such as buffering issues, writing directly to output devices like the terminal (TTY), and ensuring logs are captured and transmitted reliably without relying on complex frameworks, using only Redis.

### Key Features

- **Redis-based Buffering**: Logs are first queued in memory and then written to a Redis stream, allowing for reliable buffering and retry if direct log transmission fails.
- **Asynchronous Writing**: The handler uses worker threads and dedicated processes to handle log records asynchronously, reducing the performance overhead on the main application thread.
- **Multiple Writers**: The logger supports writing logs to multiple outputs, including TTY (for on-screen logs) and files, managed by separate asynchronous processes.
- **Automatic TTY Detection**: It detects the TTY associated with the process and writes logs directly for better monitoring and debugging.
- **Process-Safe**: Separate processes handle log writing, ensuring safe operations even when the logger is used in multi-process environments.
- **Signal Handling for Cleanup**: Proper signal handling is implemented to ensure graceful shutdown and cleanup, including stopping all worker threads and deleting the Redis stream if no consumers are active.
- **Retry Mechanism**: For remote Redis locations, buffering and retry mechanisms are provided to handle intermittent connection issues.

### How It Works
The **AsyncBufferedRedisHandler** is designed to be a plug-and-play replacement for traditional logging handlers. When initialized, it creates a queue that temporarily stores log records. A worker thread processes these records and sends them to a Redis stream, which provides durability and potential multi-consumer support.

The handler also supports additional asynchronous writers:
- **TTY Writer**: Uses `AsyncRedisTTYWriter` to output logs to a terminal (TTY), allowing you to monitor log messages in real time.
- **File Writer**: Uses `AsyncRedisFileWriter` to log messages to a specified file, supporting features like log rotation (max bytes and backup count).

### Usage Example
To use the AsyncBufferedRedisHandler in your Python application, you can instantiate it and add it to your logging configuration as follows:

```python
import logging
from AsyncBufferedRedisHandler import AsyncBufferedRedisHandler

logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)

redis_handler = AsyncBufferedRedisHandler(
    redis_host='localhost',
    redis_port=6379,
    stream_name='my_log_stream',
    buffer_size=10,
    flush_interval=0.05,
    start_tty_writer=True,
    filename='/var/log/my_app.log',
    max_bytes=10*1024*1024,
    backup_count=5
)

logger.addHandler(redis_handler)
logger.info('This is a test log message.')
```

### Summary
The **AsyncBufferedRedisHandler** is a sophisticated logging solution for Python that integrates with Redis to offer asynchronous, reliable logging capabilities. It is ideal for distributed applications that need resilient and scalable logging, while still supporting features like real-time console output and file-based log storage.

By combining multi-threading, Redis-based buffering, and dedicated processes for writing logs, this handler provides a comprehensive approach to address the shortcomings of traditional Python logging mechanisms.

### Feature Suggestions
If you find any issues, you can make suggestions.
