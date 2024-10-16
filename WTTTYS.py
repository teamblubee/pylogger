import fcntl
import os
import time
from typing import Optional

def write_to_tty_safely(
    message: Optional[str] = None, 
    tty_path: Optional[str] = None, 
    retry_attempts: int = 30, 
    retry_delay: float = 0.1
) -> bool:
    """
    Writes a message to the TTY in a multiprocessing and multi-program safe way using file locking.
    
    Args:
        message (Optional[str]): The message to write to the TTY. If None, the function will return without doing anything.
        tty_path (Optional[str]): The path to the TTY device. If None, the function will return without doing anything.
        retry_attempts (int): Number of times to retry if the lock is unavailable. Default is 30.
        retry_delay (float): Time (in seconds) to wait between retries. Default is 0.1 seconds.
    
    Returns:
        bool: True if the message was successfully written, False otherwise.
    """
    # Return immediately if either the message or tty_path is None
    if message is None or tty_path is None:
        return False

    attempts = 0
    
    while attempts < retry_attempts:
        try:
            # Open the TTY for writing
            with open(tty_path, 'w') as tty:
                # Try to acquire an exclusive lock on the TTY
                fcntl.flock(tty, fcntl.LOCK_EX)
                
                # Write the message to TTY
                tty.write(message + '\n')
                tty.flush()  # Ensure immediate writing
                
                # Release the lock
                fcntl.flock(tty, fcntl.LOCK_UN)
                
            return True  # Successfully wrote to the TTY
        
        except (OSError, IOError):
            # Retry on any file-related errors (e.g., lock contention or I/O issue)
            attempts += 1
            time.sleep(retry_delay)
        
        except:
            # Silently handle unexpected exceptions (do not print or log)
            break
    
    return False  # Failed after retry attempts
