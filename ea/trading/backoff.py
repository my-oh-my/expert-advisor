import time

from ea.misc.logger import logger


def retry(exception, retries=3, backoff_in_seconds=2):
    def decorator(f):
        def wrapper(*args, **kwargs):
            attempt = 1
            while True:
                logger.info(f'Attempting to open order, attempt no: {attempt}')
                try:
                    return f(*args, **kwargs)
                except exception:
                    if attempt == retries:
                        raise

                    sleep = backoff_in_seconds * 2 ** attempt
                    time.sleep(sleep)
                    attempt += 1

        return wrapper

    return decorator
