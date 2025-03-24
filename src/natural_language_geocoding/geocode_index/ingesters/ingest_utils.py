from collections.abc import Generator, Iterator
from logging import Logger
from math import ceil
from time import time


def counting_generator[T](
    items: Iterator[T], *, logger: Logger, log_after_secs: int = 10
) -> Generator[T, None, None]:
    """Logs the rate at which items are being pulled from the source iterator."""
    start_time = time()
    last_logged = time()
    count = 0

    def _log() -> None:
        now = time()
        elapsed = now - start_time
        rate_per_sec = count / elapsed
        rate_per_min = rate_per_sec * 60
        logger.info(
            "Processed %s items. Rate: %s per min. Elapsed time: %s mins",
            count,
            ceil(rate_per_min),
            ceil(elapsed / 60),
        )

    for item in items:
        yield item
        count += 1
        if time() - last_logged >= log_after_secs:
            _log()
            last_logged = time()

    _log()
