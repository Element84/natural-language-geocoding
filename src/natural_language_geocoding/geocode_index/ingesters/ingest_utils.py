import threading
from collections.abc import Callable, Generator, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from logging import Logger
from math import ceil
from time import time

from e84_geoai_common.util import chunk_items

from natural_language_geocoding.geocode_index.index import GeocodeIndex


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


def filter_items[T](
    items: Iterator[T], filter_fn: Callable[[T], bool], *, logger: Logger | None = None
) -> Generator[T, None, None]:
    """TODO docs."""
    for item in items:
        if filter_fn(item):
            yield item
        elif logger:
            logger.info("Filtered out %s", item)


def process_ingest_items[T](
    items: Iterable[T],
    index_item: Callable[[GeocodeIndex, list[T]], None],
    *,
    max_workers: int = 10,
    max_inflight: int = 20,
    chunk_size: int = 25,
) -> None:
    """TODO docs."""
    thread_local = threading.local()
    all_conns: set[GeocodeIndex] = set()

    def _get_index() -> GeocodeIndex:
        if not hasattr(thread_local, "index"):
            thread_local.index = GeocodeIndex()
            all_conns.add(thread_local.index)

        return thread_local.index

    def _bulk_index(items_in_chunk: list[T]) -> None:
        index = _get_index()
        index_item(index, items_in_chunk)

    with ThreadPoolExecutor(max_workers=max_workers) as e:
        # The maximum number of concurrent future to queue before waiting.
        futures: list[Future[None]] = []

        for features in chunk_items(items, chunk_size):
            futures.append(e.submit(_bulk_index, features))

            # We only append until the maximum number of futures is reached to avoid OOM errors
            if len(futures) >= max_inflight:
                # Wait for at least one to complete
                done_futures, not_done_futures = wait(futures, return_when=FIRST_COMPLETED)
                # Process results from completed futures
                for future in done_futures:
                    future.result()
                # Save the set of not done futures as the set we're still waiting on
                futures = list(not_done_futures)

        # Wait for any remaining futures
        for future in as_completed(futures):
            future.result()
        for conn in all_conns:
            conn.client.close()
