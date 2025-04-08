import threading
from collections.abc import Callable, Generator, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from functools import singledispatch
from logging import Logger
from math import ceil
from time import time
from typing import TypeVar

from e84_geoai_common.util import chunk_items
from shapely import (
    LinearRing,
    MultiPolygon,
    Polygon,
    remove_repeated_points,  # type: ignore[reportUnknownVariableType]
)
from shapely.geometry.base import BaseGeometry
from shapely.validation import explain_validity

from natural_language_geocoding.geocode_index.index import GeocodeIndex


def counting_generator[T](
    items: Iterator[T], *, logger: Logger, log_after_secs: int = 10
) -> Generator[T, None, None]:
    """Logs the rate at which items are being pulled from the source iterator."""
    start_time = time()
    last_logged = time()
    count = 0
    last_logged_count = 0

    def _log() -> None:
        now = time()
        total_elapsed = now - start_time
        total_rate_per_sec = count / total_elapsed
        rate_per_min = total_rate_per_sec * 60

        since_last_log_elapsed = now - last_logged
        rolling_rate_per_sec = (count - last_logged_count) / since_last_log_elapsed
        rolling_rate_per_min = rolling_rate_per_sec * 60

        logger.info(
            (
                "Processed %s items. Rolling Rate: %s per min. Total Rate: %s per min."
                " Elapsed time: %s mins"
            ),
            count,
            ceil(rolling_rate_per_min),
            ceil(rate_per_min),
            ceil(total_elapsed / 60),
        )

    for item in items:
        yield item
        count += 1
        if time() - last_logged >= log_after_secs:
            _log()
            last_logged_count = count
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


# Used for removing repeated points so that shapely and opensearch will consider them valid.
_DUPLICATE_POINT_TOLERANCE = 0.00001

T_Geom = TypeVar("T_Geom", bound=BaseGeometry)


@singledispatch
def _remove_duplicate_points(geom: T_Geom) -> T_Geom:
    """Removes the duplicate points switching based on type."""
    return remove_repeated_points(geom, _DUPLICATE_POINT_TOLERANCE)


@_remove_duplicate_points.register
def _(geom: Polygon) -> Polygon:
    # The shapely function doesn't seem to remove duplicates from interiors
    fixed_exterior: LinearRing = _remove_duplicate_points(geom.exterior)
    fixed_interiors: list[LinearRing] = [_remove_duplicate_points(i) for i in geom.interiors]

    return Polygon(shell=fixed_exterior, holes=fixed_interiors)


@_remove_duplicate_points.register
def _(geom: MultiPolygon) -> MultiPolygon:
    return MultiPolygon([_remove_duplicate_points(geom) for geom in geom.geoms])


def fix_geometry(feature_id: str, orig_geom: BaseGeometry) -> BaseGeometry:
    """Attempts to fix the geometry if it's invalid.

    This uses a variety of approaches like remove duplicate points or adding a 0 length buffer.
    Raises an exception if it can't fix a geometry.
    """
    # Remove explicity duplicated points. This is valid for Shapely but not for opensearch
    result_geom = remove_repeated_points(orig_geom)

    if not result_geom.is_valid:
        # Sometimes geometry points are too close together and considered duplicates
        result_geom = _remove_duplicate_points(result_geom)

        if not result_geom.is_valid:
            # One last approach is to create a buffer of 0 distance from an object. This can fix
            # some invalid geometry
            geom_with_buffer = result_geom.buffer(0)
            # We must remove any new duplicates this might add
            geom_with_buffer_and_no_dups = _remove_duplicate_points(result_geom)

            if geom_with_buffer_and_no_dups.is_valid:
                # We only use this last one if it's valid. Sometimes buffering will fix the problem
                # but then removing duplicates will cause a different problem. We still remove the
                # remaining duplicates if it's not invalid because this can fix opensearch issues
                # that shapely doesn't catch.
                result_geom = geom_with_buffer_and_no_dups
            else:
                result_geom = geom_with_buffer

    if not result_geom.is_valid:
        # If it's still not valid or wasn't fixed raise an error
        reason = explain_validity(result_geom)
        raise ValueError(f"Geometry for feature {feature_id} is not valid due to {reason}")

    return result_geom


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
