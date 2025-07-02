import threading
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from functools import singledispatch
from itertools import batched
from logging import Logger
from math import ceil
from time import time

from shapely import (
    GEOSException,
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
    """Filters out items that don't match the filter_fn."""
    for item in items:
        if filter_fn(item):
            yield item
        elif logger:
            logger.info("Filtered out %s", item)


# Used for removing repeated points so that shapely and opensearch will consider them valid.
_DUPLICATE_POINT_TOLERANCE = 0.00001


def _is_tiny_linear_ring_error(e: GEOSException) -> bool:
    msg = str(e.args[0])
    # An error that can occur if the ring has only/mostly duplicate points.
    return msg.startswith("IllegalArgumentException: Invalid number of points in LinearRing")


@singledispatch
def remove_duplicate_points[T_Geom: BaseGeometry](geom: T_Geom, tolerance: float) -> T_Geom:
    """Removes the duplicate points switching based on type."""
    return remove_repeated_points(geom, tolerance)


@remove_duplicate_points.register
def _(geom: Polygon, tolerance: float) -> Polygon:
    # This will throw a tiny linear ring error if it's really small. We'll handle that a higher
    # level.
    fixed_exterior: LinearRing = remove_duplicate_points(geom.exterior, tolerance)

    fixed_interiors: list[LinearRing] = []

    for interior in geom.interiors:
        try:
            fixed_interiors.append(remove_duplicate_points(interior, tolerance))
        except GEOSException as e:
            # An error that can occur if the ring has only/mostly duplicate points.
            # We'll just drop the interior in that case as it would effectively be a tiny hole.
            if not _is_tiny_linear_ring_error(e):
                raise

    return Polygon(shell=fixed_exterior, holes=fixed_interiors)


@remove_duplicate_points.register
def _(geom: MultiPolygon, tolerance: float) -> MultiPolygon:
    polygons: list[Polygon] = []

    for poly in geom.geoms:
        try:
            polygons.append(remove_duplicate_points(poly, tolerance))
        except GEOSException as e:
            # Indicates that the exterior of one of the polygons was a tiny area. Drop the polygon.
            if not _is_tiny_linear_ring_error(e):
                raise
    return MultiPolygon(polygons)


# FUTURE add unit tests for this function.
def fix_geometry(feature_id: str, orig_geom: BaseGeometry) -> BaseGeometry:
    """Attempts to fix the geometry if it's invalid.

    This uses a variety of approaches like remove duplicate points or adding a 0 length buffer.
    Raises an exception if it can't fix a geometry.
    """
    try:
        # Remove explicity duplicated points. This is valid for Shapely but not for opensearch
        result_geom = remove_duplicate_points(orig_geom, 0)

        if not result_geom.is_valid:
            # Sometimes geometry points are too close together and considered duplicates
            result_geom = remove_duplicate_points(result_geom, _DUPLICATE_POINT_TOLERANCE)

            if not result_geom.is_valid:
                # One last approach is to create a buffer of 0 distance from an object. This can fix
                # some invalid geometry
                geom_with_buffer = result_geom.buffer(0)
                # We must remove any new duplicates this might add
                geom_with_buffer_and_no_dups = remove_duplicate_points(
                    result_geom, _DUPLICATE_POINT_TOLERANCE
                )

                if geom_with_buffer_and_no_dups.is_valid:
                    # We only use this last one if it's valid. Sometimes buffering will fix the
                    # problem but then removing duplicates will cause a different problem. We still
                    # remove the remaining duplicates if it's not invalid because this can fix
                    # opensearch issues that shapely doesn't catch.
                    result_geom = geom_with_buffer_and_no_dups
                else:
                    result_geom = geom_with_buffer
    except Exception as e:
        # Shapely might throw an error and if it does we want to include the feature id.
        raise Exception(f"Error while fixing feature {feature_id}") from e
    else:
        if not result_geom.is_valid:
            # If it's still not valid or wasn't fixed raise an error
            reason = explain_validity(result_geom)
            raise ValueError(f"Geometry for feature {feature_id} is not valid due to {reason}")

        return result_geom


def process_ingest_items[T](
    items: Iterable[T],
    index_items_fn: Callable[[GeocodeIndex, Sequence[T]], None],
    *,
    max_workers: int = 10,
    max_inflight: int = 20,
    chunk_size: int = 25,
) -> None:
    """Ingests the items in parallel using the index_items_fn."""
    thread_local = threading.local()
    all_conns: set[GeocodeIndex] = set()

    def _get_index() -> GeocodeIndex:
        if not hasattr(thread_local, "index"):
            thread_local.index = GeocodeIndex()
            all_conns.add(thread_local.index)

        return thread_local.index

    def _bulk_index(items_in_chunk: Sequence[T]) -> None:
        index = _get_index()
        index_items_fn(index, items_in_chunk)

    with ThreadPoolExecutor(max_workers=max_workers) as e:
        futures: list[Future[None]] = []

        for features in batched(items, chunk_size):
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
