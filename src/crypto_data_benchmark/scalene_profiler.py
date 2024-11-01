import time

from scalene import scalene_profiler


def profile(func, *args, **kwargs):
    # Start Scalene profiling
    scalene_profiler.Scalene._Scalene__stats.clear_all()
    scalene_profiler.start()

    start_memory = scalene_profiler.Scalene._Scalene__stats.current_footprint
    start_time = time.time()

    try:
        # Run the function
        func(*args, **kwargs)
    finally:
        # Ensure Scalene is stopped even if an exception occurs
        scalene_profiler.stop()
        end_memory = scalene_profiler.Scalene._Scalene__stats.current_footprint

    peak_memory = scalene_profiler.Scalene._Scalene__stats.max_footprint
    end_time = time.time()

    elapsed_time = end_time - start_time

    metrics = {
        "peak_memory": peak_memory * 1024 * 1024,
        "memory_delta": (end_memory - start_memory) * 1024 * 1024,
        "ingestion_time": elapsed_time,
    }

    return metrics
