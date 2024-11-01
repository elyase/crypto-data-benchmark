import numpy as np
import polars as pl
import pyarrow as pa

from crypto_data_benchmark.data.usdt_transfers import usdt_transfers
from crypto_data_benchmark.scalene_profiler import profile


def allocate_500mb():
    # Allocate approximately 500 MB
    chunk_size = 1024 * 1024  # 1 MB
    num_chunks = 500
    memory_chunks = []
    for _ in range(num_chunks):
        memory_chunks.append(bytearray(chunk_size))
    return sum(len(chunk) for chunk in memory_chunks)


def allocate_and_free_1gb():
    # Allocate approximately 1 GB, then free half of it
    chunk_size = 1024 * 1024  # 1 MB
    num_chunks = 1024
    memory_chunks = []
    for _ in range(num_chunks):
        memory_chunks.append(bytearray(chunk_size))

    # Free half of the allocated memory
    for _ in range(num_chunks // 2):
        memory_chunks.pop()

    return sum(len(chunk) for chunk in memory_chunks)


def pyarrow_dataset():
    num_rows = 10_000_000
    num_columns = 5

    # Generate data with a known size (8 bytes per float64)
    data_size_bytes = num_rows * num_columns * 8
    expected_size_mb = data_size_bytes / (1024 * 1024)

    # Generate data for each column
    data = {
        f"col_{i}": np.full(num_rows, i, dtype=np.float64) for i in range(num_columns)
    }

    # Create a PyArrow Table
    table = pa.Table.from_pydict(data)
    actual_size_mb = table.nbytes / (1024 * 1024)


def polars_dataset(scale: int):
    data = usdt_transfers(scale=scale)
    num_rows = 10_000_000  # 1 million rows

    # Generate random data for multiple columns
    data = {
        "id": np.arange(num_rows),
        "value": np.random.rand(num_rows),
        "category": np.random.choice(["A", "B", "C"], size=num_rows),
        "timestamp": np.arange(
            np.datetime64("2023-01-01"),
            np.datetime64("2023-01-01") + np.timedelta64(num_rows, "s"),
            np.timedelta64(1, "s"),
        ),
    }

    # Create the DataFrame
    df = pl.DataFrame(data)
    print(df.estimated_size("mb"))
    # concatenate the dataframes
    return df.estimated_size("mb")


def test_scalene_profiler():
    print("\nTest 2: Allocate 1 GB and free 500 MB")
    metrics = profile(allocate_and_free_1gb)
    print(f"Function metrics: {metrics}")

    print("Test 1: Allocate 500 MB")
    metrics = profile(allocate_500mb)
    print(f"Function metrics: {metrics}")

    print("\nTest 3: PyArrow Dataset")
    metrics = profile(pyarrow_dataset)
    print(f"Function metrics: {metrics}")

    print("\nTest 4: Polars Dataset")
    metrics = profile(polars_dataset, scale=1500)
    print(f"Function metrics: {metrics}")


if __name__ == "__main__":
    test_scalene_profiler()
