"""
TileDB provider (WIP)
"""

import shutil

import numpy as np
import tiledb

from crypto_data_benchmark.dbs.common import (
    ROOT_DATA_DIR,
    disk_usage,
)


class SimpleTileDBProvider:
    def __init__(self):
        self.data_dir = ROOT_DATA_DIR / "tiledb_simple"
        self.path = self.data_dir / "data"

    def setup(self) -> None:
        """Create an empty TileDB array if it doesn't exist"""
        self.teardown()
        # Ensure the data directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._create_array()

    def teardown(self) -> None:
        """Delete the data folder if it exists"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def add(self, block_numbers: np.ndarray, log_indices: np.ndarray) -> None:
        """Add new data to the existing dataset"""
        # Prepare the data dictionary for TileDB
        data_dict = {
            "log_index": log_indices.astype(np.uint64),
        }

        # Write the data to the TileDB array
        with tiledb.SparseArray(str(self.path), mode="w") as A:
            A[block_numbers] = data_dict

    def read(self) -> dict:
        """Read data from the TileDB array"""
        with tiledb.SparseArray(str(self.path), mode="r") as A:
            data = A[:]
            return {
                "block_number": data["block_number"],
                "log_index": data["log_index"],
            }

    def disk_usage(self) -> str:
        """Return the size of the data folder"""
        return disk_usage(self.data_dir)

    def _create_array(self):
        block_number = tiledb.Dim(
            name="block_number", domain=(0, 2**63 - 1), tile=10_000_000, dtype=np.uint64
        )
        domain = tiledb.Domain(block_number)

        attributes = [
            tiledb.Attr(name="log_index", dtype=np.uint64),
        ]

        schema = tiledb.ArraySchema(domain=domain, sparse=True, attrs=attributes)
        tiledb.Array.create(str(self.path), schema)


if __name__ == "__main__":
    # Generate some simple test data
    block_numbers = np.array([1, 2, 3, 4, 5], dtype=np.uint64)
    log_indices = np.array([10, 20, 30, 40, 50], dtype=np.uint64)

    db = SimpleTileDBProvider()
    db.setup()
    db.add(block_numbers, log_indices)

    # Read the data
    read_data = db.read()

    # Verify the data
    is_correct = np.array_equal(
        read_data["block_number"], block_numbers
    ) and np.array_equal(read_data["log_index"], log_indices)

    if is_correct:
        print("Data verification successful!")
    else:
        print("Data verification failed!")

    print(db.disk_usage())
    db.teardown()
