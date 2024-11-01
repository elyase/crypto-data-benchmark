import shutil

import lancedb
import polars as pl

from crypto_data_benchmark.dbs.common import (
    ROOT_DATA_DIR,
    disk_usage,
)


class LanceDBProvider:
    def setup(self) -> None:
        """Create a LanceDB connection and table"""
        self.data_dir = ROOT_DATA_DIR / "lancedb"
        self.table_name = "benchmarks"

        self.teardown()

        # Create a fresh data directory
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Connect to LanceDB
        self.db = lancedb.connect(str(self.data_dir))

    def teardown(self) -> None:
        """Delete the data folder if it exists"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def add(self, data: pl.DataFrame) -> None:
        """Add new data to the existing dataset"""
        # Convert Polars DataFrame to PyArrow Table
        pa_table = data.to_arrow()

        # Create or append to the table
        if self.table_name not in self.db.table_names():
            self.db.create_table(self.table_name, pa_table)
        else:
            table = self.db[self.table_name]
            table.add(pa_table)

    def disk_usage(self) -> str:
        """Return the size of the data folder"""
        return disk_usage(self.data_dir)
