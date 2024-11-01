import os
import shutil

import polars as pl

from crypto_data_benchmark.dbs.common import (
    ROOT_DATA_DIR,
    disk_usage,
)


class ParquetProvider:
    def setup(self) -> None:
        """Create an empty Parquet file if it doesn't exist"""
        self.data_dir = ROOT_DATA_DIR / "parquet"
        self.file_path = self.data_dir / "data.parquet"

        self.teardown()

        # create a fresh data directory
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def teardown(self) -> None:
        """Delete the data folder if it exists"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def add(self, data: pl.DataFrame) -> None:
        """Add new data to the existing dataset"""
        data.write_parquet(self.file_path)

    def disk_usage(self) -> str:
        """Return the size of the data folder"""
        return disk_usage(self.data_dir)


if __name__ == "__main__":
    from crypto_data_benchmark.data.usdt_transfers import usdt_transfers

    db = ParquetProvider()
    db.setup()
    data = usdt_transfers(scale=500)
    db.add(data)
    print(db.disk_usage())
    db.teardown()
