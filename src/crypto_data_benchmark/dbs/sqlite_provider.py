import os
import shutil
import sqlite3

import polars as pl

from crypto_data_benchmark.dbs.common import (
    ROOT_DATA_DIR,
    disk_usage,
)


class SQLiteProvider:
    def setup(self) -> None:
        """Establish connection to the database and create the table"""
        self.data_dir = ROOT_DATA_DIR / "sqlite"
        self.db_path = self.data_dir / "data.db"
        self.connection_string = f"sqlite:///{self.db_path}"

        self.teardown()

        # Create a fresh data directory
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def teardown(self) -> None:
        """Delete the data folder if it exists"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def add(self, data: pl.DataFrame) -> None:
        """Add new data to the existing dataset"""
        data.write_database(
            "transfers", connection=self.connection_string, if_table_exists="append"
        )

    def disk_usage(self) -> str:
        """Return the size of the data folder"""
        return disk_usage(self.data_dir)

    def read_all(self) -> pl.DataFrame:
        """Read all data from the database"""
        return pl.read_database(
            query="SELECT * FROM transfers", connection=self.connection_string
        )


if __name__ == "__main__":
    from crypto_data_benchmark.data.usdt_transfers import usdt_transfers

    db = SQLiteProvider()
    db.setup()
    data = usdt_transfers(scale=500)
    db.add(data)
    print(f"Storage used: {db.disk_usage()}")

    # Read and print the first few rows to verify
    result = db.read_all()
    print(result.head())

    db.teardown()
