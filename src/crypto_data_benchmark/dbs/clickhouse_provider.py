import shutil
from pathlib import Path

import polars as pl
from chdb.session import Session

from crypto_data_benchmark.dbs.common import (
    ROOT_DATA_DIR,
    disk_usage,
)


class ClickHouseProvider:
    def setup(self) -> None:
        """Create a fresh ClickHouse database"""
        self.data_dir = ROOT_DATA_DIR / "clickhouse"
        self.db_path = self.data_dir / "db.chdb"

        self.teardown()

        # Create fresh data directory
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Initialize session
        self.session = Session(str(self.db_path))
        self.session.query("CREATE DATABASE IF NOT EXISTS benchmarks ENGINE = Atomic")

    def teardown(self) -> None:
        """Delete the data folder if it exists"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def add(self, data: pl.DataFrame) -> None:
        """Add new data to the existing dataset"""
        # Convert Polars DataFrame to Arrow table
        arrow_table = data.to_arrow()

        # Create view and table using type inference
        self.session.query("""
            CREATE VIEW benchmarks.view
            AS SELECT * FROM Python(arrow_table)
        """)

        self.session.query("""
            CREATE TABLE IF NOT EXISTS benchmarks.transfers ENGINE = Log AS benchmarks.view
        """)

        # Insert data from view into table
        self.session.query("""
            INSERT INTO benchmarks.transfers 
            SELECT * FROM benchmarks.view
            FORMAT Native
        """)

    def disk_usage(self) -> str:
        """Return the size of the data folder"""
        return disk_usage(self.data_dir)


if __name__ == "__main__":
    from crypto_data_benchmark.data.usdt_transfers import usdt_transfers

    db = ClickHouseProvider()
    db.setup()
    data = usdt_transfers(scale=1, hex=True)
    db.add(data)
    print(db.disk_usage())
    db.teardown()
