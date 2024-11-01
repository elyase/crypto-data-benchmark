import shutil

import duckdb
import polars as pl

from crypto_data_benchmark.dbs.common import (
    ROOT_DATA_DIR,
    disk_usage,
)


class DuckDBProvider:
    def setup(self) -> None:
        """Establish connection to the database and create the table"""
        self.data_dir = ROOT_DATA_DIR / "duckdb"
        self.db_path = self.data_dir / "data.db"

        self.teardown()

        # Create a fresh data directory
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Create a new DuckDB database
        self.conn = duckdb.connect(str(self.db_path))

    def teardown(self) -> None:
        """Delete the data folder if it exists"""
        if hasattr(self, "conn"):
            self.conn.close()
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def add(self, data: pl.DataFrame) -> None:
        """Add new data to the existing dataset"""
        self.conn.execute("CREATE TABLE transfers AS SELECT * FROM data")
        # self.conn.execute("INSERT INTO transfers SELECT * FROM data")

    def disk_usage(self) -> str:
        """Return the size of the data folder"""
        return disk_usage(self.data_dir)


if __name__ == "__main__":
    from crypto_data_benchmark.data.usdt_transfers import usdt_transfers

    db = DuckDBProvider()
    db.setup()
    data = usdt_transfers(scale=100)

    db.add(data)
    print(f"Storage used: {db.disk_usage()}")
    db.teardown()
