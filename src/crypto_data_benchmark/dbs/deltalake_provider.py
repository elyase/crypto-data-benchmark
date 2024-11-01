# ... existing imports ...
import shutil
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from crypto_data_benchmark.dbs.common import (
    ROOT_DATA_DIR,
    disk_usage,
)


class DeltaLakeProvider:
    def setup(self) -> None:
        """Initialize Delta Lake table"""

        self.data_dir = ROOT_DATA_DIR / "deltalake"
        self.teardown()

    def teardown(self) -> None:
        """Delete the Delta Lake table"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def add(self, data: pd.DataFrame) -> None:
        """Add new data to the Delta Lake table"""
        write_deltalake(str(self.data_dir), data.to_arrow(), mode="overwrite")

    def disk_usage(self) -> str:
        """Return the size of the Delta Lake table"""
        return disk_usage(self.data_dir)


# ... existing code ...

if __name__ == "__main__":
    from crypto_data_benchmark.data.usdt_transfers import usdt_transfers

    db = DeltaLakeProvider()
    db.setup()
    data = usdt_transfers(scale=100)
    columns = [
        "removed",
        "log_index",
        "transaction_index",
        "transaction_hash",
        "block_hash",
        "block_number",
        "address",
        "data",
        "topic0",
        "topic1",
        "topic2",
        "topic3",
        "from",
        "to",
        "value",
    ]
    db.add(data)
    print(db.disk_usage())
    db.teardown()
