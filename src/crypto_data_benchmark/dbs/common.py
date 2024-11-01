import pathlib
import subprocess
from typing import Protocol

import polars as pl

ROOT_DATA_DIR = pathlib.Path("data")


def disk_usage(path: str) -> str:
    """disk usage in human readable format"""
    blocks = subprocess.check_output(["du", "-s", path]).split()[0].decode("utf-8")
    return int(blocks) * 512


class DatabaseInterface(Protocol):
    def setup(self) -> None:
        """Establish connection to the database and create the table"""
        ...

    def teardown(self) -> None:
        """Delete the data folder if it exists"""
        ...

    def add(self, data: pl.DataFrame) -> None:
        """Add new data to the existing dataset"""
        ...

    def disk_usage(self) -> str:
        """Return the size of the data folder"""
        ...


if __name__ == "__main__":
    print(disk_usage("data"))
