import polars as pl
import pyarrow as pa


def deepcopy_arrow(arr: pa.Array) -> pa.Array:
    return arr.take(pa.array(range(len(arr))))


class PolarsProvider:
    def setup(self) -> None:
        """Initialize an empty DataFrame"""
        pass

    def teardown(self) -> None:
        """Clear the DataFrame"""
        pass

    def add(self, data: pl.DataFrame) -> None:
        fake_allocation = bytes(data.estimated_size("bytes"))

    def disk_usage(self) -> str:
        """Return the size of the in-memory DataFrame"""
        return 0


if __name__ == "__main__":
    from crypto_data_benchmark.data.usdt_transfers import usdt_transfers

    db = PolarsProvider()
    db.setup()
    data = usdt_transfers(scale=100)
    db.add(data)
    print(db.disk_usage())
    db.teardown()
