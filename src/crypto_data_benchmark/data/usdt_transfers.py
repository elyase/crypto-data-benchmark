import pathlib

import polars as pl

DATA_PATH = pathlib.Path.cwd().parent / "data" / "logs.parquet"


async def collect_events(path: str):
    import hypersync as hs

    client = hs.HypersyncClient(hs.ClientConfig())
    from_block = 20_000_000
    to_block = from_block + 10_000
    address = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
    event_signature = (
        "Transfer(address indexed from, address indexed to, uint256 value)"
    )
    topic0 = hs.signature_to_topic0(event_signature)
    query = hs.Query(
        from_block=from_block,
        to_block=to_block,
        logs=[
            hs.LogSelection(
                address=[address],
                topics=[[topic0]],
            )
        ],
        field_selection=hs.FieldSelection(
            log=[e.value for e in hs.LogField],
        ),
    )
    config = hs.StreamConfig(
        column_mapping=hs.ColumnMapping(
            # map value columns to float so we can do calculations with them
            decoded_log={
                "value": hs.DataType.FLOAT64,
            },
        ),
        # give event signature so client can decode logs into decoded_logs.parquet file
        event_signature=event_signature,
    )

    res = await client.collect_arrow(query, config)
    logs = pl.from_arrow(res.data.logs)
    decoded_logs = pl.from_arrow(res.data.decoded_logs)
    logs = pl.concat([logs, decoded_logs], how="horizontal")
    logs.write_parquet(path)


def download_usdt_transfers(path: pathlib.Path = DATA_PATH):
    import asyncio

    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        asyncio.run(collect_events(path))
    print(f"Downloaded USDT transfers to {path}")


def usdt_transfers(scale: int = 1, hex=False):
    if not DATA_PATH.exists():
        download_usdt_transfers()

    df = pl.read_parquet(DATA_PATH)
    # concat the df with itself scale times
    if hex:
        import polars_evm

        df = df.evm.binary_to_hex()
    df = pl.concat([df for _ in range(scale)])
    return df


if __name__ == "__main__":
    df = usdt_transfers(scale=100)
    print(df.schema)
    print(df.estimated_size("gb"))
    print(df.shape)
