import os
import re
import subprocess
import sys

import polars as pl
from humanize import naturalsize, precisedelta

from crypto_data_benchmark.data.usdt_transfers import usdt_transfers
from crypto_data_benchmark.dbs.clickhouse_provider import ClickHouseProvider
from crypto_data_benchmark.dbs.deltalake_provider import DeltaLakeProvider
from crypto_data_benchmark.dbs.duckdb_provider import DuckDBProvider
from crypto_data_benchmark.dbs.lancedb_provider import LanceDBProvider
from crypto_data_benchmark.dbs.parquet_provider import ParquetProvider
from crypto_data_benchmark.dbs.sqlite_provider import SQLiteProvider
from crypto_data_benchmark.scalene_profiler import profile


def get_name_from_class(cls: type) -> str:
    """Get the name of the class without the module name and the Provider suffix"""
    return cls.__name__.lower().replace("provider", "")


def run_benchmarks(data: pl.DataFrame):
    dbs = [
        ParquetProvider(),
        LanceDBProvider(),
        DeltaLakeProvider(),
        DuckDBProvider(),
        ClickHouseProvider(),
        SQLiteProvider(),
    ]

    results = []
    for db in dbs:
        db.setup()
        name = get_name_from_class(db.__class__)
        print(f"Benchmarking {name}")
        if name == "clickhouse":
            # there is a bug in the Python chdb engine with large_binary columns so we use strings instead
            data = data.evm.binary_to_hex()
        metrics = profile(db.add, data)
        metrics["name"] = name
        metrics["disk_usage"] = db.disk_usage()
        results.append(metrics)
        db.teardown()

    return results


def format_results(results: list[dict], data: pl.DataFrame) -> str:
    pl.Config.set_tbl_hide_column_data_types(True)
    pl.Config.set_tbl_hide_dataframe_shape(True)
    pl.Config.set_fmt_str_lengths(1000)
    df = pl.DataFrame(results).sort("ingestion_time")
    # now add relative to the min of the ingestion time and the disk usage
    min_ingestion_time = df["ingestion_time"].min()
    min_disk_usage = df["disk_usage"].min()
    min_peak_memory = df["peak_memory"].sort().item(1)
    df = df.with_columns(
        ingestion_time_relative=(pl.col("ingestion_time") / min_ingestion_time)
        .map_elements(lambda x: f"({x:,.1f}x)", return_dtype=pl.String)
        .str.replace("nanx", "best")
        .str.replace("1.0x", "best"),
        disk_usage_relative=(pl.col("disk_usage") / min_disk_usage)
        .map_elements(lambda x: f"({x:,.1f}x)", return_dtype=pl.String)
        .str.replace("nanx", "best")
        .str.replace("1.0x", "best"),
        peak_memory_relative=(pl.col("peak_memory") / min_peak_memory)
        .map_elements(lambda x: f"({x:,.1f}x)", return_dtype=pl.String)
        .str.replace("nanx", "best"),
    )
    # convert to str and add x suffix to the relative columns

    df = df.select(
        [
            "name",
            pl.concat_str(
                [
                    pl.col("disk_usage").map_elements(
                        lambda x: f"{naturalsize(x, gnu=True)}", return_dtype=pl.String
                    ),
                    pl.lit(" "),
                    pl.col("disk_usage_relative"),
                ]
            ).alias("disk usage"),
            pl.concat_str(
                [
                    pl.col("peak_memory").map_elements(
                        lambda x: f"{naturalsize(x)}", return_dtype=pl.String
                    ),
                    pl.lit(" "),
                    pl.col("peak_memory_relative"),
                ]
            )
            .str.replace("(0.0x)", "")
            .alias("peak memory"),
            pl.concat_str(
                [
                    pl.col("ingestion_time").map_elements(
                        lambda x: f"{precisedelta(x)}", return_dtype=pl.String
                    ),
                    pl.lit(" "),
                    pl.col("ingestion_time_relative"),
                ]
            ).alias("ingestion time"),
        ]
    )
    output = f"Test data size: {naturalsize(data.estimated_size())}\n"
    output += str(df)
    return output


def update_readme(output: str):
    with open("README.md", "r") as file:
        content = file.read()

    pattern = r"\nData size:.*┘\n"
    updated_content = re.sub(pattern, output, content, flags=re.DOTALL)

    with open("README.md", "w") as file:
        file.write(updated_content)


if __name__ == "__main__":
    if not os.getenv("SCALE_PROFILING"):
        subprocess.run(
            [
                "scalene",
                sys.argv[0],
                "--no-browser",
            ],
            env={"SCALE_PROFILING": "1", **os.environ},
        )
    else:
        data = usdt_transfers(scale=102)
        results = run_benchmarks(data)
        output = format_results(results, data)
        update_readme(output)
        print(output)
