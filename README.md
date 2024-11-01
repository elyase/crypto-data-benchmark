# Crypto Analytics Database Benchmarks

Comparison of several embedded, open source databases with crypto data & queries

## Results

M3 Macbook Air

Test data: ~5.0 GB (17_312_300 rows) of USDT transfers

| name       | disk usage       | peak memory           | ingestion time                     |
|------------|------------------|-----------------------|------------------------------------|
| lancedb    | 6.0G (8.5x)      | 359.9 MB (1.0x)      | 4 seconds (best)                   |
| parquet    | 720.6M (best)    | 0 Bytes              | 5 seconds (1.2x)                   |
| deltalake  | 1.6G (2.3x)      | 533.1 MB (1.5x)      | 10 seconds (2.5x)                  |
| duckdb     | 2.5G (3.6x)      | 4.0 GB (11.2x)       | 15 seconds (3.6x)                  |
| clickhouse | 3.1G (4.5x)      | 20.5 GB (56.9x)      | 1 minute and 42 seconds (23.2x)    |
| sqlite     | 9.7G (13.7x)     | 43.8 GB (121.6x)     | 13 minutes and 29 seconds (184.0x) |

## Databases

- [x] [parquet](https://parquet.apache.org/)
- [x] [deltalake](https://delta-io.github.io/delta-rs/usage/create-delta-lake-table/)
- [x] [duckdb](https://duckdb.org/docs/api/python/overview)
- [x] [lancedb](https://lancedb.github.io/lancedb/basic/)
- [x] [sqlite](https://docs.python.org/3/library/sqlite3.html#sqlite3-tutorial)
- [x] [clickhouse(chdb)](https://clickhouse.com/docs/en/chdb/install/python)
- [ ] [tiledb](https://docs.tiledb.com/main/how-to/arrays/creating-arrays/creating-dimensions)
- [ ] [articdb](https://docs.arcticdb.io/latest/)

## Benchmarks

- [x] Disk Usage: How much storage each db needs for the same data
- [x] Ingestion Time: total time to add the data
- [x] Peak Memory Usage (during ingestion)
- [ ] Point Lookup: get a WETH transfer row given a transaction hash
- [ ] Temporal Batch Scan: i.e. get all USDC transfers in a block range
- [ ] Multidimensional queries


## Notes

* Use this code to find out which database is best if you want to optimize for metric X (ex peak memory usage) when you only have Y hours to adjust settings and make your choice.
* Be mindful of [benchmarking caveats](https://matthewrocklin.com/biased-benchmarks.html) and adjust the parameters to your use case
* Related benchmarks:
    * [Coiled TPC-H Benchmarks](https://docs.coiled.io/blog/tpch.html#tpc-h-experimental-details)
    * [ClickBench: a Benchmark For Analytical DBMS](https://benchmark.clickhouse.com/)
* ArticDB doesnt have [osx wheels yet](https://github.com/man-group/ArcticDB/issues/759)






