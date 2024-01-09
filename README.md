# Polars-pc

Registers pyarrow-compute functions to expressions namespace under "pc". This isn't a rust port, it just calls them as they are with `map_batches`. All the methods that only have a single input are dynamically loaded. For instance:

```
df.with_columns(c=pl.col('a').pc.cumulative_sum())
```

Methods which take two inputs are statically loaded, right now only `index_in` is loaded and that works like

```
df.with_columns(c=pl.col('a').pc.index_in('b'))
```

## Install

```
pip install polars-pc
```

## Usage

```
import polars_pc
import polars as pl
df=pl.DataFrame({
    'a':[1,2,2,3,4],
    'b':[1,1,1,3,3]
})
df.with_columns(c=pl.col('a').pc.cumulative_sum())
df.with_columns(c=pl.col('a').pc.index_in('b'))
```