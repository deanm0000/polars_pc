# Polars-pc

Registers pyarrow-compute functions to expressions namespace under "pc". This isn't a rust port, it just calls them as they are with `map_batches`. All the methods that only have a single input are dynamically loaded. For instance:

```
df.with_columns(c=pl.col('a').pc.cumulative_sum())
```

Methods which take two inputs are statically loaded, right now only `index_in` is loaded and that works like

```
df.with_columns(c=pl.col('a').pc.index_in('b'))
```