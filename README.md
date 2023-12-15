# Polars-pc

Registers pyarrow-compute functions to expressions namespace under "pc". This isn't a rust port, it just calls them as they are with `map_batches`. There is no type checking as all of the methods are loaded dynamically from a list of strings.