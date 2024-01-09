import polars as pl
import pyarrow.compute as pc

_pc_funcs = [
    "abs",
    "abs_checked",
    "acos",
    "array_sort_indices",
    "asin",
    "atan",
    "bit_wise_not",
    "ceil",
    "coalesce",
    "cos",
    "cos_checked",
    "cumulative_max",
    "cumulative_mean",
    "cumulative_min",
    "cumulative_prod",
    "cumulative_prod_checked",
    "cumulative_sum",
    "cumulative_sum_checked",
    "dictionary_decode",
    "dictionary_encode",
    "drop_null",
    "exp",
    "fill_null_backward",
    "fill_null_forward",
    "floor",
    "indices_nonzero",
    "is_finite",
    "is_inf",
    "is_nan",
    "is_null",
    "is_valid",
    "ln",
    "log10",
    "log1p",
    "log2",
    "make_struct",
    "max_element_wise",
    "min_element_wise",
    "mode",
    "negate",
    "negate_checked",
    "pairwise_diff",
    "pairwise_diff_checked",
    "quantile",
    "rank",
    "round",
    "round_to_multiple",
    "run_end_encode",
    "sign",
    "sin",
    "sin_checked",
    "sort_indices",
    "sqrt",
    "tan",
    "tan_checked",
    "tdigest",
    "true_unless_null",
    "trunc",
    "unique",
    "value_counts",
    "ascii_capitalize",
    "ascii_is_alnum",
    "ascii_is_alpha",
    "ascii_is_decimal",
    "ascii_is_lower",
    "ascii_is_printable",
    "ascii_is_space",
    "ascii_is_title",
    "ascii_is_upper",
    "ascii_lower",
    "ascii_ltrim_whitespace",
    "ascii_reverse",
    "ascii_rtrim_whitespace",
    "ascii_split_whitespace",
    "ascii_swapcase",
    "ascii_title",
    "ascii_trim_whitespace",
    "ascii_upper",
    "binary_join_element_wise",
    "binary_length",
    "string_is_ascii",
    "utf8_capitalize",
    "utf8_is_alnum",
    "utf8_is_alpha",
    "utf8_is_decimal",
    "utf8_is_digit",
    "utf8_is_lower",
    "utf8_is_numeric",
    "utf8_is_printable",
    "utf8_is_space",
    "utf8_is_title",
    "utf8_is_upper",
    "utf8_length",
    "utf8_lower",
    "utf8_ltrim_whitespace",
    "utf8_reverse",
    "utf8_rtrim_whitespace",
    "utf8_split_whitespace",
    "utf8_swapcase",
    "utf8_title",
    "utf8_trim_whitespace",
    "utf8_upper",
]


@pl.api.register_expr_namespace("pc")
class PC:
    # Thanks https://stackoverflow.com/questions/13079299/dynamically-adding-methods-to-a-class
    @staticmethod
    def make_inner_func(func_str):
        pc_func = getattr(pc, func_str)

        def new_func(self):
            return self._expr.map_batches(
                lambda x: (pl.from_arrow(pc_func(x.to_arrow())))
            )

        return new_func

    def __init__(self, expr: pl.Expr):
        from types import MethodType

        self._expr = expr
        for func_str in _pc_funcs:
            new_func = self.make_inner_func(func_str)
            method = MethodType(new_func, self)
            setattr(self, func_str, method)

    def index_in(self, other_col: str | pl.Expr):
        if isinstance(other_col, str):
            other_col = pl.col(other_col)
        return pl.struct(self._expr.alias("__x"), other_col.alias("__y")).map_batches(
            lambda S: (
                pl.from_arrow(
                    pc.index_in(
                        S.struct.field("__x").to_arrow(),
                        S.struct.field("__y").to_arrow(),
                    )
                )
            )
        )
