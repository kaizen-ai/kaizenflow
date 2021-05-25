# Column transformation patterns

We document various column transformation patterns, focusing on how multilevel
column indices are handled.

Throughout, let `MN0`, `MN1`, etc., denote instrument symbols.
Let `N` denote the column index depth of the input dataflow dataframe.

Multiindexed column conventions:
  - leaf column names correspond to symbols
  - input column groups specify column levels up to but not including leaves
  - all leaves within the input column group are selected implicitly (e.g., no
    need to explicitly enumerate all symbols in a universe)

## CrossSectionalDfToDf

Core transformation:
  - dataframe -> dataframe
  - input columns processed cross-sectionally (e.g., each output column
     typically depends upon all input columns)
  - may or may not rename columns
  - e.g., columns transform like
    Input:
    ```
    MN0 MN1 MN3 MN4`
    ```
    Output:
    ```
    0 1 2 3
    ```
    if the transformation is principal component projection

Dataflow transformation:
```
ret_0           close
MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3
```
with PCA applied to `ret_0` becomes
```
pca     ret_0           close
0 1 2 3 MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3
```

Comments:
  - The core transformation need not rename columns. Residualization is an
    example of this.
  - This transformation behavior makes sense when cross-sectional information
    is important

Examples:
  - Principcal component projection
  - Residualization

User responsibilities:
  - implement stateful node
  - specify input column tuple with `N - 1` levels
  - specify `N - 1`st output column name

Signatures:
```
def _preprocess(
    df: pd.DataFrame,
    in_col_group: Tuple(_COL_TYPE),
) -> pd.DataFrame:
```

```
# Here we can derive `out_col_group` from `in_col_group` and the user-supplied
#  output column name.
def _postprocess(
    df_in: pd.DataFrame,
    df_out: pd.DataFrame,
    out_col_name: str,
) -> pd.DataFrame
```

##  SrsToDf

Core transformation:
  - series -> dataframe
  - columns are processed independently of each other
  - creates new column names (typically not dependent upon the name of the
    input series)
  - e.g., columns transform like
    Input:
    ```
    MN0
    ```
    Output:
    ```
    lag_1 lag_2
    ```

Dataflow transformation:
```
ret_0
MN0 MN1 MN2 MN3
```
with two lags computed becomes
```
ret_0_lag_1     ret_0_lag_2     ret_0
MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3
```

Examples:
  - Series decompositions (e.g., STL, Fourier coefficients, wavelet levels)
  - Signal filters (e.g., smooth moving averages, z-scoring, outlier processing)
  - Rolling features (e.g., moments, centered moments)
  - Lags
  - Volatility modeling

User responsibilities:
  - provide transformer function
  - specify input column tuple with `N - 1` levels

Signatures:
```
def _preprocess(
    df: pd.DataFrame,
    in_col_group: Tuple(_COL_TYPE),
) -> pd.DataFrame:
```

```
def _postprocess(
    df_in: pd.DataFrame,
    df_out_group: Dict[str, pd.DataFrame],
    out_col_prefix: Optional[str] = None,
) -> pd.DataFrame
"""

:param df_out_group: dataframes indexed by symbol
"""
```

## SrsToSrs

Core transformation:
  - series -> series
  - preserves series name
  - e.g., columns transform like
    Input:
    ```
    MN0 MN1 MN2 MN3
    ```
    Output:
    ```
    MN0 MN1 MN2 MN3
    ```

Dataflow transformation:
```
ret_0
MN0 MN1 MN2 MN3
```
becomes
```
ret_0_clipped
MN0 MN1 MN2 MN3
```

Comments:
  - Output leaf column names should agree with input leaf column names

Signatures:
```
def _preprocess(
    df: pd.DataFrame,
    in_col_group: Tuple(_COL_TYPE),
) -> pd.DataFrame:
```

```
def _postprocess(
    df_in: pd.DataFrame,
    srs_out_group: List[pd.Series],
    out_col_name: str,
) -> pd.DataFrame
```

## GroupedColumnDfToDf

Core transformation:
  - dataframe -> dataframe
  - e.g., columns transform like
    Input:
    ```
    feat1 feat2 y_0
    ```
    Output:
    ```
    y_2_hat y_2
    ```

Dataflow transformation:
```
feat1           feat2           y_0
MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3
```
becomes
```
y_2_hat         y_2             feat1           feat2           y_0
MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3
```

User responsibilities:
  - provide transformer function
  - specify input column tuples with `N - 1` levels
