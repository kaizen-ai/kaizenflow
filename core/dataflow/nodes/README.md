# Column transformation patterns

We document various column transformation patterns, focusing on how multilevel
column indices are handled.

Throughout, let `MN0`, `MN1`, etc., denote instrument symbols.
Let `N` denote the column index depth of the input dataflow dataframe.

## Case 1

Core transformation:
  - dataframe -> dataframe
  - stateful
  - may rename columns
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

Column transformation assumptions:
  - leaf column names correspond to symbols
  - input column names specified up to leaves
  - all leaves within the input column group are selected implicitly (e.g., no
    need to explicitly enumerate all symbols in a universe)
  - the user specifies the description of the output column group

Comments:
  - The core transformation need not rename columns. Residualization is an
    example of this.
  - This transformation behavior makes sense when cross-sectional information
    is important.

Examples:
  - Principcal component projection
  - Residualization

User responsibilities:
  - implement stateful node
  - specify input column tuple with `N - 1` levels
  - specify `N - 1`st output column name

## Case 2

Core transformation:
  - series -> dataframe
  - stateless
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

Column transformation assumptions:
  - leaf column names correspond to symbols
  - input column names specified up to leaves
  - all leaves within the input column group are selected implicitly (e.g., no
    need to explicitly enumerate all symbols in a universe)
  - output leaf column names are the same as the input leaf column names
  - output column level names agree with input column level names up to the
    name that immediately precedes the leaf column name.

Examples:
  - Series decompositions (e.g., STL, Fourier coefficients, wavelet levels)
  - Signal filters (e.g., smooth moving averages, z-scoring, outlier processing)
  - Rolling features (e.g., moments, centered moments)
  - Lags

User responsibilities:
  - provide transformer function
  - specify input column tuple with `N - 1` levels

## Case 3

Core transformation:
  - series -> dataframe
  - stateful
  - creates new column names (typically not dependent upon the name of the
    input series)
  - e.g., columns transform like
    Input:
    ```
    MN0
    ```
    Output:
    ```
    vol vol_hat
    ```

User responsibilities:
  - implement stateful node
  - specify input column tuple with `N - 1` levels

## Case 4

Core transformation:
  - series -> series
  - stateless
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

Column transformation assumptions:
  - all leaves within the input column group are selected implicitly (e.g., no
    need to explicitly enumerate all symbols in a universe)
  - output leaf column names are the same as the input leaf column names
  - output column level names agree with input column level names up to the
    name that immediately precedes the leaf column name

User responsibilities:
  - provide transformer function
  - specify input column tuple with `N - 1` levels
  - specify `N - 1`st output column name

## Case 5

Core transformation:
  - dataframe -> dataframe
  - stateful
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
  - specify input column tuple with `N - 1` levels
