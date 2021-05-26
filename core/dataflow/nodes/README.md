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
