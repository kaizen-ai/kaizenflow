def df_filter_by_column(df, col, values):
    """
    Filter dataframe by columns.

    :param df: DataFrame    
    :type df: pd.DataFrame
    :param col: Column name.
    :type col: str
    :param values: Valid values in column `col`.
    :type values: 

    :returns df_filtered: Filtered dataframe.
    :rtype df_filtered: pd.DataFrame
    """
    if values is None:
        return df

    if not isinstance(values, list):
        values = [values]
    values = set(values)
    _filter = df[col].isin(values)
    return df[_filter]

