def convert_columns_to_Int64(df, columns):
    for column in columns:
        df[column] = df[column].astype('Int64')

def get_distinct_across_columns(df, columns):
    for c in columns:
        if df[c].dtype == "object":
            df[c] = df[c].str.strip()

    distinct_keys = df[columns].drop_duplicates()
    distinct_keys = distinct_keys.dropna()
    distinct_keys = distinct_keys[~distinct_keys.eq("") & ~distinct_keys.eq("NaN")]
    return distinct_keys.sort_values(by=columns)
