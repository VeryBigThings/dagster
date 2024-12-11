import pandas as pd

normalization_special_cases = {
  "TrEbor Inventory": "Trebor Inventory",
}

# normalize string column and return dataframe
def normalize_string_column(df: pd.DataFrame, column: str):
    # df[column] = df[column].str.strip().str.upper()
    df[column] = df[column].str.strip()
    df[column] = df[column].replace(normalization_special_cases)
    return df
