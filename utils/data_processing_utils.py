import pandas as pd
from pathlib import Path


def london_cleaner(
    df: pd.DataFrame, splits_keys: list[str], cols_to_drop: list[str]
) -> pd.DataFrame:
    """
    ### Function to clean London marathons' data.
    #### `N.B` A copy of the original DataFrame is returned after all operations are performed.
    ----
    ### Arguments:
    + df: DataFrame with data to convert.
    + splits_keys: Name of split columns.
    + cols_to_drop: Name of columns to remove from the DataFrame.
    ----
    ### Returns a new DataFrame after applying the operations below.
    1. Columns in `cols_to_drop` are removed.
    2. Remove runners that did not start the marathon `race_stat = Not Started`
    3. Dropping runners that do not have a non-null value in these columns `[age_cat, gender, last_split]`.
    4. Replacing `['-', ', -, {SPACE}]` with an empty character.
    5. The time and pace for each split in `splits_keys` are converted into seconds.
    6. Convert columns into best possible dtype using `convert_dtypes()`.
    7. The time, pace, and speed for each split in `splits_keys` dtype are converted to `float32`.
    """
    df = df.copy()
    # 1. Removing unused columns.
    if cols_to_drop and len(cols_to_drop) >= 1:
        df.drop(cols_to_drop, axis=1, inplace=True)

    # 2. Removing runners did not start.
    print("Removing Runners That did not start: ")
    rows_count = len(df)
    df = df.drop(df.loc[df.race_state == "Not Started"].index).reset_index(drop=True)
    print(
        f"Original rows count: {rows_count} || New rows count: {len(df)} || Dropped Rows: {rows_count - len(df)}"
    )

    # 3. Dropping runners that do not have a non-null value in these columns [age_cat, gender, last_split]
    df = drop_null_by_col(df, ["age_cat", "gender", "last_split"])

    # 4. Replace the characters in to_replace by the char in value. N.B Works but Slow.
    df = replace_value_in_cols(df, regex_pattern="('-'|'+|-| )")

    # 5. Converting time and pace into seconds.
    df = convert_to_sec(df, splits_keys)

    # 6. Convert columns into best possible dtypes (dtypes are inferred).
    df = df.convert_dtypes()

    # 7. Converting dtype.
    df = convert_dtype(df, splits_keys)

    return df


def convert_to_sec(df: pd.DataFrame, splits_keys: list[str]) -> pd.DataFrame:
    """
    ### Function to convert the splits' time and pace to their total seconds.
    ----
    ### Arguments:
    + df: DataFrame with data to convert.
    + splits_keys: Name of split columns.
    ----
    ### Returns the DataFrame.
    """
    for key in splits_keys:
        k_time = f"{key}_time"
        k_pace = f"{key}_pace"
        # Converting time and pace of a split from hh:mm:ss to seconds.
        df[k_time] = (
            pd.to_timedelta(df[k_time], errors="coerce")
            .astype("timedelta64[s]")
            .dt.seconds
        )
        # Adding "00:" prefix to the pace to change its format from mm:ss -> hh:mm:ss
        # if the pace is already in the correct format (hh:mm:ss) it is not changed
        non_null_rows = ~df[k_pace].isnull()
        df.loc[non_null_rows, k_pace] = df.loc[non_null_rows, k_pace].map(
            lambda x: "00:" + x if len(x) == 5 else x
        )
        df[k_pace] = (
            pd.to_timedelta(df[k_pace], errors="coerce")
            .astype("timedelta64[s]")
            .dt.seconds
        )
    return df


def convert_dtype(df: pd.DataFrame, splits_keys: list[str]) -> pd.DataFrame:
    """
    ### Function to convert the splits' time, pace, and speed dtype.
    ----
    ### Arguments:
    + df: DataFrame with data to convert.
    + splits_keys: Name of split columns.
    ----
    ### Returns the DataFrame.
    """
    for key in splits_keys:
        df[f"{key}_time"] = df[f"{key}_time"].astype("Int32")
        df[f"{key}_pace"] = df[f"{key}_pace"].astype("Int32")
        df[f"{key}_speed"] = pd.to_numeric(
            df[f"{key}_speed"], errors="coerce", downcast="float"
        )
    return df


def save_df(df: pd.DataFrame, file_path: str) -> None:
    """
    ### Function to save Pandas DataFrame, `file_path` should contain the full path where to save the DataFrame and the file name with `csv` extension.
    ----
    ### Arguments:
    + df: DataFrame to save.
    + file_path: Must include the path where to save file and the file name.
    ----
    ### Returns: None
    """
    f_path = "/".join(file_path.split("/")[:-1])
    f_path = Path(f_path)
    if not f_path.exists():
        f_path.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, index=False)


def drop_null_by_col(df: pd.DataFrame, cols: str) -> pd.DataFrame:
    """
    ### Function to drop rows if they have a Null value in the specified column.
    ----
    ### Arguments
    + df: The DataFrame to be used.
    + cols: Columns name to check for null values.
    ----
    ### Returns the DataFrame after rows with null values have been dropped.
    """
    for col in cols:
        org_count = len(df)
        df.dropna(subset=col, inplace=True)
        dropped_count = org_count - len(df)
        print(
            f"Original rows count: {org_count} || New rows count: {len(df)} || Dropped rows based on {col.center(11)}: {dropped_count}"
        )
    return df


def replace_value_in_cols(
    df: pd.DataFrame, regex_pattern: str = "('-'|'+|-| )", replace_value=None
):
    """
    ### Function to replace the characters in the `regex_pattern` by `replace_pattern`, it utilise pandas replace function with `regex=True`.
    #### N.B The function only check split columns, their name start with `k_`.
    ----
    ### Arguments:
    + df: DataFrame to operate on.
    + regex_pattern: The regex pattern to match characters to.
        Default: `('-'|'+|-| )`; which matches `(hyphen: - or '-') or (apostrophe: ') or (space: )`.
    + replace_value: The character to replace the matched value.
        Default: None.
    ----
    ### Returns the DataFrame after replacing the match characters with the new value.
    """
    cols = df.columns.str.startswith("k_")
    df.loc[:, cols] = df.loc[:, cols].replace(
        to_replace=regex_pattern, value=replace_value, regex=True
    )
    return df
