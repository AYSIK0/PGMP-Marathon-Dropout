import pandas as pd


def london_cleaner(
    df: pd.DataFrame, splits_keys: list[str], cols_to_drop: list[str]
) -> pd.DataFrame:
    """
    ### `WIP` Function to clean London marathons' data.
    ----
    ### Arguments:
    + df: DataFrame with data to convert.
    + splits_keys: Name of split columns.
    + cols_to_drop: Name of columns to remove from the DataFrame.
    ----
    ### Returns a new DataFrame after applying the operations below.
    1. Convert columns into best possible dtype using `convert_dtypes()`.
    2. Columns in `cols_to_drop` are removed.
    3. Remove runners that did not start the marathon `race_stat = Not Started`
    4. Replacing `['-', ', -, {SPACE}]` with an empty character.
    5. The time and pace for each split in `splits_keys` are converted into seconds.
    6. The time, pace, and speed for each split in `splits_keys` dtype are converted to `float32`.
    """
    df = df.copy()
    # 1. Convert columns into best possible dtypes (dtypes are inferred).
    df = df.convert_dtypes()

    # 2. Removing unused columns.
    if cols_to_drop and len(cols_to_drop) >= 1:
        df.drop(cols_to_drop, axis=1, inplace=True)

    # 3. Removing runners did not start.
    print(f"Total samples before removing 'Not Started' runners: {len(df)}")
    df = df.drop(df.loc[df.race_state == "Not Started"].index).reset_index(drop=True)
    print(f"Total samples after removing 'Not Started' runners: {len(df)}")

    # 4. Replace the characters in to_replace by the char in value. N.B Works but Slow.
    cols_to_ignore = ["age_cat", "gender", "race_state", "last_split"]
    df.loc[:, df.columns.difference(cols_to_ignore)] = df.loc[
        :, df.columns.difference(cols_to_ignore)
    ].replace(to_replace=r"('-'|'+|-| )", value="", regex=True)

    # 5. Converting time and pace into seconds.
    df = convert_to_sec(df, splits_keys)

    # 6. Converting dtype.
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
        df[f"{key}_time"] = df[f"{key}_time"].astype("float32")
        df[f"{key}_pace"] = df[f"{key}_pace"].astype("float32")
        df[f"{key}_speed"] = pd.to_numeric(
            df[f"{key}_speed"], errors="coerce", downcast="float"
        ).astype("float32")
    return df
