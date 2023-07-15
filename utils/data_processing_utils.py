import pandas as pd
import numpy as np
from pathlib import Path
import re


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
    2. Replacing `['-', ', -, {SPACE}]` with an empty character.
    3. Remove runners that did not start the marathon `race_stat = Not Started` or `all splits' data is null`.
    4. Dropping runners that do not have a non-null value in these columns `[age_cat, gender]`.
    5. The time and pace for each split in `splits_keys` are converted into seconds.
    6. The time, pace, and speed for each split in `splits_keys` dtype are converted to `Int32`, `Int32`, and`Float32` respectively.
    7. Convert columns into best possible dtype using `convert_dtypes()`.
    """
    df = df.copy()
    # 1. Removing unused columns.
    if cols_to_drop and len(cols_to_drop) >= 1:
        df.drop(cols_to_drop, axis=1, inplace=True)

    # 2. Replace the characters that match `regex_pattern` by the `replace_value`. N.B Works but Slow.
    df = replace_value_in_cols(df, regex_pattern="('-'|'+|-| )")

    # 3. Removing runners did not start.
    # 3.1 Runners that have a race_state == "Not Started" will be dropped.
    print("** Removing Runners That did not start:")
    rows_count = len(df)
    df = df.drop(df.loc[df.race_state == "Not Started"].index).reset_index(drop=True)
    # 3.2 Runners that do not have any split data will be dropped.
    not_started_indices = df[
        df.iloc[:, df.columns.str.startswith("k_")].isna().values.all(axis=1)
    ].index
    df = df.drop(index=not_started_indices).reset_index(drop=True)
    print(
        f"Original rows count: {rows_count} || New rows count: {len(df)} || Dropped Rows: {rows_count - len(df)}"
    )

    # 4. Dropping runners that have a null value in these columns [age_cat, gender].
    print("** Dropping rows with null values in `age_cat` and `gender` columns:")
    df = drop_null_by_col(df, ["age_cat", "gender"])

    # 5. Converting time and pace into seconds.
    df = convert_to_sec(df, splits_keys)

    # 6. Converting dtype.
    df = convert_split_dtype(df, splits_keys)

    # 7. Convert columns into best possible dtypes (dtypes are inferred).
    df = df.convert_dtypes()

    return df


def hamburg_cleaner(
    df: pd.DataFrame,
    splits_keys: list[str],
    cols_to_drop: list[str],
    last_split_std: dict,
    cols_order: list[str],
) -> pd.DataFrame:
    """
    ### Function to clean Hamburg marathons' data.
    #### `N.B` A copy of the original DataFrame is returned after all operations are performed.
    ----
    ### Arguments:
    + df: DataFrame with data to convert.
    + splits_keys: Name of split columns.
    + cols_to_drop: Name of columns to remove from the DataFrame.
    + last_split_std: Dictionary of key value pair of old last_split column names `key` and the new ones `values`.
    + cols_order: The order of the returned DataFrame columns, it's a list of columns' names arranged in the desired order.
    ----
    ### Returns a new DataFrame after applying the operations below.
    1. Columns in `cols_to_drop` are removed.
    2. Replacing `['-', ', -, {SPACE}]` with an empty character.
    3. Remove runners that did not start the marathon `All the splits columns values are None`.
    4. Dropping runners that do not have a non-null value in these columns `[age_cat, gender, last_split]`.
    5. The time and pace for each split in `splits_keys` are converted into seconds.
    6. The time, pace, and speed for each split in `splits_keys` dtype are converted to `Int32`, `Int32`, and`Float32` respectively.
    7. Adding the `last_split` and `race_state` columns.
    8. Replacing `age_cat` and `last_split` values with the standard values.
    9. Reordering the DataFrame columns according to cols_order.
    10. Convert columns into best possible dtype using `convert_dtypes()`.
    """
    df = df.copy()
    # 1. Removing unused columns.
    if cols_to_drop and len(cols_to_drop) >= 1:
        df.drop(cols_to_drop, axis=1, inplace=True)

    # 2. Replace the characters match `regex_pattern` by the `replace_value`. N.B Works but Slow.
    df = replace_value_in_cols(df)

    # 3. Removing runners did not start. (if all splits columns are null then the runner did not start.)
    print("** Removing Runners That did not start")
    rows_count = len(df)
    not_started_indices = df[
        df.iloc[:, df.columns.str.startswith("k_")].isna().values.all(axis=1)
    ].index
    df.drop(index=not_started_indices, inplace=True)
    print(
        f"Original rows count: {rows_count} || New rows count: {len(df)} || Dropped Rows: {rows_count - len(df)}"
    )

    # 4. Dropping runners that have a null value in these columns [age_cat, gender]
    print("** Dropping rows with null values in `age_cat` and `gender` columns:")
    df = drop_null_by_col(df, ["age_cat", "gender"])

    # 5. Converting time and pace into seconds.
    df = convert_to_sec(df, splits_keys)

    # 6. Converting splits' time, pace, and speed dtype.
    df = convert_split_dtype(df, splits_keys)

    # 7. Adding the `last_split` and `race_state` columns.
    df["last_split"] = df.iloc[:, df.columns.str.contains("time")].idxmax(axis=1)
    df["race_state"] = np.where(
        df["last_split"] == "k_finish_time", "Finished", "Started"
    )

    # 8. Replacing `age_cat` and `last_split` values with the standard values.
    age_cat_std = get_ham_age_cat_dict(df)
    df["age_cat"] = df["age_cat"].replace(age_cat_std)
    df["last_split"] = df["last_split"].replace(last_split_std)

    # 9. Reordering the DataFrame columns.
    df = df[cols_order]

    # 10. Convert columns into best possible dtypes (dtypes are inferred).
    df = df.convert_dtypes()

    return df


def stockholm_cleaner(
    df: pd.DataFrame,
    splits_keys: list[str],
    cols_to_drop: list[str],
    last_split_std: dict,
    cols_order: list[str],
    year: int,
) -> pd.DataFrame:
    """
    ### Function to clean Stockholm marathons' data.
    #### `N.B` A copy of the original DataFrame is returned after all operations are performed.
    ----
    ### Arguments:
    + df: DataFrame with data to convert.
    + splits_keys: Name of split columns.
    + cols_to_drop: Name of columns to remove from the DataFrame.
    + last_split_std: Dictionary of key value pair of old last_split column names `key` and the new ones `values`.
    + cols_order: The order of the returned DataFrame columns, it's a list of columns' names arranged in the desired order.
    + year: The marathon year.
    ----
    ### Returns a new DataFrame after applying the operations below.
    1. Columns in `cols_to_drop` are removed.
    2. Replacing `['-', ', -, {SPACE}]` with an empty character.
    3. Remove runners that did not start the marathon `All the splits columns values are None`.
    4. Dropping runners that do not have a non-null value in these columns `[age_cat, gender, last_split]`.
    5. The time and pace for each split in `splits_keys` are converted into seconds.
    6. The time, pace, and speed for each split in `splits_keys` dtype are converted to `Int32`, `Int32`, and`Float32` respectively.
    7. Adding the `last_split` column and updating `race_state` column.
    8. Steps:
    + 8.1 Calculate the age based on the year of birth.
    + 8.2 Dropping any non-adult runner age < 18.
    + 8.3 Get the appropriate age category.
    + 8.4 the column named `yob` changed to `age_cat`.
    9. Replacing `last_split` values with the standard values.
    10. Reordering the DataFrame columns according to cols_order.
    11. Convert columns into best possible dtype using `convert_dtypes()`.
    """
    df = df.copy()
    # 1. Removing unused columns.
    if cols_to_drop and len(cols_to_drop) >= 1:
        df.drop(cols_to_drop, axis=1, inplace=True)

    # 2. Replace the characters match `regex_pattern` by the `replace_value`. N.B Works but Slow.
    df = replace_value_in_cols(df)

    # 3. Removing runners did not start. (if all splits columns are null then the runner did not start.)
    print("** Removing Runners That did not start:")
    rows_count = len(df)
    not_started_indices = df[
        df.iloc[:, df.columns.str.startswith("k_")].isna().values.all(axis=1)
    ].index
    df.drop(index=not_started_indices, inplace=True)
    print(
        f"Original rows count: {rows_count} || New rows count: {len(df)} || Dropped Rows: {rows_count - len(df)}"
    )

    # 4. Dropping runners that have a null value in these columns [yob, gender]
    print("** Dropping rows with null values in `yob` and `gender` columns:")
    df = drop_null_by_col(df, ["yob", "gender"])

    # 5. Converting time and pace into seconds.
    df = convert_to_sec(df, splits_keys)

    # 6. Converting splits' time, pace, and speed dtype.
    df = convert_split_dtype(df, splits_keys)

    # 7. Adding the `last_split` and `race_state` columns.
    df["last_split"] = df.iloc[:, df.columns.str.contains("time")].idxmax(axis=1)
    df["race_state"] = np.where(
        df["last_split"] == "k_finish_time", "Finished", "Started"
    )

    # 8.
    # 8.1 Calculate the age based on the year of birth.
    df["yob"] = df["yob"].apply(calc_age, args=(year,))
    # 8.2 Dropping any non-adult runner age < 18.
    print("** Dropping non-adult runners (age < 18):")
    org_count = len(df)
    df.drop(df[df["yob"] < 18].index, inplace=True)
    dropped_count = org_count - len(df)
    print(
        f"Original rows count: {org_count} || New rows count: {len(df)} || Dropped rows: {dropped_count}"
    )
    # 8.3 Get the appropriate age category.
    df["yob"] = df["yob"].apply(get_age_cat)
    # 8.4 the column named `yob` changed to `age_cat`.
    df.rename(columns={"yob": "age_cat"}, inplace=True)
    print("** column name `yob` changed to `age_cat`.")
    # 9. Replacing `last_split` values with the standard values.
    df["last_split"] = df["last_split"].replace(last_split_std)

    # 10. Reordering the DataFrame columns.
    df = df[cols_order]

    # 11. Convert columns into best possible dtypes (dtypes are inferred).
    df = df.convert_dtypes()

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


def convert_split_dtype(df: pd.DataFrame, splits_keys: list[str]) -> pd.DataFrame:
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


def get_ham_age_cat_dict(df: pd.DataFrame) -> dict:
    """
    ### Function to get the age_cat translation dictionary.
    ----
    ### Arguments:
    + df: The DataFrame.
    ----
    ### Returns a dictionary with the values of `age_cat` as `key` and a value from this list
    `['18-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69', '70-74', '75-79', '80+']` as the `value` pair.
    """
    age_cat_dict = {}
    # Extracting the age range form the age_cat.
    for key in df["age_cat"].unique().tolist():
        match = re.search(r"(?<= |\w)\d{2}(?= |:)|(?<=H)\s(?=1)", key)
        if match:
            age_cat_dict[key] = match.group()
        else:
            age_cat_dict[key] = key

    # Replacing the age range with the age_cat standard values.
    for key, value in age_cat_dict.items():
        match value:
            case " " | "20" | "U20" | "30" | "35":
                age_cat_dict[key] = "18-39"
            case "40":
                age_cat_dict[key] = "40-44"
            case "45":
                age_cat_dict[key] = "45-49"
            case "50":
                age_cat_dict[key] = "50-54"
            case "55":
                age_cat_dict[key] = "55-59"
            case "60":
                age_cat_dict[key] = "60-64"
            case "65":
                age_cat_dict[key] = "65-59"
            case "70":
                age_cat_dict[key] = "70-74"
            case "75":
                age_cat_dict[key] = "75-79"
            case "80" | "85":
                age_cat_dict[key] = "80+"
            case _:
                age_cat_dict[key] = value
    return age_cat_dict


def calc_age(yob: int, cur_year: int) -> int:
    """
    ### Function to calculate the age from the year of birth (i.e yob).
    ----
    ### Arguments:
    + yob: Year of birth.
    + curr_year: THe year to be used to calculate the age.
    ----
    ### Returns the age as int.
    """
    # Checking if the yob is between 2000 and the curr_year.
    if 00 <= yob <= cur_year % 100:
        full_yob = yob + 2000
    else:
        full_yob = yob + 1900

    return cur_year - full_yob


def get_age_cat(age: int) -> str:
    """
    ### Functions to create the appropriate age category based on age; if no age category matches the age will be returned instead.
    N.B Age categories are: `['18-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69', '70-74', '75-79', '80+']`
    ----
    ### Arguments
    + age: The age of the runner.
    ----
    ### Returns The age category based on the age.
    """
    if 18 <= age <= 39:
        return "18-39"
    elif 40 <= age <= 44:
        return "40-44"
    elif 45 <= age <= 49:
        return "45-49"
    elif 50 <= age <= 54:
        return "50-54"
    elif 55 <= age <= 59:
        return "55-59"
    elif 60 <= age <= 64:
        return "60-64"
    elif 65 <= age <= 69:
        return "65-69"
    elif 70 <= age <= 74:
        return "70-74"
    elif 75 <= age <= 79:
        return "75-79"
    elif age >= 80:
        return "80+"
    else:
        return age


def valid_df(df: pd.DataFrame) -> bool:
    """
    ### Function to check if these columns `[age_cat, gender, race_state, last_split]` have the same length.
    ----
    ### Arguments:
    + df: DataFrame to check.
    ----
    ### Returns `True` if they have the same length else `False`.
    """
    if (
        df["age_cat"].count()
        == df["gender"].count()
        == df["race_state"].count()
        == df["last_split"].count()
    ):
        return True
    else:
        print("The DataFrame is not valid !!! |-_-|")
        return False
