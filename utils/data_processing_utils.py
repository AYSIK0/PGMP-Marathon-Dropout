import pandas as pd
import numpy as np
from pathlib import Path
import re


def london_cleaner(
    df: pd.DataFrame,
    splits_keys: list[str],
    cols_to_drop: list[str],
    cols_order: list[str],
) -> pd.DataFrame:
    """
    ### Function to clean London marathons' data.
    #### `N.B` A copy of the original DataFrame is returned after all operations are performed.
    ----
    ### Arguments:
    + df: DataFrame with data to convert.
    + splits_keys: Name of split columns.
    + cols_to_drop: Name of columns to remove from the DataFrame.
    + cols_order: The order of the returned DataFrame columns, it's a list of columns' names arranged in the desired order.
    ----
    ### Returns a new DataFrame after applying the operations below.
    1. Columns in `cols_to_drop` are removed.
    2. Replacing `['-', ', -, {SPACE}]` with an empty character.
    3. Remove runners that did not start the marathon `race_stat = Not Started` or `all splits' data is null`.
    4. Dropping rows with splits that only contain time.
    5. Dropping runners that do not have a non-null value in these columns `[age_cat, gender]`.
    6. The time and pace for each split in `splits_keys` are converted into seconds.
    7. The time, pace, and speed for each split in `splits_keys` dtype are converted to `Int32`, `Int32`, and`Float32` respectively.
    8. Replacing these age categories `'70-74', '75-79', '80-84', '80+', '85+' by '70+'`
    9. Reordering the DataFrame columns according to cols_order.
    10. Convert columns into best possible dtype using `convert_dtypes()`.
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

    # 4. Dropping rows with splits that only contain time.
    df = drop_rows_with_time_only_splits(df, splits_keys)

    # 5. Dropping runners that have a null value in these columns [age_cat, gender].
    print("** Dropping rows with null values in `age_cat` and `gender` columns:")
    df = drop_null_by_col(df, ["age_cat", "gender"])

    # 6. Converting time and pace into seconds.
    df = convert_to_sec(df, splits_keys)

    # 7. Converting dtype.
    df = convert_split_dtype(df, splits_keys)

    # 8. Replacing these age categories '70-74', '75-79', '80-84', '80+', '85+' by '70+'
    print(
        "** Replacing these age categories '70-74', '75-79', '80-84', '80+', '85+' by '70+'"
    )
    df["age_cat"].replace(
        ["70-74", "75-79", "80-84", "80+", "85+"], "70+", inplace=True
    )

    # 9. Reordering the DataFrame columns.
    df = df[cols_order]

    # 10. Convert columns into best possible dtypes (dtypes are inferred).
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
    8. Dropping rows with splits that only contain time.
    9. Replacing `age_cat` and `last_split` values with the standard values.
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

    # 8. Dropping rows with splits that only contain time.
    df = drop_rows_with_time_only_splits(df, splits_keys)

    # 9. Replacing `age_cat` and `last_split` values with the standard values.
    age_cat_std = get_ham_age_cat_dict(df)
    df["age_cat"] = df["age_cat"].replace(age_cat_std)
    df["last_split"] = df["last_split"].replace(last_split_std)

    # 10. Reordering the DataFrame columns.
    df = df[cols_order]

    # 11. Convert columns into best possible dtypes (dtypes are inferred).
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
    8. Dropping rows with splits that only contain time.
    9. Steps:
    + 9.1 Calculate the age based on the year of birth.
    + 9.2 Dropping any non-adult runner age < 18.
    + 9.3 Get the appropriate age category.
    + 9.4 the column named `yob` changed to `age_cat`.
    10. Replacing `last_split` values with the standard values.
    11. Reordering the DataFrame columns according to cols_order.
    12. Convert columns into best possible dtype using `convert_dtypes()`.
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

    # 7. Adding the `last_split` column and updating `race_state` column.
    df["last_split"] = df.iloc[:, df.columns.str.contains("time")].idxmax(axis=1)
    df["race_state"] = np.where(
        df["last_split"] == "k_finish_time", "Finished", "Started"
    )

    # 8. Dropping rows with splits that only contain time.
    df = drop_rows_with_time_only_splits(df, splits_keys)

    # 9.
    # 9.1 Calculate the age based on the year of birth.
    df["yob"] = df["yob"].apply(calc_age, args=(year,))
    # 9.2 Dropping any non-adult runner age < 18.
    print("** Dropping non-adult runners (age < 18):")
    org_count = len(df)
    df.drop(df[df["yob"] < 18].index, inplace=True)
    dropped_count = org_count - len(df)
    print(
        f"Original rows count: {org_count} || New rows count: {len(df)} || Dropped rows: {dropped_count}"
    )
    # 9.3 Get the appropriate age category.
    df["yob"] = df["yob"].apply(get_age_cat)
    # 9.4 the column named `yob` changed to `age_cat`.
    df.rename(columns={"yob": "age_cat"}, inplace=True)
    print("** column name `yob` changed to `age_cat`.")

    # 10. Replacing `last_split` values with the standard values.
    df["last_split"] = df["last_split"].replace(last_split_std)

    # 11. Reordering the DataFrame columns.
    df = df[cols_order]

    # 12. Convert columns into best possible dtypes (dtypes are inferred).
    df = df.convert_dtypes()

    return df


def boston_cleaner(
    df: pd.DataFrame,
    splits_keys: list[str],
    cols_to_drop: list[str],
    cols_order: list[str],
) -> pd.DataFrame:
    """
    ### Function to clean Boston marathons' data.
    #### `N.B` A copy of the original DataFrame is returned after all operations are performed.
    ----
    ### Arguments:
    + df: DataFrame with data to convert.
    + splits_keys: Name of split columns.
    + cols_to_drop: Name of columns to remove from the DataFrame.
    + cols_order: The order of the columns in the DataFrame, it's a list of columns' names arranged in the desired order.
    ----
    ### Returns a new DataFrame after applying the operations below.
    1. Columns in `cols_to_drop` are removed.
    2. Replacing `['-', ', -, {SPACE}]` with an empty character.
    3. Remove runners that did not start the marathon `race_stat = not started` or `all splits' data is null`.
    4. Dropping rows with splits that only contain time.
    5. Dropping runners that do not have a non-null value in these columns `[age_cat, gender]`.
    6. The time and pace for each split in `splits_keys` are converted into seconds.
    7. The time, pace, and speed for each split in `splits_keys` dtype are converted to `Int32`, `Int32`, and`Float32` respectively.
    8. Convert to pace and speed from sec/mile and miles/h to sec/km and km/h respectively.
    9. Replacing `'70-74', '75-79', '80+'  by '70+'`.
    10. Reordering the DataFrame columns according to cols_order.
    11. Convert columns into best possible dtype using `convert_dtypes()`.
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
    df = df.drop(df.loc[df.race_state == "not started"].index).reset_index(drop=True)
    # 3.2 Runners that do not have any split data will be dropped.
    not_started_indices = df[
        df.iloc[:, df.columns.str.startswith("k_")].isna().values.all(axis=1)
    ].index
    df = df.drop(index=not_started_indices).reset_index(drop=True)
    print(
        f"Original rows count: {rows_count} || New rows count: {len(df)} || Dropped Rows: {rows_count - len(df)}"
    )

    # 4. Dropping rows with splits that only contain time.
    df = drop_rows_with_time_only_splits(df, splits_keys)

    # 5. Dropping runners that have a null value in these columns [age_cat, gender].
    print("** Dropping rows with null values in `age_cat` and `gender` columns:")
    df = drop_null_by_col(df, ["age_cat", "gender"])

    # 6. Converting time and pace into seconds.
    df = convert_to_sec(df, splits_keys)

    # 7. Converting dtype.
    df = convert_split_dtype(df, splits_keys)

    # 8. Convert to pace and speed from sec/mile and miles/h to sec/km and km/h respectively.
    df = convert_pace_and_speed(df, splits_keys)

    # 9. Replacing '70-74', '75-79', '80+'  by '70+'.
    print("** Replacing these age categories '70-74', '75-79', '80+' by '70+'")
    df["age_cat"].replace(["70-74", "75-79", "80+"], "70+", inplace=True)

    # 10. Reordering the DataFrame columns.
    df = df[cols_order]

    # 11. Convert columns into best possible dtypes (dtypes are inferred).
    df = df.convert_dtypes()

    return df


def chicago_cleaner(
    df: pd.DataFrame,
    splits_keys: list[str],
    cols_to_drop: list[str],
    last_split_std: dict,
    cols_order: list[str],
) -> pd.DataFrame:
    """
    ### Function to clean Chicago marathons' data.
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
    7. Adding the `last_split` column and updating `race_state` column.
    8. Dropping rows with splits that only contain time.
    9. Replacing `last_split` values with the standard values.
    10. Removing rows with invalid age categories, `[W-15, M-15, 19 and under]`.
    11.
    + 11.1 Replacing "20-24", "25-29", "30-34", and "35-39" by "18-39".
    + 11.2 Replacing '70-74', '75-79', '80+'  by '70+'.
    12. Convert columns into best possible dtype using `convert_dtypes()`.
    13. Reordering the DataFrame columns according to cols_order.
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

    # 4. Dropping runners that have a null value in these columns [age_cat, gender]
    print("** Dropping rows with null values in `age_cat` and `gender` columns:")
    df = drop_null_by_col(df, ["age_cat", "gender"])

    # 5. Converting time and pace into seconds.
    df = convert_to_sec(df, splits_keys)

    # 6. Converting splits' time, pace, and speed dtype.
    df = convert_split_dtype(df, splits_keys)

    # 7. Adding the `last_split` column and updating `race_state` column.
    df["last_split"] = df.iloc[:, df.columns.str.contains("time")].idxmax(axis=1)
    df["race_state"] = np.where(
        df["last_split"] == "k_finish_time", "Finished", "Started"
    )

    # 8. Dropping rows with splits that only contain time.
    df = drop_rows_with_time_only_splits(df, splits_keys)

    # 9. Replacing `last_split` values with the standard values.
    df["last_split"] = df["last_split"].replace(last_split_std)

    # 10. Removing rows with invalid age categories.
    print("** Dropping rows with invalid age categories [W-15, M-15, 19 and under]:")
    invalid_age_cat_indices = df[
        df["age_cat"].isin({"W-15", "M-15", "19 and under"})
    ].index
    org_count = len(df)
    df = df.drop(invalid_age_cat_indices).reset_index(drop=True)
    dropped_count = org_count - len(df)
    print(
        f"Original rows count: {org_count} || New rows count: {len(df)} || Dropped rows: {dropped_count}"
    )

    # 11.
    # 11.1 Replacing '20-24', '25-29', '30-34', and '35-39' by '18-39' to adhere to the standard age categories.
    print(
        "** Replacing these age categories '20-24', '25-29', '30-34', and '35-39' by '18-39'"
    )
    df["age_cat"].replace(["20-24", "25-29", "30-34", "35-39"], "18-39", inplace=True)
    # 11.2 Replacing '70-74', '75-79', '80+'  by '70+'.
    print("** Replacing these age categories '70-74', '75-79', '80+' by '70+'")
    df["age_cat"].replace(["70-74", "75-79", "80+"], "70+", inplace=True)

    # 12. Reordering the DataFrame columns.
    df = df.convert_dtypes()

    # 13. Convert columns into best possible dtypes (dtypes are inferred).
    df = df[cols_order]

    return df


def houston_cleaner(
    df: pd.DataFrame,
    splits_keys: list[str],
    cols_to_drop: list[str],
    last_split_std: dict,
    cols_order: list[str],
) -> pd.DataFrame:
    """
    ### Function to clean Houston marathons' data.
    #### `N.B` A copy of the original DataFrame is returned after all operations are performed.
    ----
    ### Arguments:
    + df: DataFrame with data to convert.
    + splits_keys: Name of split columns.
    + cols_to_drop: Name of columns to remove from the DataFrame.
    + last_split_std: Standard name for the last split column.
    + cols_order: The order of the columns in the DataFrame.
    ----
    ### Returns a new DataFrame after applying the operations below.
    1. Columns in `cols_to_drop` are removed.
    2. Replacing `['-', ', -, {SPACE}]` with an empty character.
    3. Remove runners that did not start the marathon, `all splits' data is null`.
    4. Dropping runners that do not have a non-null value in these columns `[age_cat, gender]`.
    5. The time and pace for each split in `splits_keys` are converted into seconds.
    6. The time, pace, and speed for each split in `splits_keys` dtype are converted to `Int32`, `Int32`, and`Float32` respectively.
    7. Convert to pace and speed from sec/mile and miles/h to sec/km and km/h respectively.
    8. cleaning age_cat column.
    + 8.1 Removing rows with invalid age categories `[12-15, 16-19, Elites]`.
    + 8.2 Replacing `'20-24', '25-29', '30-34', and '35-39' by '18-39'` to adhere to the standard age categories.
    + 8.3 Replacing these age categories `'70-74', '75-79', '80+' by '70+'`.
    9. Cleaning race_state column.
    + 9.1 Removing rows with invalid race state. `['Other', 'DQ - No Reason Was Given']`
    + 9.2 Replacing race_state values with the "Started" for runners that started the marathon but did not finish.
    10. Dropping rows with splits that only contain time.
    11. Adding last_split column.
    + 11.1 Getting the last_split column values based on the max value in the splits columns.
    + 11.2 Replacing the last_split column values with the standard values.
    12. Reordering the DataFrame columns according to cols_order.
    13. Convert columns into best possible dtype using `convert_dtypes()`.
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
    # df = df.drop(df.loc[df.race_state == "not started"].index).reset_index(drop=True)
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

    # 7. Convert to pace and speed from sec/mile and miles/h to sec/km and km/h respectively.
    df = convert_pace_and_speed(df, splits_keys)

    # 8 Cleaning age_cat column.
    # 8.1 Removing rows with invalid age categories.
    print("** Dropping rows with invalid age categories [12-15, 16-19, Elites]:")
    invalid_age_cat_indices = df[df["age_cat"].isin({"12-15", "16-19", "Elites"})].index
    org_count = len(df)
    df = df.drop(invalid_age_cat_indices).reset_index(drop=True)
    dropped_count = org_count - len(df)
    print(
        f"Original rows count: {org_count} || New rows count: {len(df)} || Dropped rows: {dropped_count}"
    )
    # 8.2. Replacing '20-24', '25-29', '30-34', and '35-39' by '18-39' to adhere to the standard age categories.
    print(
        "** Replacing these age categories '20-24', '25-29', '30-34', and '35-39' by '18-39'"
    )
    df["age_cat"].replace(["20-24", "25-29", "30-34", "35-39"], "18-39", inplace=True)

    # 8.3. Replacing these age categories '70-74', '75-79', '80+' by '70+'
    print("** Replacing these age categories '70-74', '75-79', '80+' by '70+'")
    df["age_cat"].replace(["70-74", "75-79", "80+"], "70+", inplace=True)

    # 9. Cleaning race_state column.
    # 9.1 Removing rows with invalid race state.
    print(
        "** Dropping rows with invalid race state ['Other', 'DQ - No Reason Was Given', 'DQ - SWITCH from HALF to MARA']:"
    )
    invalid_race_state_indices = df[
        df["race_state"].isin(
            {"Other", "DQ - No Reason Was Given", "DQ - SWITCH from HALF to MARA"}
        )
    ].index
    org_count = len(df)
    df = df.drop(invalid_race_state_indices).reset_index(drop=True)
    dropped_count = org_count - len(df)
    print(
        f"Original rows count: {org_count} || New rows count: {len(df)} || Dropped rows: {dropped_count}"
    )
    # 9.2 Replacing race_state values with the "Started" for runners that started the marathon but did not finish.
    df["race_state"].replace(
        ["DNF", "DQ - Over 6h", "DQ - missing split"],
        "Started",
        inplace=True,
    )

    # 10. Dropping rows with splits that only contain time. (N.B 20k split is skipped since Houston did not provide it.)
    df = drop_rows_with_time_only_splits(df, splits_keys, skip_splits=["k_20"])

    # 11. Adding last_split column.
    # 11.1 Getting the last_split column values based on the max value in the splits columns.
    df["last_split"] = df.iloc[:, df.columns.str.contains("time")].idxmax(axis=1)
    # 11.2 Replacing the last_split column values with the standard values.
    df["last_split"] = df["last_split"].replace(last_split_std)

    # 12. Reordering the DataFrame columns.
    df = df[cols_order]

    # 13. Convert columns into best possible dtypes (dtypes are inferred).
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
    #### N.B The function only check split columns, their name start with `k_`.
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
    ####  N.B The function will create the path if it does not exist.
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
    ### Function to get the age_cat translation dictionary for hamburg marathon.
    ----
    ### Arguments:
    + df: The DataFrame.
    ----
    ### Returns a dictionary with the values of `age_cat` as `key` and a value from this list
    `['18-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69', '70+']` as the `value` pair.
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
            case " " | "20" | "U20" | "30" | "35" | "JA":
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
                age_cat_dict[key] = "65-69"
            case "70" | "75" | "80" | "85":
                age_cat_dict[key] = "70+"
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
    elif age >= 70:
        return "70+"
    else:
        return age


def valid_df(df: pd.DataFrame) -> bool:
    """
    ### Function to check if the DataFrame is valid.
    + These columns `[age_cat, gender, race_state, last_split]` must have the same length.
    + The `age_cat` column must only have these values `['18-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69', '70+']`.
    ----
    ### Arguments:
    + df: DataFrame to check.
    ----
    ### Returns `True` if the DataFrame is valid else `False`.
    """
    assert set(df["age_cat"].unique().tolist()) == {
        "18-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60-64",
        "65-69",
        "70+",
    }, f"The `age_cat` column must only have these values ['18-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69', '70+'], found: {df['age_cat'].unique().tolist()}"
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


def convert_pace(mile_pace: int | float) -> np.float64:
    """
    ### Function to convert the pace form sec/mile to sec/km.
    ----
    ### Arguments:
    + mile_pace: The pace in sec/mile.
    ----
    ### Returns: the pace in sec/km.
    """
    return np.round(mile_pace / 1.609)


def convert_speed(mile_speed: float) -> np.float64:
    """
    ### Function to convert the speed form miles/h to km/h.
    ----
    ### Arguments:
    + mile_pace: The speed in miles/h.
    ----
    ### Returns: the speed in km/h.
    """
    return np.round(mile_speed * 1.60934, decimals=2)


def convert_pace_and_speed(df: pd.DataFrame, splits_keys: list[str]) -> pd.DataFrame:
    """
    ### Function to convert the splits' pace, and speed from sec/mile and miles/h to sec/km and km/h.
    ----
    ### Arguments:
    + df: DataFrame with data to convert.
    + splits_keys: Name of split columns.
    ----
    ### Returns the DataFrame.
    """
    for key in splits_keys:
        df[f"{key}_pace"] = df[f"{key}_pace"].map(convert_pace, na_action="ignore")
        df[f"{key}_speed"] = df[f"{key}_speed"].map(convert_speed, na_action="ignore")
    return df


def get_indices_of_rows_with_only_time(
    df: pd.DataFrame,
    splits_names: list[str],
    skip_splits: list[str] = None,
    return_indices_list: list[int] = True,
) -> dict[str, pd.Index]:
    """
    ### Returns the indices of rows that have time.
    ----
    Arguments:
    + df: The DataFrame to be used.
    + splits_names: The names of the splits columns.
    + skip_splits: The names of the splits columns to skip.
    + return_indices_list: If True the function will return a list of indices else it will return a dictionary of indices.
    ----
    Returns:
    + If `return_indices_list` is True the function will return a list of indices.
    + If `return_indices_list` is False the function will return a dictionary of indices.
    """
    # Turning the `skip_splits` list into a set.
    skip_splits = set(skip_splits) if skip_splits else set()
    split_dict = {}
    # Getting the indices of rows that have time.
    for split in splits_names:
        if split in skip_splits:
            continue
        split_dict[split] = df[
            df[f"{split}_time"].notnull()
            & df[f"{split}_pace"].isnull()
            & df[f"{split}_speed"].isnull()
        ].index
    if return_indices_list:
        return list({index for indices in split_dict.values() for index in indices})
    return split_dict


def drop_rows_with_time_only_splits(
    df: pd.DataFrame,
    splits_names: list[str],
    skip_splits: list[str] = None,
    return_indices_list: list[int] = True,
) -> pd.DataFrame:
    """
    ### Returns the DataFrame after dropping the rows that have time only in the splits columns.
    ----
    Arguments:
    + df: The DataFrame to be used.
    + splits_names: The names of the splits columns.
    + skip_splits: The names of the splits columns to skip.
    + return_indices_list: If True the function will return a list of indices else it will return a dictionary of indices.
    ----
    Returns:
    + The DataFrame after dropping the rows that have time only in the splits columns.
    """
    miss_indices = get_indices_of_rows_with_only_time(
        df, splits_names, skip_splits, return_indices_list
    )
    finished_count = df.loc[
        (df.index.isin(miss_indices)) & (df["race_state"] == "Finished")
    ].shape[0]
    started_count = df.loc[
        (df.index.isin(miss_indices)) & (df["race_state"] == "Started")
    ].shape[0]
    print(
        f"** Dropping rows with splits that only contain time: Finished: {finished_count} || Started: {started_count}"
    )
    df = df.drop(index=miss_indices).reset_index(drop=True)
    return df
