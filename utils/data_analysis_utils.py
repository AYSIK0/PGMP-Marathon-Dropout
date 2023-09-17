import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def plot_feature_skewness(
    df: pd.DataFrame, split_data: str = "time", fig_size: tuple[int, int] = (10, 5)
) -> None:
    """
    ### Plot the skewness of the features.
    ----
    ### Arguments:
    + df: The DataFrame to plot the skewness of.
    + split_data: The split data to plot the skewness of (either `time`, `pace`, or `speed`).
    + fig_size: The size of the figure.
    ----
    ### Returns:
    + None
    """
    plt.figure(figsize=fig_size)
    df.filter(regex=f"^k_.*_{split_data}$").skew().plot(
        kind="bar",
        color="teal",
        rot=45,
    )
    plt.title(f"Skewness of Split {split_data.capitalize()}")
    plt.ylabel("Skewness Value")
    plt.xlabel("Feature Name")
    plt.xticks(rotation=45)
    plt.grid(axis="y", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.show()


def plot_runners_dist(df: pd.DataFrame, fig_size: tuple[int, int] = (12, 10)) -> None:
    """
    ### Plot runners distribution based on Runner Type, Age Category, and Gender.
    ----
    ### Arguments:
    + df: The DataFrame to plot the runner distribution of.
    + fig_size: The size of the figure.
    ----
    ### Returns:
    + None
    """
    plt.figure(figsize=fig_size)

    # Distribution of runners based on runner_type
    plt.subplot(2, 2, 1)
    sns.countplot(
        data=df,
        x="runner_type",
        order=df["runner_type"].value_counts().index,
        palette="viridis",
    )
    plt.title("Distribution of Runners by Type")
    plt.xticks(rotation=45)
    plt.ylabel("Number of Runners")

    # Distribution of runners based on age category
    plt.subplot(2, 2, 2)
    sns.countplot(
        data=df,
        x="age_cat",
        order=df["age_cat"].value_counts().index,
        palette="viridis",
    )
    plt.title("Distribution of Runners by Age Category")
    plt.xticks(rotation=45)
    plt.ylabel("Number of Runners")

    # Distribution of runners based on gender
    plt.subplot(2, 2, 3)
    sns.countplot(data=df, x="gender", palette="cool")
    plt.title("Distribution of Runners by Gender")
    plt.ylabel("Number of Runners")

    plt.tight_layout()
    plt.show()


def plot_splits_feature_dist(
    df: pd.DataFrame, split_data: str, fig_size: tuple[int, int] = (15, 10)
) -> None:
    """
    ### Plot the distribution of the splits.
    ----
    ### Arguments:
    + df: The DataFrame to plot the distribution of.
    + split_data: The split data to plot the distribution of (either `time`, `pace`, or `speed`).
    + fig_size: The size of the figure.
    ----
    ### Returns:
    + None
    """
    assert split_data in [
        "time",
        "pace",
        "speed",
    ], "split_data must be either `time`, `pace`, or `speed`."
    unit_dict = {"time": "seconds", "pace": "sec/km", "speed": "km/h"}
    plt.figure(figsize=(15, 10))
    # Selecting the columns related to the split data
    subset = df.filter(regex=f"k_.*_{split_data}")
    # Getting the columns names.
    features_to_check = subset.columns.to_list()
    # Plotting the distribution of each split
    for i, feature in enumerate(features_to_check, 1):
        plt.subplot(5, 2, i)
        sns.histplot(data=subset, x=feature, bins=50, kde=True)
        plt.title(f"Distribution of {feature}")
        plt.xlabel(f"{feature.capitalize()} (in {unit_dict[split_data]})")
        plt.ylabel("Number of Runners")

    plt.tight_layout()
    plt.show()


def plot_split_data_trend(
    df: pd.DataFrame, split_data: str = "time", fig_size: tuple[int, int] = (8, 5)
) -> None:
    """
    ### Plot the trend of the splits.
    ----
    ### Arguments:
    + df: The DataFrame to plot the trend of.
    + split_data: The split data to plot the trend of (either `time`, `pace`, or `speed`).
    + fig_size: The size of the figure.
    ----
    ### Returns:
    + None
    """
    assert split_data in [
        "time",
        "pace",
        "speed",
    ], "split_data must be either `time`, `pace`, or `speed`."
    unit_dict = {"time": "seconds", "pace": "sec/km", "speed": "km/h"}
    cols = [col for col in df.columns if f"_{split_data}" in col]
    # Calculating the average split times.
    avg_split_data = df.filter(regex=f"k_.*_{split_data}").mean()
    # Calculating non-cumulative split times.
    if split_data == "time":
        non_cumulative_times = avg_split_data.diff()
        non_cumulative_times[cols[0]] = avg_split_data[
            cols[0]
        ]  # First split remains the same
        data = non_cumulative_times
    else:
        data = avg_split_data

    plt.figure(figsize=fig_size)
    plt.plot(data.index, data.values, marker="o", linestyle="-", color="teal")
    if split_data == "time":
        plt.title("Non-Cumulative Average Split Time Across Race Progression")
        plt.ylabel("Non-Cumulative Average Time (in seconds)")
    else:
        plt.title(f"Average Split {split_data} Across Race Progression")
    plt.xlabel("Race Splits")
    plt.ylabel(f"Average {split_data} (in {unit_dict[split_data]})")
    plt.xticks(rotation=45)
    plt.grid(True, linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.show()


def plot_split_data_trend_by_cat(
    df: pd.DataFrame, split_data: str, fig_size: tuple[int, int] = (15, 10)
) -> None:
    """
    ### Plot the trend of the splits by these categories (`runner_type`, `age_cat`, or `gender`)
    ----
    ### Arguments:
    + df: The DataFrame to plot the trend of.
    + split_data: The split data to plot the trend of (either `time`, `pace`, or `speed`).
    + fig_size: The size of the figure.
    ----
    ### Returns:
    + None
    """
    assert split_data in [
        "time",
        "pace",
        "speed",
    ], "split_data must be either `time`, `pace`, or `speed`."

    unit_dict = {"time": "seconds", "pace": "sec/km", "speed": "km/h"}

    plt.figure(figsize=fig_size)

    category = "gender"
    cols = [col for col in df.columns if f"_{split_data}" in col]
    grouped_cat_type = df.groupby(category)[cols].mean().transpose()
    plt.subplot(2, 2, 1)
    for cat_type in grouped_cat_type.columns:
        plt.plot(
            grouped_cat_type.index,
            grouped_cat_type[cat_type],
            marker="o",
            label=cat_type,
        )
    plt.title(f"Average {split_data} Across Race Progression by Gender")
    plt.xlabel("Race Splits")
    plt.ylabel(f"Average {split_data} (in {unit_dict[split_data]})")
    plt.xticks(rotation=45)
    plt.legend(loc="best")
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()

    category = "age_cat"
    cols = [col for col in df.columns if f"_{split_data}" in col]
    grouped_cat_type = df.groupby(category)[cols].mean().transpose()
    plt.subplot(2, 2, 2)
    for cat_type in grouped_cat_type.columns:
        plt.plot(
            grouped_cat_type.index,
            grouped_cat_type[cat_type],
            marker="o",
            label=cat_type,
        )

    plt.title(f"Average {split_data} Across Race Progression by Age Category")
    plt.xlabel("Race Splits")
    plt.ylabel(f"Average {split_data} {unit_dict[split_data]}")
    plt.xticks(rotation=45)
    plt.legend(loc="best")
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()

    category = "runner_type"
    cols = [col for col in df.columns if f"_{split_data}" in col]
    grouped_cat_type = df.groupby(category)[cols].mean().transpose()
    plt.subplot(2, 2, 3)
    for cat_type in grouped_cat_type.columns:
        plt.plot(
            grouped_cat_type.index,
            grouped_cat_type[cat_type],
            marker="o",
            label=cat_type,
        )
    plt.title(f"Average {split_data} Across Race Progression by Runner Type")
    plt.xlabel("Race Splits")
    plt.ylabel(f"Average {split_data} {unit_dict[split_data]}")
    plt.xticks(rotation=45)
    plt.legend(loc="lower left", bbox_to_anchor=(0.95, 0))
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()

    plt.show()


def plot_non_finishers_by_cat(
    df: pd.DataFrame, category: str, fig_size: tuple[int, int] = (10, 5)
) -> None:
    """
    ### Plot the distribution of non-finishers by these categories (`age_cat`, `gender`, or `runner_type`).
    ----
    ### Arguments:
    + df: The DataFrame to plot the distribution of.
    + category: The category to plot the distribution of (either `age_cat`, `gender`, or `runner_type`).
    + fig_size: The size of the figure.
    ----
    ### Returns:
    + None
    """
    # Check the category.
    assert category in [
        "age_cat",
        "gender",
        "runner_type",
    ], "category must be either `age_cat`, `gender`, or `runner_type`."
    # Select non-finishers.
    data = df[df["race_state"] == "Started"]
    plt.figure(figsize=fig_size)
    sns.countplot(
        data=data,
        x=category,
        hue="race_state",
        palette="YlGnBu",
        order=data[category].value_counts().index,
    )
    plt.title(f"Distribution of Non-Finishers by {category}")
    plt.xlabel(f"{category.capitalize()}")
    plt.ylabel("Number of Non-Finishers")
    plt.legend().remove()
    plt.show()
