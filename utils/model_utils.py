import os
from joblib import dump, load
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix
import seaborn as sns
import pandas as pd
from sklearn.preprocessing import (
    PowerTransformer,
    RobustScaler,
    StandardScaler,
    OneHotEncoder,
)
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline as skpipeline
from sklearn.model_selection import train_test_split
from pprint import pprint


def load_and_concat_all_data(
    marathon_name: str,
    parent_path: str,
    years: list[str],
    dataset_name: str = None,
    dataset_type: str = "impute",
    dtype=None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | pd.DataFrame:
    """
    ### Load and concatenate all the DataFrames.
    ----
    ### Arguments:
    + marathon_name: The name of the marathon.
    + parent_path: The path to load the DataFrames.
    + years: The years of the marathon.
    + dataset_name: The name of the dataset to return.
    + dtype: The data types of the DataFrame.
    + dataset_type: The type of the dataset, either the imputed `impute` or the extended `ext`.
    ----
    ### Returns:
    + data: The DataFrames, if `dataset_name` is None.
    + The specified DataFrame, if `dataset_name` is not None.
    """
    # Check the dataset type.
    assert dataset_type in [
        "impute",
        "ext",
    ], "dataset_type must be either `impute` or `ext`."

    data = [[], []]
    for year in years:
        knn_path = f"{parent_path}/{marathon_name}{year}/{marathon_name}{year}_knn_{dataset_type}.csv"
        iter_path = f"{parent_path}/{marathon_name}{year}/{marathon_name}{year}_iter_{dataset_type}.csv"
        if os.path.isfile(knn_path) and os.path.isfile(iter_path):
            data[0].append(pd.read_csv(knn_path, dtype=dtype))
            data[1].append(pd.read_csv(iter_path, dtype=dtype))
        else:
            print(f"DataFrame not found at {knn_path} or {iter_path}.")
            continue
    # Concatenate all data
    data[0] = pd.concat(data[0], ignore_index=True)
    data[1] = pd.concat(data[1], ignore_index=True)
    if dataset_name:
        match dataset_name:
            case "knn":
                return data[0]
            case "iter":
                return data[1]
    return data


def get_preprocessed_datasets(
    df: pd.DataFrame,
    cols_order: list[str],
    splits_cols: list[str],
    test_split: float = 0.2,
    val_split: float = 0.25,
    random_state: int = 17,
    return_cols: bool = False,
    last_split: int = 10,
    return_full_data: bool = False,
) -> tuple[
    tuple[np.ndarray, np.ndarray],
    tuple[np.ndarray, np.ndarray],
    tuple[np.ndarray, np.ndarray],
]:
    """
    ### Get the preprocessed datasets.
    ----
    ### Arguments:
    + df: The DataFrame.
    + cols_order: The columns order.
    + splits_cols: The splits columns.
    + test_split: The test split.
    + val_split: The validation split.
    + random_state: The random state.
    + return_cols: Whether to return the columns or not.
    + last_split: The last split to be used. `None` means all splits will be used.
    ['5k': 1, '10k': 2, '15k': 3, '20k': 4, 'half': 5, '25k': 6, '30k': 7, '35k': 8, '40k': 9, 'finish': 10]
    + return_full_data: Whether to return the full data before splitting or not.
    ----
    ### Returns:
    + (X_train, y_train): The training set.
    + (X_test, y_test): The test set.
    + (X_val, y_val): The validation set.
    """
    # Transform skewed features.
    tmp_df = transform_skewed_features(df, cols_order)

    # Transform the columns.
    tmp_df = transform_cols(tmp_df, splits_cols)

    # Define the columns to be used to train the model.
    cols_names = tmp_df.columns.to_list()
    # N.B Set could be used instead of list, but the order of the columns will be lost, plus the list is small.
    # Only one gender column is used, since it is a binary column.
    non_split_cols = [
        col for col in cols_names if col not in splits_cols and col != "gender_M"
    ]
    # the speed cols are not used since they are highly correlated with the pace cols.
    used_split_cols = [
        item for item in cols_names if "_time" in item or "_pace" in item
    ][: last_split * 2]
    used_cols = non_split_cols + used_split_cols
    pprint(f"The columns to be used: {used_cols}")

    # Main DataFrame.
    X = tmp_df[used_cols]

    # Encoding the target variable.
    y = df["race_state"].apply(lambda x: 1 if x == "Started" else 0).to_numpy()

    # Splitting The data into train, validation and test sets.
    X_train, X_test, y_train, y_test = train_test_split(
        X.to_numpy(), y, test_size=test_split, random_state=random_state, stratify=y
    )
    X_train, X_val, y_train, y_val = train_test_split(
        X_train,
        y_train,
        test_size=val_split,
        random_state=random_state,
        stratify=y_train,
    )
    if return_full_data:
        return (X, y)
    elif return_cols:
        return (X_train, y_train), (X_test, y_test), (X_val, y_val), used_cols
    return (X_train, y_train), (X_test, y_test), (X_val, y_val)


def transform_skewed_features(
    df: pd.DataFrame, cols_order: str, min: float = -0.1, max: float = 0.1
) -> pd.DataFrame:
    """
    ### Transform skewed features, using the Yeo-Johnson transformation, all features with a skewness value greater than 0.1 or less than -0.1 will be transformed.
    ----
    ### Arguments:
    + df: The DataFrame.
    + min: The minimum skewness value.
    + max: The maximum skewness value.
    ----
    ### Returns:
    + df: The DataFrame with the transformed features.
    """
    print("Transforming skewed features...\n")
    # Get the skewed features.
    skewness = df.skew(numeric_only=True).sort_values(ascending=False)
    feats = skewness[(skewness > max) | (skewness < min)].index
    # Transform the features.
    yeojohn_tr = PowerTransformer(standardize=False)
    df_transformed = pd.DataFrame(yeojohn_tr.fit_transform(df[feats]), columns=feats)
    # Concatinating the transformed features with the rest of the data.
    df = pd.concat([df.drop(feats, axis=1), df_transformed], axis=1)
    df = df[cols_order]
    return df


def transform_cols(
    df: pd.DataFrame, splits_cols: list[str], task_type: str = "class"
) -> pd.DataFrame:
    """
    ### Transform the columns, using the ColumnTransformer, the splits columns will be scaled using the RobustScaler, \
    and the rest of the columns will be scaled using the StandardScaler, and the categorical columns will be encoded using the OneHotEncoder.
    ----
    ### Arguments:
    + df: The DataFrame.
    + splits_cols: The splits columns.
    + task_type: The task type, either classification `class` or regression `reg`.
    ----
    ### Returns:
    + df: The DataFrame with the transformed columns.
    """

    # Define the pipelines.
    splits_pipeline = skpipeline([("splits_scale", RobustScaler())])
    num_pipeline = skpipeline([("num_scale", StandardScaler())])
    cat_pipeline = skpipeline([("encode", OneHotEncoder())])

    # Define the columns.
    # These columns were selected since they will be scaled using the standard scaler.
    if task_type == "class":
        num_cols = [
            "daily_min",
            "daily_max",
            "medium_temp",
            "avg_humidity",
            "avg_barometer",
            "avg_windspeed",
            "5k_dnf_pct",
            "10k_dnf_pct",
            "15k_dnf_pct",
            "20k_dnf_pct",
            "half_dnf_pct",
            "25k_dnf_pct",
            "30k_dnf_pct",
            "35k_dnf_pct",
            "40k_dnf_pct",
        ]
    elif task_type == "reg":
        num_cols = [
            "daily_min",
            "daily_max",
            "medium_temp",
            "avg_humidity",
            "avg_barometer",
            "avg_windspeed",
        ]
    else:
        raise ValueError("task_type must be either `class` or `reg`.")

    cat_cols = ["age_cat", "gender", "runner_type"]
    cols_transformer = ColumnTransformer(
        [
            ("splits_pipeline", splits_pipeline, splits_cols),
            ("num_pipeline", num_pipeline, num_cols),
            ("cat_pipeline", cat_pipeline, cat_cols),
        ],
        n_jobs=-1,
    )

    # Transform the columns.
    print("Transforming columns (oneHotEncoding and Scaling)...\n")
    df = cols_transformer.fit_transform(df)
    cols_names = [
        col.split("__")[1] for col in cols_transformer.get_feature_names_out()
    ]
    # Create a DataFrame, with the transformed columns.
    df = pd.DataFrame(df, columns=cols_names)

    return df


def save_sklearn_model(model, model_name: str, model_path: str) -> None:
    """
    ### Save the sklearn model.
    ----
    ### Arguments:
    + model: The model.
    + model_name: The model name.
    + model_path: The model path.
    ----
    ### Returns:
    + None
    """
    if os.path.exists(model_path):
        dump(model, f"{model_path}{model_name}.joblib")
    else:
        os.makedirs(model_path)
        dump(model, f"{model_path}{model_name}.joblib")
    print(f"Model saved to {model_path}{model_name}.joblib")


def load_sklearn_model(model_name: str, model_path: str):
    """
    ### Load the sklearn model.
    ----
    ### Arguments:
    + model_name: The model name.
    + model_path: The model path.
    ----
    ### Returns:
    + model: The model, if it exists.
    """
    if os.path.isfile(f"{model_path}{model_name}.joblib"):
        model = load(f"{model_path}{model_name}.joblib")
        print(f"Model loaded from {model_path}{model_name}.joblib")
        return model
    else:
        print(f"Model not found at {model_path}{model_name}.joblib")
        return None


def plot_confusion_matrix(
    y_true: list[int | float],
    y_pred: list[int | float],
    class_names: list[str],
    title="Confusion Matrix",
    cmap=plt.cm.Blues,
    figsize=(10, 7),
) -> None:
    """
    ### This function prints and plots the confusion matrix.
    ----
    ### Arguments:
    + y_true: The true labels.
    + y_pred: The predicted labels.
    + class_names: The class names.
    + title: The title of the plot.
    + cmap: The color map.
    + figsize: The figure size.
    ----
    ### Returns:
    + None
    """
    # Compute confusion matrix
    conf_matrix = confusion_matrix(y_true, y_pred)
    # Calculate the count and percentages of each category.
    cat_count = [v for v in conf_matrix.flatten()]
    cat_pct = [f"{value:.2%}" for value in conf_matrix.flatten() / np.sum(conf_matrix)]
    # Create the annotations by concatenating the count and percentages.
    annot = [f"{co}\n{pc}" for co, pc in zip(cat_count, cat_pct)]
    annot = np.asarray(annot).reshape(conf_matrix.shape)

    # Plot the confusion matrix.
    plt.figure(figsize=figsize)
    sns.heatmap(
        conf_matrix,
        annot=annot,
        fmt="",
        cmap=cmap,
        xticklabels=class_names,
        yticklabels=class_names,
    )
    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.title(title)
    plt.show()
