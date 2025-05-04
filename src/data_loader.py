import os

import pandas as pd

def load_dfs(dataset_folder='sample_dataset', train_csv='final_train.csv', 
             val_csv='final_val.csv', test_csv='final_test.csv'):
    """
    Load the dataframes from the specified paths.
    """
    # Load the dataframes
    train_csv = os.path.join(dataset_folder, 'final_train.csv')
    val_csv = os.path.join(dataset_folder, 'final_val.csv')
    test_csv = os.path.join(dataset_folder, 'final_test.csv')

    print(os.path.abspath(train_csv))

    train_df = pd.read_csv(train_csv, index_col='Unnamed: 0')
    val_df = pd.read_csv(val_csv, index_col='Unnamed: 0')
    test_df = pd.read_csv(test_csv, index_col='Unnamed: 0')

    return train_df, val_df, test_df