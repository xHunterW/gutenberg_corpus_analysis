import os

import pandas as pd


class GutenbergDataLoader:
    """
    GutenbergDataLoader class to handle loading and preprocessing of datasets.
    """

    def __init__(self, data_dir='sample_dataset', train_csv='final_train.csv',
                 val_csv='final_val.csv', test_csv='final_test.csv', gutenberg_repo_path=None):
        self._data_dir = data_dir

        self.train_df = self._load_data_set(train_csv)
        self.val_df = self._load_data_set(val_csv)
        self.test_df = self._load_data_set(test_csv)

    def _load_data_set(self, csv_file):
        """
        Load a dataframe from a CSV file and enrich it with token and word information.
        """
        csv_path = os.path.join(self._data_dir, csv_file)
        df = pd.read_csv(csv_path, index_col='Unnamed: 0')
