import os

import pandas as pd
from pathlib import Path


class GutenbergDataLoader:
    """
    GutenbergDataLoader class to handle loading and preprocessing of datasets.
    """

    def __init__(self, data_dir='sample_dataset', train_csv='final_train.csv',
                 val_csv='final_val.csv', test_csv='final_test.csv', gutenberg_repo_path=None):
        self._data_dir = data_dir

        # If a custom Gutenberg repository path is provided, use it
        if gutenberg_repo_path is not None:
            gutenberg_repo_path = os.path.abspath(gutenberg_repo_path)
        else:
            # Default to the directory adjacent to the gutenberg_corpus_analysis repo
            # This assumes the structure: /<REPOS>/gutenberg_corpus_analysis/
            #                             /<REPOS>/gutenberg/data
            repos_path = Path(__file__).resolve().parent.parent.parent
            gutenberg_repo_path = repos_path / 'gutenberg'

        self._gutenberg_data_path = os.path.join(gutenberg_repo_path, 'data')

        self.train_df = self._load_data_set(train_csv)
        self.val_df = self._load_data_set(val_csv)
        self.test_df = self._load_data_set(test_csv)

    def _load_data_set(self, csv_file):
        """
        Load a dataframe from a CSV file and enrich it with token and word information.
        """
        csv_path = os.path.join(self._data_dir, csv_file)
        df = pd.read_csv(csv_path, index_col='Unnamed: 0')
        df['text'] = df['id'].apply(lambda x: self._get_book(x))


        return df

    # TODO: add the ability to use the gutenberg.data_io.get_book function in order to fetch the book text
    #       This will allow getting the pre-calculated count and token information, if using the original
    #       gutenberg repo (Standardized Project Gutenberg Corpus)
    def _get_book(self, pg_id):
        """
        Fetch the book text using the pg_id."""
        ## location of the gutenberg data
        path_read = os.path.join(self._gutenberg_data_path,'text')
        fname_read = '%s_text.txt'%(pg_id)
        filename = os.path.join(path_read,fname_read)

        if not os.path.exists(filename):
            return None

        with open(filename,'r') as f:
            x = f.readlines()
        text =  ' '.join([h.strip() for h in x])
        return text
