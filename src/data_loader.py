import os

import pandas as pd
from pathlib import Path


class GutenbergDataLoader:
    """
    GutenbergDataLoader class to handle loading and preprocessing of datasets.
    """

    def __init__(self, data_dir='sample_dataset',
                 train_csv='final_train.csv', val_csv='final_val.csv', test_csv='final_test.csv',
                 gutenberg_repo_path=None, enrich_df=True):

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

        if enrich_df:
            self.train_df = self._enrich_dataframe(self.train_df)
            self.val_df = self._enrich_dataframe(self.val_df)
            self.test_df = self._enrich_dataframe(self.test_df)

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

    def _enrich_dataframe(self, df):
        """
        Enrich the dataframe with word and token counts.
        """
        count_path = os.path.join(self._gutenberg_data_path, 'counts')
        text_path = os.path.join(self._gutenberg_data_path, 'text')
        token_path = os.path.join(self._gutenberg_data_path, 'tokens')

        df['word_count'] = df['id'].apply(lambda pid: self._get_word_count(pid, count_path))
        df['unique_word_count'] = df['id'].apply(lambda pid: self._get_unique_word_count(pid, count_path))
        df['line_count'] = df['id'].apply(lambda pid: self._get_line_count(pid, text_path))
        df['token_count'] = df['id'].apply(lambda pid: self._get_token_count(pid, token_path))

        return df

    def _get_word_count(self, book_id, raw_text_dir):
        """
        Given something like 'PG10007' and a directory containing a file called
        'PG10007_counts.txt' whose lines each have a word and a count, sum up
        all those counts and return the total.
        """
        filename = f"{book_id}_counts.txt"
        file_path = os.path.join(raw_text_dir, filename)

        # If the file doesn’t exist, return None
        if not os.path.exists(file_path):
            return None

        total_count = 0
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Each line looks like: word count
                word, count_str = line.strip().split()
                total_count += int(count_str)

        return total_count

    def _get_unique_word_count(self, book_id, raw_text_dir):
        """
        Given something like 'PG10007' and a directory containing a file
        called 'PG10007_counts.txt' whose lines each have 'word count',
        return how many lines that file has (i.e., how many unique words).
        """
        filename = f"{book_id}_counts.txt"
        file_path = os.path.join(raw_text_dir, filename)

        if not os.path.exists(file_path):
            return None

        # Count lines to get # of unique words
        with open(file_path, 'r', encoding='utf-8') as f:
            num_unique_words = sum(1 for _ in f)

        return num_unique_words

    def _get_line_count(self, book_id, text_dir):
        """
        Given something like 'PG10007' and a directory containing
        'PG10007_text.txt', return how many lines are in the file.
        """
        filename = f"{book_id}_text.txt"
        file_path = os.path.join(text_dir, filename)

        # If the file doesn’t exist, return None (or 0)
        if not os.path.exists(file_path):
            return None

        # Count lines
        with open(file_path, 'r', encoding='utf-8') as f:
            line_count = sum(1 for _ in f)

        return line_count

    def _get_token_count(self, book_id, text_dir):
        """
        Given something like 'PG10007' and a directory containing
        'PG10007_tokens.txt', return how many lines are in the file.
        """
        filename = f"{book_id}_tokens.txt"
        file_path = os.path.join(text_dir, filename)

        # If the file doesn’t exist, return None (or 0)
        if not os.path.exists(file_path):
            return None

        # Count lines
        with open(file_path, 'r', encoding='utf-8') as f:
            token_count = sum(1 for _ in f)

        return token_count