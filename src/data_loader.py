import os
import random
from pathlib import Path

import pandas as pd
import numpy as np
from tqdm.contrib.concurrent import process_map
from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet as wn
from collections import defaultdict




class GutenbergDataLoader:
    """
    GutenbergDataLoader class to handle loading and preprocessing of datasets.
    """

    def __init__(self, data_dir='sample_dataset',
                 train_csv='final_train.csv', val_csv='final_val.csv', test_csv='final_test.csv',
                 gutenberg_repo_path=None, enrich_df=True, skip_first_and_last_words=100, num_threads=None):

        self._data_dir = data_dir
        self._num_threads = num_threads
        if num_threads is None:
            self._num_threads = os.cpu_count() - 1

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

        self.train_df = self._load_data_set(train_csv, skip_first_and_last_words)
        self.val_df = self._load_data_set(val_csv, skip_first_and_last_words)
        self.test_df = self._load_data_set(test_csv, skip_first_and_last_words)

        if enrich_df:
            self.train_df = self._enrich_dataframe(self.train_df)
            self.val_df = self._enrich_dataframe(self.val_df)
            self.test_df = self._enrich_dataframe(self.test_df)

        self._tokenize_all_text(self)

    def _load_data_set(self, csv_file, skip_first_and_last_words=100):
        """
        Load a dataframe from a CSV file and enrich it with token and word information.
        """
        csv_path = os.path.join(self._data_dir, csv_file)
        df = pd.read_csv(csv_path, index_col='Unnamed: 0')
        df['text'] = df['id'].apply(lambda x: self._get_book(x, skip_first_and_last_words=100))

        return df

    # TODO: add the ability to use the gutenberg.data_io.get_book function in order to fetch the book text
    #       This will allow getting the pre-calculated count and token information, if using the original
    #       gutenberg repo (Standardized Project Gutenberg Corpus)
    def _get_book(self, pg_id, skip_first_and_last_words=100):
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

        # Skip the first and last N words (technically there might be a few spaces in there)
        if skip_first_and_last_words > 0:
            text = text.split(' ')
            text = text[skip_first_and_last_words:-skip_first_and_last_words]
            text = ' '.join(text)

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

    def _random_chunk_one_text(self, text, num_chunks=10, chunk_size=1000, overlap=False):
        chunk = []
        words = text.split(' ')

        # If the total number of words is less than the chunk size * number of chunks, return the text
        if num_chunks * chunk_size > len(words):
            return text

        for i in range(num_chunks):
            num_words = len(words)
            # If the chunk is larger than the number of words, return the rest of the text
            if chunk_size > num_words:
                chunk = chunk + words
                words = []
                return ' '.join(chunk)

            start = random.randint(0, num_words)
            chunk = [*chunk,  *words[start:start+chunk_size]]
            if start == 0:
                words = words[chunk_size:]
            elif start == num_words - chunk_size:
                words = words[0:start]
            else:
                words = words[0:start] + words[start+chunk_size:]
        return ' '.join(chunk)

    def random_chunk_all_text(self, num_chunks=10, chunk_size=1000, overlap=False):
        """
        Randomly chunk the text in the train, validation, and test dataframes.
        """
        # Randomly chunk the text in the train, validation, and test dataframes
        self.train_df['text'] = self.train_df['text'].apply(
            lambda x: self._random_chunk_one_text(x, num_chunks, chunk_size, overlap))
        self.test_df['text'] = self.test_df['text'].apply(
            lambda x: self._random_chunk_one_text(x, num_chunks, chunk_size, overlap))
        self.val_df['text'] = self.val_df['text'].apply(
            lambda x: self._random_chunk_one_text(x, num_chunks, chunk_size, overlap))

    def parse_subjects(df):
        subj = df['subjects'].replace('set()', np.nan)
        subj_docs = []
        for h in subj:
            try:
                h = h.strip("{}")[1:-1]
            except AttributeError:
                subj_docs.append(h)
                continue
            h = h.replace(' -- ', '-')
            h = h.replace("', '","_")
            h = h.split('_')
            h = [item.replace(' ','').replace(',', ' ') for item in h]
            h = ' '.join(h)
            subj_docs.append(h)

        df['subj_str']=subj_docs
        return df

    def _tokenize_all_text(self):
        """
        Tokenize all text in the train, validation, and test dataframes.
        """
        # Tokenize the text in the train, validation, and test dataframes
        tokenized = process_map(word_tokenize, self.train_df['text'], max_workers=self._num_threads, chunksize=5)
        self.train_df['tokenized'] = tokenized
        tokenized = process_map(word_tokenize, self.val_df['text'], max_workers=self._num_threads, chunksize=5)
        self.val_df['tokenized'] = tokenized
        tokenized = process_map(word_tokenize, self.test_df['text'], max_workers=self._num_threads, chunksize=5)
        self.test_df['tokenized'] = tokenized

        # Check for null values in the tokenized columns
        if self.train_df['tokenized'].isnull().any():
            print('Warning: There are null elements in train_df')

        if self.val_df['tokenized'].isnull().any():
            print('Warning: There are null elements in val_df')

        if self.test_df['tokenized'].isnull().any():
            print('Warning: There are null elements in test_df')

    def _lemmatize_text(self, tokenized_text):
        tag_map = defaultdict(lambda : wn.NOUN)
        tag_map['J'] = wn.ADJ
        tag_map['V'] = wn.VERB
        tag_map['R'] = wn.ADV
        # Do I need to add noun?

        # Declaring Empty List to store the words that follow the rules for this step
        final_words = []
        # Initializing WordNetLemmatizer()
        word_lemmatized = WordNetLemmatizer()
        # pos_tag function below will provide the 'tag' i.e if the word is Noun(N) or Verb(V) or something else.
        for word, tag in pos_tag(tokenized_text):
            # Below condition is to check for Stop words and consider only alphabets
            if word not in stopwords.words('english') and word.isalpha():
                word_final = word_lemmatized.lemmatize(word,tag_map[tag[0]])
                final_words.append(word_final)
        return str(final_words)
        # The final processed set of words for each iteration will be stored in 'text_final'

    def lemmatize_all_text(self):
        """
        Lemmatize all text in the train, validation, and test dataframes.
        """
        # Lemmatize the text in the train, validation, and test dataframes
        # This function uses the process_map function from tqdm to parallelize the lemmatization process
        # across multiple threads for faster processing.
        # Do I want to do this using Pool.starmap instead?
        lemmatized = process_map(self._lemmatize_text, self.train_df['tokenized'], max_workers=self._num_threads, chunksize=5)
        self.train_df['lemmatized'] = lemmatized
        lemmatized = process_map(self._lemmatize_text, self.val_df['tokenized'], max_workers=self._num_threads, chunksize=5)
        self.val_df['lemmatized'] = lemmatized
        lemmatized = process_map(self._lemmatize_text, self.test_df['tokenized'], max_workers=self._num_threads, chunksize=5)
        self.test_df['lemmatized'] = lemmatized