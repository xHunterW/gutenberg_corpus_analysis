# -*- coding: utf-8 -*-
"""

"""

import os
import sys
import string
import pandas as pd


repos = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(repos)

import gutenberg.src.metaquery as metaquery


def read_metadata_and_catalog(metadata_filepath: str, pg_catalog_filepath: str, filter_exist: bool=False,
                              types_to_drop=['Sound', 'Collection', 'Image', 'StillImage', 'MovingImage', 'Dataset']):
    # Read them in
    mq = metaquery.meta_query(path=metadata_filepath, filter_exist=filter_exist)

    pg_df = pd.read_csv(pg_catalog_filepath)

    # Let's get rid of all non-text entries
    ind_to_drop = mq.df[mq.df['type'].isin(types_to_drop)].index
    mq.df.drop(ind_to_drop, inplace=True)

    # Let's rename the PG Catalog ID column
    pg_df.rename({'Text#': 'PG_ID',
                  'Title': 'title',
                  'Type': 'type'}, axis='columns', inplace=True)

    ind_to_drop = pg_df[pg_df['type'].isin(types_to_drop)].index
    pg_df.drop(ind_to_drop, inplace=True)

    # Let's make an ID column in the metaquery df
    mq.df['PG_ID'] = mq.df['id'].str.replace('PG', '')

    # Lets make the columns ints
    mq.df['PG_ID'] = pd.to_numeric(mq.df['PG_ID'])

    pg_df.drop('Issued', axis=1, inplace=True)

    return mq.df.join(pg_df.set_index('PG_ID'), on='PG_ID', rsuffix='_pgc', how='inner')


def compare_columns(df, col_a, col_b, verbose=False):
    def compare(s1, s2):
        s1 = s1.replace('$b', '')
        s2 = s2.replace('$b', '')
        remove = string.punctuation + string.whitespace
        mapping = {ord(c): None for c in remove}
        if not isinstance(s1, str) or not isinstance(s2, str):
            print(f'WARNING: something isnt a string {s1}   {s2}')
            return False, s1, s2

        s1_clean = s1.translate(mapping).casefold()
        s2_clean = s2.translate(mapping).casefold()

        return s1_clean == s2_clean, s1_clean, s2_clean

    col_a_sanitized = df[col_a].str.replace('\r\n', ': ')
    col_b_sanitized = df[col_b].str.replace('\r\n', ': ')
    # matches = df[col_a].apply()
    dont_match = df.loc[~(col_a_sanitized == col_b_sanitized)]

    entries_that_dont_match = []
    attribute_errors = []
    for _, row in dont_match.iterrows():
        try:
            comparison, s1_clean, s2_clean = compare(row[col_a], row[col_b])
            if comparison is False:
                # The title in pg_catalog.csv is just the first part of the title, move along, all
                # is well
                if s1_clean.startswith(s2_clean) or s2_clean.startswith(s1_clean):
                    pass
                    # print('THEY DO MATCH')
                else:
                    if verbose:
                        print(f'Dont Match: id: {row["id"]}   {row[col_a]}   {row[col_b]}')
                    entries_that_dont_match.append(row)

        except AttributeError as e:
            attribute_errors.append(row)
            if verbose:
                print(f'\nAttribute Error: {e}')
                print(row[['id', 'title', col_a, col_b]])

            # raise e
    return dont_match, attribute_errors


def get_word_count(book_id, raw_text_dir):
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


def get_unique_word_count(book_id, raw_text_dir):
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


def get_line_count(book_id, text_dir):
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


def get_token_count(book_id, text_dir):
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


if __name__ == '__main__':
    mq_filepath = '/Users/dean/Documents/gitRepos/gutenberg/metadata/metadata.csv'
    pg_catalog_filepath = '/Users/dean/Documents/gitRepos/gutenberg_corpus_analysis/pg_catalog.csv'

    print(read_metadata_and_catalog(mq_filepath, pg_catalog_filepath))
