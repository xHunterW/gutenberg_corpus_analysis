# -*- coding: utf-8 -*-
"""

"""

import os
import re
import pandas as pd
import sys
import string

repos = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(repos)

import gutenberg.src.metaquery as metaquery

def read_metadata_and_catalog(metadata_filepath: str, pg_catalog_filepath: str, filter_exist:bool=False, 
                              types_to_drop=['Sound', 'Collection', 'Image', 'StillImage', 'MovingImage', 'Dataset']):
    # Read them in
    mq = metaquery.meta_query(path=metadata_filepath, filter_exist=filter_exist)

    pg_df = pd.read_csv(pg_catalog_filepath)

    # Let's get rid of all non-text entries
    ind_to_drop = mq.df[mq.df['type'].isin(types_to_drop)].index
    mq.df.drop(ind_to_drop, inplace=True)

    ind_to_drop = pg_df[pg_df['type'].isin(types_to_drop)].index
    pg_df.drop(ind_to_drop, inplace=True)
    
    # Let's rename the PG Catalog ID column
    pg_df.rename({'Text#':'PG_ID',
                  'Title': 'title',
                  'Type': 'type'}, axis='columns', inplace=True)

    # Let's make an ID column in the metaquery df
    mq.df['PG_ID'] = mq.df['id'].str.replace('PG','')

    # Lets make the columns ints
    mq.df['PG_ID'] = pd.to_numeric(mq.df['PG_ID'])

    pg_df.drop('Issued', axis=1, inplace=True)

    return mq.df.join(pg_df.set_index('PG_ID'), on='PG_ID', rsuffix='_pgc', how='inner')

def compare_columns(df, col_a, col_b):
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

    for _, row in dont_match.iterrows():
        try:
            comparison, s1_clean, s2_clean = compare(row[col_a], row[col_b])
            if comparison is False:
                # The title in pg_catalog.csv is just the first part of the title, move along, all
                # is well
                if s1_clean.startswith(s2_clean):
                    pass
                    #print('THEY DO MATCH')
                else:
                    print('THEY DONT')
                    print('Dont Match:')
                    print(row[col_a])
                    print(row[col_b])

                    print(s1_clean)
                    print(s2_clean)
                    print()
        except AttributeError as e:
            print()
            print(row)
            print(row[col_a])
            print(row[col_b])
            print()

            #raise e

if __name__ == '__main__':
    mq_filepath='/Users/dean/Documents/gitRepos/gutenberg/metadata/metadata.csv'
    pg_catalog_filepath='/Users/dean/Documents/gitRepos/gutenberg_corpus_analysis/pg_catalog.csv'

    print(read_metadata_and_catalog(mq_filepath, pg_catalog_filepath))