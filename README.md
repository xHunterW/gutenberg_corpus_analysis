# Gutenberg Corpus Analysis

Comparing various machine learning models for the task of author classification using the [Standard Project Gutenberg Corpus](https://github.com/pgcorpus/gutenberg).  Currently examining an 80-author classification problem, looking only at authors with at least 30 books and at least 30,000 lines.

## Dependencies
May use features that have not yet been merged upstream on corpus repo, as such requires a copy of our fork of [gutenberg](https://github.com/DeanKW/gutenberg), cloned to the same directory, until a pyproject.toml is written.

## Getting the Data
If you cannot use `rsync` due to using Windows, you can either install rsync for windows, Windows Subsystem for Linux, or use our data downloader.
