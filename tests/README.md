# Testing the percolator

To test the percolator you first have to setup the 
[development environment](https://github.com/naturalis/docker-ppdb).

## Specimen test

To run the data (specimen) import test you need to copy the data. 
From the `data/test` directory to the common `data/test` directory.

This test will load xeno canto specimen records for updates, new and 
deleted records.

## Create test

This test suite creates taxon enrichments, delete records and summaries.

## Connect test

This suite connects to the database, elastic search logging, locking and 
parses job info.


## Pytest

To run these tests:

 1. Setup the development environment
 2. Copy the data to `/shared-data/test/`
 3. `pip install -e git+https://github.com/naturalis/ppdb_nba.git#egg=ppdb_nba`
 4. `cd src`
 5. run `pytest`


