import argparse
import pandas as pd
import sqlite3
import os
import pandas as pd
from extra_data import extra_data_fdj
from prepare_clean_datasets import prepare_clean_datasets


def main():
   
   extra_data_fdj()
   print("The data has been extracted and saved in the database.")
   extra_data_fdj("loto")
   print("The data has been extracted and saved in the database.")
   prepare_clean_datasets()
   print("The data has been cleaned and saved in the database.")


if __name__ == '__main__':
    main()
