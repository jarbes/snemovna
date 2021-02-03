import os
from os import path
from urllib.parse import urlparse

import pandas as pd

from utility import *

from setup_logger import log


class Snemovna:

    #data_dir = None
    #url = None
    #paths = {}

    def __init__(self, data_dir='.'):
        log.debug("--> Snemovna")
        self.data_dir = data_dir
        self.url=None
        self.zip=None

        self.paths = {}
        log.debug("<-- Snemovna")

    def missing_files(self):
        missing_files = []
        paths_flat = sum([item if isinstance(item, list) else [item] for item in self.paths.values()], [])
        print(f"Checking for: {paths_flat}")
        for p in paths_flat:
            if path.isfile(p) is not True:
                missing_files.append(p)
        print(f"Missing files: {missing_files}")
        return missing_files

    def nastav_datovy_zdroj(self, url):
        a = urlparse(url)
        self.file_name = os.path.basename(a.path)
        self.url = url #f"https://www.psp.cz/eknih/cdrom/opendata/{self.file_name}"
        self.zip = f"{self.data_dir}/{self.file_name}"
        print(self.zip)

    def stahni(self):
        if (self.url is not None) and (self.zip is not None) and (self.data_dir is not None):
            download_and_unzip(self.url, self.zip, self.data_dir)
        else:
            print("Error: download paths not set!")

    def pretipuj(self, df, header, name=None):
        if name is not None:
            print(f"Tabulka {name}:")
        new_df = pd.DataFrame()
        for col in df.columns:
            if col in header:
                print(f"Přetypovávám sloupec: '{col}'.")
                new_df[col] = df[col].astype(header[col])
        return new_df

