import os
from os import path
from urllib.parse import urlparse

import pytz

import pandas as pd

from utility import *

from setup_logger import log


# TODO: Explicitly define variables in the header of each class
# How about kwargs & args & multiple inheritance ???

class Snemovna(object):

    def __init__(self, volebni_obdobi=None, data_dir='.', stahni=False, *args, **kwargs):
        log.debug("--> Snemovna")

        super().__init__(*args, **kwargs)

        self.volebni_obdobi = volebni_obdobi

        self.data_dir = data_dir
        self.url=None
        self.zip=None

        self.stahni = stahni

        self.paths = {}

        self.tzn = pytz.timezone('Europe/Prague')

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
        self.url = url
        self.zip = f"{self.data_dir}/{self.file_name}"
        print(self.zip)

    def stahni_data(self):
        mf = self.missing_files()
        log.debug(f"Missing files: {mf}, stahni: {self.stahni}")
        if (len(mf) > 0) or self.stahni:
            if (self.url is not None) and (self.zip is not None) and (self.data_dir is not None):
                download_and_unzip(self.url, self.zip, self.data_dir)
            else:
                print("Error: download paths not set!")

    def pretipuj(self, df, header, name=None):
        if name is not None:
            print(f"Tabulka {name}:")
        #new_df = df.copy()
        for col in df.columns:
            if col in header:
                print(f"Přetypovávám sloupec: '{col}'.")
                df[col] = df[col].astype(header[col])
        return df

