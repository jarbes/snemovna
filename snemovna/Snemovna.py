import os
from os import path
from urllib.parse import urlparse

import pytz

import pandas as pd

from snemovna.utility import *

from snemovna.setup_logger import log


class Snemovna(object):
    '''
    Základní třída, která především nastavuje cesty pro stažení tabulek
    '''
    tzn = pytz.timezone('Europe/Prague')

    def __init__(self, volebni_obdobi=None, data_dir='./data/', stahni=True, *args, **kwargs):
        log.debug("--> Snemovna")

        super().__init__(*args, **kwargs)

        self.df = pd.DataFrame()
        self.volebni_obdobi = volebni_obdobi
        self.id_organu = None
        self.data_dir = data_dir
        self.url = None
        self.zip_path = None
        self.file_name = None
        self.stahni = stahni
        self.meta = Meta(
            index_name='sloupec',
            dtypes=dict(popis='string', tabulka='string', vlastni='bool', aktivni='bool'),
            defaults=dict(popis=None, tabulka=None, vlastni=None, aktivni=None),
        )
        self.paths = {}

        log.debug("<-- Snemovna")

    def nastav_datovy_zdroj(self, url):
        a = urlparse(url)
        self.file_name = os.path.basename(a.path)
        self.url = url
        self.zip_path = f"{self.data_dir}/{self.file_name}"
        log.debug(f"Nastavuji cestu k zip souboru na: {self.zip_path}")

    def missing_files(self):
        missing_files = []
        paths_flat = sum([item if isinstance(item, list) else [item] for item in self.paths.values()], [])
        for p in paths_flat:
            if path.isfile(p) is not True:
                missing_files.append(p)
        log.debug(f"Počet chybějících souborů: {len(missing_files)}")
        return missing_files

    def stahni_data(self):
        mf = self.missing_files()
        log.debug(f"Počet chybějících souborů: {len(mf)}, stahni: {self.stahni}")
        if (len(mf) > 0) or self.stahni:
            if (self.url is not None) and (self.zip_path is not None) and (self.data_dir is not None):
                download_and_unzip(self.url, self.zip_path, self.data_dir)
            else:
                log.error("Chyba: cesty pro stahování nebyly nastaveny!")


    def drop_by_inconsistency (self, df, suffix, threshold, t1_name=None, t2_name=None, t1_on=None, t2_on=None, inplace=False):
        inconsistency = {}
        abundance = []

        for col in df.columns[df.columns.str.endswith(suffix)]:
            short_col = col[:len(col)-len(suffix)]

            # Note: np.nan != np.nan by default
            difference = df[(df[short_col] != df[col]) & ~(df[short_col].isna() & df[col].isna())]
            if len(difference) > 0:
              inconsistency[short_col] = float(len(difference))/len(df)
              on = f", left_on={t1_on} right_on={t2_on}" if ((t1_on != None) and (t2_on != None)) else ''
              log.warning(f"While merging '{t1_name}' with '{t2_name}'{on}: Columns '{short_col}' and '{col}' differ in {len (difference)} values from {len(df)}. Inconsistency ratio: {inconsistency[short_col]:.4f}. Example of inconsistency: '{difference.iloc[0][short_col]}' (i.e. {short_col}@{difference.index[0]}) != '{difference.iloc[0][col]}' (i.e. {col}@{difference.index[0]})")
            else:
              abundance.append(short_col)

        to_drop = [col for (col, i) in inconsistency.items() if i >= threshold]
        if len(to_drop) > 0:
            log.warning(f"While merging '{t1_name}' with '{t2_name}': Dropping {to_drop} because of big inconsistency.")

        to_skip = [col + suffix for col in set(inconsistency.keys()).union(abundance)]
        if len(to_skip) > 0:
          log.warning(f"While merging '{t1_name}' with '{t2_name}': Dropping {to_skip} because of abundance.")

        if inplace == True:
            df.drop(columns=set(to_drop).union(to_skip), inplace=True)
            ret = df
        else:
            ret = df.drop(columns=set(to_drop).union(to_skip))

        return ret

    def nastav_meta(self):
        for c in self.meta:
            if c not in self.df.columns:
                self.meta[c] = {"aktivni": False}
            else:
                self.meta[c] = {"aktivni": True}

        for c in self.df.columns:
            if c not in self.meta:
                log.warning(f"Pro sloupec {c} nebyla nalezena metadata!")

    def rozsir_meta(self, header, tabulka=None, vlastni=None):
        for k, i in header.items():
            self.meta[k] = dict(popis=i.popis, tabulka=tabulka, vlastni=vlastni)
