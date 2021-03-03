
import os
from os import path
from urllib.parse import urlparse

import pytz
from datetime import datetime
from uuid import uuid4

import pandas as pd

from snemovna.Helpers import *
from snemovna.utility import *
from snemovna.setup_logger import log


class SnemovnaDataFrame(MyDataFrame):
    """Základní třída, která zajišťuje sdílené proměnné a metody pro dětské třídy.

    Attributes
    ----------
    df : pandas DataFrame
        základní tabulka dané třídy
    paths : dict
        cesty k souborům načtených tabulek
    tbl : dict
        načtené tabulky
    meta : třída Meta
        metadata všech dostupných sloupců (napříč načtenými tabulkami)
    volební období : Int64
        volební období sněmovny
    snemovna : Int64
        objekt obsahujici data aktualni snemovny, defaultně None, hodnota se nastaví až v dětské třídě Orgány
    tzn : pytz formát
        časová zóna
    data_dir : string
        adresář, do kterého se ukládají data
    url : string
        url, na které jsou pro danou třídu zazipované tabulky
    zip_path
        lokální cesta k zazipovaným tabulkám
    file_name
        jméno zip souboru (basename)

    Methods
    -------
    drop_by_inconsistency (df, suffix, threshold, t1_name=None, t2_name=None, t1_on=None, t2_on=None, inplace=False)
        Prozkoumá tabulku a oveří konzistenci dat po mergování
    nastav_meta()
        Nastaví meta informace k sloupcům dle aktuálního stavu tabuky df
    rozsir_meta(header, tabulka=None, vlastni=None)
        Rozšíří meta informace k sloupcům dle hlavičky konkrétní tabulky
    """
    def __init__(self, volebni_obdobi=None, *args, **kwargs):
        log.debug("--> SnemovnaDataFrame")
        log.debug(f"Base kwargs: {kwargs}")
        super(SnemovnaDataFrame, self).__init__(*args, **kwargs)
        self._metadata = [
            "df", "meta", 'paths', 'tbl', 'parameters',
            "volební období", "snemovna", "tzn"
        ]

        self.df = pd.DataFrame()
        self.meta = Meta(
            index_name='sloupec',
            dtypes=dict(popis='string', tabulka='string', vlastni='bool', aktivni='bool'),
            defaults=dict(popis=None, tabulka=None, vlastni=None, aktivni=None),
        )
        self.paths = {}
        self.tbl = {}
        self.volebni_obdobi = volebni_obdobi
        self.snemovna = None
        self.tzn = pytz.timezone('Europe/Prague')

        self.parameters = {}
        #log.debug(f"SnemovnaDataFrame1: {self.parameters}")
        #self.parameters['data_dir'] = data_dir
        #self.parameters['stazeno'] = stazeno
        #self.parameters['stahni'] = stahni
        #log.debug(f"SnemovnaDataFrame2: {self.parameters}")

        log.debug("<-- SnemovnaDataFrame")

    def nastav_dataframe(self, frame):
        self.df = frame
        self.drop(index=self.index, inplace=True)
        self.drop(columns=self.columns, inplace=True)
        for col in frame.columns:
            self[col] = frame[col].astype(frame[col].dtype)
        self.nastav_meta()

    def pripoj_data(self, obj, jmeno=''):
        for key in obj.paths:
            self.paths[key] = obj.paths[key]
        for key in obj.tbl:
            self.tbl[key] = obj.tbl[key]
        for key in obj.meta:
            row = obj.meta.data.loc[key].to_dict()
            if row['tabulka'] == 'df':
                row['tabulka'] = jmeno + '_df'
            self.meta[key] = row
        return obj

    def popis(self):
        popis_tabulku(self.df, self.meta, schovej=['aktivni'])

    def popis_sloupec(self, sloupec):
        popis_sloupec(self.df, sloupec)

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
                log.warning(f"Pro sloupec '{c}' nebyla nalezena metadata!")

    def rozsir_meta(self, header, tabulka=None, vlastni=None):
        for k, i in header.items():
            self.meta[k] = dict(popis=i.popis, tabulka=tabulka, vlastni=vlastni)


class SnemovnaZipDataMixin(object):
    def __init__(self, url, data_dir='./data/', stahni=True, stazeno=[], *args, **kwargs):
        log.debug("--> SnemovnaZipDataMixin")
        log.debug(f"SnemovnaZipDataMixin args: {args}")
        log.debug(f"SnemovnaZipDataMixin kwargs: {kwargs}")

        super(SnemovnaZipDataMixin, self).__init__(*args, **kwargs)
        
        log.debug(f"SnemovnaZipDataMixin2 args: {args}")
        log.debug(f"SnemovnaZipDataMixin2 kwargs: {kwargs}")
        
        log.debug(f"SnemovnaZipDataMixin2: {self.parameters}")

        log.debug(f"url: {url}")
        log.debug(f"stahni: {stahni}")
        log.debug(f"stazeno: {stazeno}")

        self.parameters['data_dir'] = data_dir
        self.parameters['stahni'] = stahni

        if 'stazeno' in self.parameters:
              stazeno = list(set(stazeno + self.parameters['stazeno']))
              self.parameters['stazeno'] = stazeno

        if (url not in stazeno) and (stahni == True):
            log.debug(f"<DOWNLOAD ZACATEK>")
            a = urlparse(url)
            filename = os.path.basename(a.path)
            zip_path = f"{data_dir}/{filename}"
            log.debug(f"SnemovnaZipDataMixin: Nastavuji cestu k zip souboru na: {zip_path}")

            # smaz starý zip soubor, pokud existuje
            if os.path.isfile(zip_path):
                os.remove(zip_path)

            log.debug(f"Stahuju {zip_path}!!!!!!!!!")
            download_and_unzip(url, zip_path, data_dir)

            if 'stazeno' not in self.parameters:
                self.parameters['stazeno'] = []
            self.parameters['stazeno'].append(url)

            log.debug(f"<DOWNLOAD KONEC>")
        log.debug("<-- SnemovnaZipDataMixin")


