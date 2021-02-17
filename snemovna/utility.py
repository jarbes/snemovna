
import requests
from pathlib import Path
from os import listdir #, path # TODO: asi stačí buď jen Path, nebo path
import zipfile

from collections import namedtuple

import pandas as pd
from IPython.display import display
#import numpy as np

import plotly.graph_objects as go

from snemovna.setup_logger import log

#######################################################################
# Pomocn0 struktury pro asociovaná metadata k sloupcům tabulek

MItem = namedtuple('MItem', ("typ", "popis"))

class Meta(object):
    def __init__(self, columns=[], defaults={}, dtypes={}, index_name='name'):
        self.defaults = defaults
        c = set([index_name]).union(columns).union(defaults.keys()).union(dtypes.keys())
        self.data = pd.DataFrame([], columns=c).set_index(index_name)

    def __init__(self, defaults={}, dtypes={}, index_name='name'):
        self.defaults = defaults
        columns = set([index_name]).union(defaults.keys()).union(dtypes.keys())
        self.data = pd.DataFrame([], columns=columns).set_index(index_name)

        for key, dtype in dtypes.items():
            self.data[key] = self.data[key].astype(dtype)

    def __getitem__(self, name):
        found = self.data[self.data.index.isin([name])]
        if len(found) > 0:
            return found.iloc[0]
        else:
            return None

    def __setitem__(self, name, val):
        found = self.data[self.data.index.isin([name])]
        if len(found) > 0:
            for k, i in val.items():
                self.data.loc[self.data.index == name, k] = i
        else:
            missing_keys = self.defaults.keys() - val.keys()
            for k in missing_keys:
                val[k] = self.defaults[k]
            self.data = self.data.append(pd.Series(val.values(), index=val.keys(), name=name))

    def __contains__(self, name):
        found = self.data[self.data.index.isin([name])]
        if len(found) > 0:
            return True
        else:
            return False

    def __iter__(self):
        for c in self.data.index:
            yield c

    def __str__(self):
          return str(self.data)

#######################################################################
# Stahování dat

def download_and_unzip(url, zip_file_name, data_dir):
    log.info(f"Stahuji '{url}'.")
    log.debug(f"Vytvářím adresář: '{data_dir}'")
    Path(data_dir).mkdir(parents=True, exist_ok=True)

    log.debug(f"Stahuji data z: '{url}'")
    r = requests.get(url)
    with open(zip_file_name, 'wb') as f:
        f.write(r.content)
    log.debug(f"Status: {r.status_code}, headers: {r.headers['content-type']}, encoding: {r.encoding}")

    log.debug(f"Rozbaluji data do: '{data_dir}'")
    with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
        zip_ref.extractall(data_dir)


#######################################################################
# Popis dat v pandas tabulkách

def popis_tabulku(df):
    """
    Popiš vlastnosti tabulky
    """
    print(f"Počet řádků v tabulce: {df.index.size}")
    print()

    uniq = df.nunique()
    is_null = df.isnull().sum()
    not_null = len(df) - is_null
    out = pd.DataFrame({
        "sloupec": uniq.index,
        "počet unikátních hodnot": uniq.values,
        "počet nenulových hodnot": not_null.values,
        "typ": df.dtypes.astype(str)
    }).set_index('sloupec').sort_values(by="počet unikátních hodnot", ascending=False)
    display(out)

    sloupce_s_jedinou_hodnotou = out[out["počet unikátních hodnot"] == 1]
    if len(sloupce_s_jedinou_hodnotou) == 0:
        print("Každý sloupec obsahuje alespoň dvě hodnoty.")
    else:
        print("Sloupce s jedinou hodnotou:")
        ret = "\n".join([f"  '{column}' má všude hodnotu '{df[column].iloc[0]}'" for column in sloupce_s_jedinou_hodnotou.index])
        print(ret)

    print()
    print('Nulové hodnoty: ')
    popis_nulove_hodnoty(df)

def popis_nulove_hodnoty(df):
    """
    Vypiš jména sloupců s nulovými hodnotami a odpovídající četnosti
    """
    nans_df = df[df.columns[df.isna().any()]]
    col_with_nans = nans_df.columns

    if len(col_with_nans) == 0:
        print("Tabulka neobsahuje žádné nulové hodnoty [NaNy atp.]")
        return

    for col in col_with_nans:
        cnt = len(df[df[col].isna() == True])
        print(f"Sloupec '{col}' obsahuje {100*cnt/len(df):.2f}% ({cnt} z {len(df)}) nulových hodnot (např. NaNů).")

def popis_sloupec(df, column):
    print(f"Typ: {df[column].dtype}")
    print(f"Počet hodnot: {df[column].count()}")
    print(f"Počet unikátních hodnot: {df[column].nunique()}")
    print(f"První hodnota: {df.iloc[0][column]}")
    print(f"Poslední hodnota: {df.iloc[-1][column]}")

def cetnost_opakovani_dle_sloupce(df, column, printout=False):
    """
    Cetnost radku dle sloupce
    """
    tmp_column = f"{column}_cnt"
    ret = df.groupby(column).agg('size').reset_index(name=tmp_column).groupby(tmp_column).size()

    if printout:
        freq_str = "\n".join([f"{cnt} hodnot se opakuje {freq} krát" for (freq, cnt) in ret.iteritems()])
        print(f"Četnost opakování '{column}' vzestupně:\n{freq_str}")
    return ret


#######################################################################
# Čištění dat v pandas tabulkách

def pretypuj(df, header, name=None, inplace=False):
    if inplace:
        new_df = df
    else:
        new_df = df.copy()

    if name is not None:
        log.debug(f"Přetypování v tabulce '{name}':")
    for col in df.columns:
        if col in header:
            log.debug(f"Přetypovávám sloupec: '{col}'.")
            if isinstance(header[col], str):
                new_df[col] = df[col].astype(header[col])
            elif isinstance(header[col], MItem):
                new_df[col] = df[col].astype(header[col].typ)
            else:
                log.error(type(header[col]))
                log.error(f"Chyba: Neznámý formát přetypování. Sloupec '{col}' nebylo možné přetypovat.")
    return new_df


def strip_all_string_columns(df):
    """
    Trims whitespace from ends of each value across all series in dataframe.
    """
    for col in df.columns:
        if str(df[col].dtype) == 'string':
            df[col] = df[col].str.strip()
    return df

def mask_by_values(series, mask):
    """
    Masks the values of a series according to adictionary
    """
    if True: #(series.dtype == pd.Int64Dtype()):
        series = series.astype(object)

    new_series = series.copy()

    for val_to_mask in series.unique():
        if val_to_mask in mask.keys(): # mask it
            new_series = new_series.mask(series == val_to_mask, mask[val_to_mask])

    return new_series

def format_to_datetime_and_report_skips(df, col, to_format):
    srs = df[col]
    new_srs = pd.to_datetime(srs[~srs.isna()], format=to_format, errors="coerce")
    skipped = srs[(~srs.isna() & new_srs.isna())| (new_srs.dt.strftime(to_format).ne(srs))]
    if len(skipped) > 0:
        log.warning(f"Skipped {len(skipped)} values while formatting '{col}' to datetime. Using format '{to_format}'. Example of skipped rows: {skipped.to_list()[:5]}.")

    return new_srs

#######################################################################
# Zobrazování dat v pandas tabulkách

def groupby_bar(df, by, xlabel=None, ylabel=None, title=''):
    xlabel = by if xlabel == None else xlabel
    ylabel = '' if ylabel == None else ylabel

    groups = df.groupby(by).size()
    fig = go.Figure(go.Bar(
        x=groups.index,
        y=groups.values,
        hovertemplate=
            xlabel + ": %{x}<br>" +
            ylabel + ": %{y:.0}<br>" +
            "<extra></extra>"
    ))

    fig.update_xaxes(title_text=xlabel, type="category")
    fig.update_yaxes(title_text=ylabel)
    fig.update_layout(title=title, width=600, height=400)
    return fig
