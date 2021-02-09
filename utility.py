# TODO: apply logger

import requests
from pathlib import Path
from os import listdir #, path # TODO: asi stačí buď jen Path, nebo path
import zipfile

#from datetime import timezone, datetime
#import pytz

import pandas as pd
#import numpy as np

import plotly.graph_objects as go

from setup_logger import log

#######################################################################
# Stahování dat

def download_and_unzip(url, zip_file_name, data_dir):
    log.debug(f"\nVytvářím adresář: '{data_dir}'")
    Path(data_dir).mkdir(parents=True, exist_ok=True)

    log.debug(f"Stahuji data z: '{url}'")
    r = requests.get(url)
    with open(zip_file_name, 'wb') as f:
        f.write(r.content)
    log.debug(f"Status: {r.status_code}, headers: {r.headers['content-type']}, encoding: {r.encoding}")

    log.debug(f"Rozbaluji data do: '{data_dir}'")
    with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
        zip_ref.extractall(data_dir)
    log.debug(f"Soubory v adresáři: {listdir(data_dir)}")


#######################################################################
# Popis dat v pandas tabulkách

def popis_tabulku(df):
    """
    Popiš vlastnosti tabulky
    """
    print(f"Počet řádků v tabulce: {df.index.size}")
    print()
    print("Počet unikátních hodnot pro každý sloupec:")
    ret = df.nunique().sort_values(ascending=False)
    print(df.nunique().sort_values(ascending=False))
    print()

    sloupce_s_jedinou_hodnotou = ret[ret == 1]
    if len(sloupce_s_jedinou_hodnotou) == 0:
        print("Každý sloupec obsahuje alespoň dvě hodnoty.")
    else:
        print("Sloupce s jedinou hodnotou:")
        ret = "\n".join([f"  '{column}' má všude hodnotu '{df[column].iloc[0]}'" for (column, cnt) in sloupce_s_jedinou_hodnotou.iteritems()])
        print(ret)

    print()
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
    #print(df[column].describe())
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

def strip_all_string_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    strip_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(strip_strings)

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
        #else:
        #    new_column = new_column.mask(df[column] == val_to_mask, f"{val_to_mask}{unmasked_suffix}")
    return new_series

# TODO: change from inplace to return
def drop_by_inconsistency (df, suffix, threshold):
    inc = {}

    for col in df.columns[df.columns.str.endswith(suffix)]:
        short_col = col[:len(col)-len(suffix)]
        difference = df[df[short_col] != df[col]]
        difference_with_right_nans = df[(df[short_col] != df[col]) | ((~df[col].isna()) & (df[col].isna()))]
        print(f"'{short_col}' and '{col}' differ in {len (difference)} columns from {len(df)} [difference with right nans: {len(difference_with_right_nans)}]")
        inc[short_col] = float(len(difference))/len(df)

    to_drop = [col for (col, inconsistency) in inc.items() if inconsistency >= threshold]
    to_skip = [col + suffix for col in inc.keys()]
    print(f"Dropping {to_drop} because of big inconsistencies.")
    new_df = df.drop(labels=to_drop, axis=1)
    print(f"Dropping {to_skip} because of abundance.")
    new_df = new_df.drop(labels=to_skip, axis=1)

    return new_df


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
