
import requests
from pathlib import Path
from os import listdir #, path # TODO: asi stačí buď jen Path, nebo path
import zipfile

#from datetime import timezone, datetime
#import pytz

import pandas as pd
#import numpy as np

import plotly.graph_objects as go

from snemovna.setup_logger import log

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

    return new_series

# TODO: change from inplace to return
def drop_by_inconsistency (df, suffix, threshold, t1_name=None, t2_name=None):
    inconsistency = {}
    abundance = []

    for col in df.columns[df.columns.str.endswith(suffix)]:
        short_col = col[:len(col)-len(suffix)]

        # Note: np.nan != np.nan by default
        difference = df[(df[short_col] != df[col]) & ~(df[short_col].isna() & df[col].isna())]
        if len(difference) > 0:
          inconsistency[short_col] = float(len(difference))/len(df)
          log.warning(f"While merging '{t1_name}' with '{t2_name}': Columns '{short_col}' and '{col}' differ in {len (difference)} values from {len(df)}, inconsistency ratio: {inconsistency[short_col]:.2f}")
        else:
          abundance.append(short_col)

    to_drop = [col for (col, i) in inconsistency.items() if i >= threshold]
    if len(to_drop) > 0:
        log.warning(f"While merging '{t1_name}' with '{t2_name}': Dropping {to_drop} because of big inconsistency.")
        df = df.drop(labels=to_drop, axis=1)

    to_skip = [col + suffix for col in set(inconsistency.keys()).union(abundance)]
    if len(to_skip) > 0:
      log.warning(f"While merging '{t1_name}' with '{t2_name}': Dropping {to_skip} because of abundance.")
      df = df.drop(labels=to_skip, axis=1)

    return df

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
