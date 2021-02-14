import re
from collections import namedtuple

from datetime import datetime

import numpy as np
import pandas as pd

from html2text import html2text
from bs4 import BeautifulSoup, NavigableString

import os
import requests
from time import time
from urllib.parse import urlparse
from joblib import Parallel, delayed, cpu_count
#from multiprocessing.pool import ThreadPool

from Snemovna import *
from Osoby import *
from Stenozaznamy import *

from setup_logger import log

Recnik = namedtuple("Recnik", ['id_rec', 'id_osoba'])
Promluva = namedtuple('Promluva', ["text", "recnik", "rid", "cas_od", "cas_do"])
Cas = namedtuple('Cas', ['typ', 'hodina', 'minuta'])


# Tabulka StenoText
# Pozor: Texty nejsou součástí oficiálních dat PS!
# Texty se stahují a scrapují z internetových stránek PS, viz např. https://www.psp.cz/eknih/2017ps/stenprot/001schuz/s001001.htm

class StenoTexty(StenoRec, OsobyZarazeni):

    def __init__(self, *args, **kwargs):
        super(StenoTexty, self).__init__(*args, **kwargs)

        self.paths['steno_text'] = f"{self.data_dir}/steno_texty-{self.volebni_obdobi}.pkl"

        if self.stahni == True:
            # scraping z webu
            self.stahni_steno_texty()
            # parsování html
            results, args = self.zpracuj_steno_texty()
            # tvorba pandas tabulky
            self._steno_texty = self.results2df(results, args)
            # ulož lokálně výslednou tabulku
            self._steno_texty.to_pickle(self.paths['steno_text'])

        self.steno_texty, self._steno_texty = self.nacti_steno_texty()

        # Doplneni recnika, který mluvil na konci minulého stenozáznamu (přetahujícího řečníka).
        # Přetahující řečník nemá v aktuálním stenozáznamu identifikátoir, ale zpravidla (v 99% případů) byl zmíněn v některém z minulých stenozáznamů (turns).
        # Tento stenozáznam je nutné vyhledat a uložit jeho číslo ('id_turn_surrogate') a číslo řečníka ('id_rec_surrogate').
        # V joinu se 'steno_rec' se pak použije 'id_rec_surrogate' místo 'id_rec' a 'id_turn_surrogate' místo 'id_turn' pro získání informací o osobě etc.
        # Pozor: naopak informace o času proslovu jsou navázány na 'turn'.
        self.steno_texty.loc[self.steno_texty.id_rec.isna(), 'turn_surrogate'] = np.nan
        self.steno_texty.loc[~self.steno_texty.id_rec.isna(), 'turn_surrogate'] = self.steno_texty.turn
        self.steno_texty['turn_surrogate'] = self.steno_texty.groupby("schuze")['turn_surrogate'].ffill().astype('Int64')
        self.steno_texty['id_rec_surrogate'] = self.steno_texty['id_rec']
        self.steno_texty['id_rec_surrogate'] = self.steno_texty.groupby("schuze")['id_rec_surrogate'].ffill().astype('Int64')

        # připoj osobu ze steno_rec ... we simply add id_osoba to places where it's missing
        m = pd.merge(left=self.steno_texty, right=self.steno_rec[['schuze', "turn", "aname", 'id_osoba']], left_on=["schuze", "turn_surrogate", "id_rec_surrogate"], right_on=["schuze", "turn", "aname"], how="left")
        ids = m[m.id_osoba_x.eq(m.id_osoba_y)].index
        ne_ids = set(m.index)-set(ids)
        assert m[m.index.isin(ne_ids) & (~m.id_osoba_x.isna())].size / m[m.index.isin(ne_ids)].size < 0.1 # This is a consistency sanity check
        m['id_osoba'] = m['id_osoba_y']
        m['turn'] = m['turn_x']
        self.steno_texty = m.drop(labels=['id_osoba_x', 'id_osoba_y', 'turn_y', 'turn_x', 'aname'], axis=1)

        # Merge steno_rec
        suffix = "__steno_rec"
        self.steno_texty = pd.merge(left=self.steno_texty, right=self.steno_rec, left_on=["schuze", "turn_surrogate", "id_rec_surrogate"], right_on=['schuze', 'turn', 'aname'], suffixes = ("", suffix), how='left')
        self.steno_texty = self.steno_texty.drop(labels=['turn__steno_rec'], axis=1) # this inconsistency comes from the 'turn-fix'
        self.steno_texty = drop_by_inconsistency(self.steno_texty, suffix, 0.1, 'steno_texty', 'steno_rec')

        # Merge osoby
        suffix = "__osoby"
        self.steno_texty = pd.merge(left=self.steno_texty, right=self.osoby, on='id_osoba', suffixes = ("", suffix), how='left')
        self.steno_texty = drop_by_inconsistency(self.steno_texty, suffix, 0.1, 'steno_texty', 'osoby')

        ## Merge osoby_zarazeni
        poslanci = self.osoby_zarazeni[(self.osoby_zarazeni.do_o_DT.isna()) & (self.osoby_zarazeni.id_organ==172) & (self.osoby_zarazeni.cl_funkce_CAT=='členství')] # všichni poslanci
        strany = self.osoby_zarazeni[(self.osoby_zarazeni.id_osoba.isin(poslanci.id_osoba)) & (self.osoby_zarazeni.nazev_typ_org_cz == "Klub") & (self.osoby_zarazeni.do_o_DT.isna()) & (self.osoby_zarazeni.cl_funkce_CAT=='členství')]
        self.steno_texty = pd.merge(self.steno_texty, strany[['id_osoba', 'zkratka']], on='id_osoba', how="left")

        ## Merge Strana
        snemovna_id = self.organy[self.organy.nazev_organu_cz=="Poslanecká sněmovna"].sort_values(by="od_organ").iloc[-1].id_organ
        snemovna_od = pd.to_datetime(self.organy[self.organy.id_organ == snemovna_id].iloc[0].od_organ).tz_localize("Europe/Prague")
        snemovna_do = pd.to_datetime(self.organy[self.organy.id_organ == snemovna_id].iloc[0].do_organ).tz_localize("Europe/Prague")

        snemovna_cond = (self.osoby_zarazeni.od_o_DT >= snemovna_od) & (self.osoby_zarazeni.nazev_typ_org_cz == "Klub") & (self.osoby_zarazeni.cl_funkce_CAT=='členství')
        if pd.isnull(snemovna_do) == False:
            snemovna_cond = snemovna_cond | (self.osoby_zarazeni.do_o_DT >= snemovna_do)
        s = self.osoby_zarazeni[snemovna_cond].groupby('id_osoba').size().sort_values()
        prebehlici = s[s > 1]
        print("prebehlici: ", prebehlici)
        
        for id_prebehlika in prebehlici.index:
            for idx, row in self.osoby_zarazeni[ snemovna_cond & (self.osoby_zarazeni.id_osoba == id_prebehlika)].iterrows():
                od, do, id_organ, zkratka =  row['od_o_DT'], row['do_o_DT'], row['id_organ'], row['zkratka']
                print(id_prebehlika, od, do, id_organ, zkratka)
                self.steno_texty.zkratka.mask((self.steno_texty.date >= od) & (self.steno_texty.date <= do) & (self.steno_texty.id_osoba == id_prebehlika), zkratka, inplace=True)

        to_drop = ['zmena', 'id_org']
        self.steno_texty.drop(labels=to_drop, inplace=True, axis=1)

        self.df = self.steno_texty

    def nacti_steno_texty(self):
        df = pd.read_pickle(self.paths['steno_text'])
        return df, df

    def results2df(self, results, args):
        texty = []
        texty_s_poznamkami = []
        schuze = []
        turns = []
        id_osoby = []
        id_reci = []
        poznamky = []
        je_poznamka = []
        cas = []
        typ_casu = []
        dates = []
        for result, arg in zip(results,  args):
            for r in result:
                id_osoba, id_rec = None, None
                if len(r['meta']['recnici']) > 0:
                    if r['meta']['recnici'][0].id_osoba != None:
                        id_osoba = int(r['meta']['recnici'][0].id_osoba)

                    if r['meta']['recnici'][0].id_rec != None:
                        id_rec = int(r['meta']['recnici'][0].id_rec)

                text = r['text']

                if len(r['meta']['poznamky']) > 0:
                    poznamka = r['meta']['poznamky']
                else:
                    poznamka = None

                if len(r['meta']['cas']) > 0:
                    #c = self.formatuj_cas(r['meta']['cas'][0].hodina, r['meta']['cas'][0].minuta)
                    c = f"{r['meta']['cas'][0].hodina}:{r['meta']['cas'][0].minuta}"
                    tc = r['meta']['cas'][0].typ
                else:
                    c, tc = None, None

                texty.append(r['text'])
                texty_s_poznamkami.append(r['meta']['text_s_poznamkami'])
                schuze.append(arg['schuze'])
                turns.append(arg['turn'])
                id_osoby.append(id_osoba)
                id_reci.append(id_rec)
                poznamky.append(poznamka)
                je_poznamka.append(r['meta']['je_poznamka'])
                cas.append(c)
                typ_casu.append(tc)
                dates.append(r['meta']['date'])

        df = pd.DataFrame({'text': texty, 'text_s_poznamkami': texty_s_poznamkami, 'schuze': schuze, 'turn': turns, 'id_osoba': id_osoby, "id_rec": id_reci, 'poznamka': poznamky, 'je_poznamka': je_poznamka, 'cas': cas, 'typ_casu': typ_casu, "date": dates})

        # object se nedaří přetypovat rovnou na Int64, proto ho nejdřív přetypujeme na float
        header1 = {
            'text': 'string',
            'text_s_poznamkami': 'string',
            'schuze': 'Int64',
            'turn': 'Int64',
            'id_osoba': 'Int64',
            'id_rec': 'Int64',
            'poznamka': 'object',
            'je_poznamka': bool,
            'cas': 'string',
            'typ_casu': 'string',
            #'date': 'datetime64[ns]'
        }
        df = self.pretipuj(df, header1, 'steno_texty [stage1]')


        return df

    def cesta(self, schuze, turn):
        return f"www.psp.cz/eknih/{self.volebni_obdobi}ps/stenprot/{schuze:03d}schuz/s{schuze:03d}{turn:03d}.htm"

    def zpracuj_steno_texty(self):
        args = [{
            "path": self.data_dir + '/' + self.cesta(item[0], item[1]),
            "schuze": item[0],
            "turn": item[1]
        } for item in self.steno.groupby(['schuze', 'turn']).groups.keys()]# Do we need some kind of sort here?
        paths = [item['path'] for item in args]

        log.info(f"K zpracování: {len(paths)} souborů.")
        #n_jobs = max([12, 3*cpu_count()])
        results = Parallel(n_jobs=-1, verbose=1, backend="threading")(delayed(self.zpracuj_stenozaznam)(item) for item in paths)

        return results, args

    def stahni_steno_texty(self):
        args = [["https://" + self.cesta(item[0], item[1]), self.data_dir ] for item in self.steno.groupby(['schuze', 'turn']).groups.keys()]
        log.info(f"K stažení: {len(args)} souborů.")
        n_jobs = max([12, 3*cpu_count()])
        Parallel(n_jobs=n_jobs, verbose=1, backend="threading")(delayed(self.stahni_url)(item) for item in args)

    def stahni_url(self, arg):
        url, dir_prefix = arg
        u = urlparse(url)
        n = u.netloc
        p = u.path
        filename = os.path.basename(p)
        d = os.path.dirname(p)
        dirname = dir_prefix + '/' + n + d
        path = dirname + '/' + filename
        #log.debug(f"path: '{path}'")
        Path(dirname).mkdir(parents=True, exist_ok=True)
        r = requests.get(url, stream = True)
        with open(path, 'wb') as f:
            for ch in r:
                f.write(ch)

    def load_soup(self, filename):
        data = open(filename, 'r', encoding='cp1250').read()
        return BeautifulSoup(data, 'html5lib') # Další varianty: 'lxml', 'html.parser'

    def flatten(self, ary):
        return [item for sublist in ary for item in sublist]

    def polish(self, text):
        text = text.strip()
        text = re.sub(r'\n', ' ', text)
        text = re.sub(r'[ ]+', r' ', text)
        return text

    #def formatuj_cas(self, H, M):
    #    try:
    #        ret = pd.to_datetime(f"{H}:{M}", format="%H:%M").time()
    #        return ret
    #    except ValueError as e:
    #        log.error("Value Error: Zachycena chyba. H='{H}', M='{M}'!")
    #        return pd.NaT

    # * (poznámka) **
    def je_poznamka(self, tag):
        return re.match('^\s*\**\s*(\(.*?\))\s*\**\s*$', tag.string) != None

    # Musím vás poprosit o klid. (V sále je hluk.)
    def najdi_poznamky(self, tag):
        return re.findall('\((.*?)\)', tag.string)

    # (9.20 hodin)
    def najdi_cas(self, tag):
        s = tag.string
        if self.je_poznamka(tag):
            # čas zahájení
            m = re.match(r'.*zaháj.*[^0-9]+([0-9]{1,2})\s*[.:]\s*([0-9]{2}).*hod', s)
            if m:
                return Cas('zahájení', m.groups()[0], m.groups()[1])

            # čas přerušení
            m = re.match(r'.*přer.*[^0-9]+([0-9]{1,2})\s*[.:]\s*([0-9]{2}).*hod', s)
            if m:
                return Cas('přerušení', m.groups()[0], m.groups()[1])

            # čas pokračování
            m = re.match(r'.*pokrač.*[^0-9]+([0-9]{1,2})\s*[.:]\s*([0-9]{2}).*hod', s)
            if m:
                return Cas('pokračování', m.groups()[0], m.groups()[1])

            # čas ukončení
            m = re.match(r'.*konč.*[^0-9]+([0-9]{1,2})\s*[.:]\s*([0-9]{2}).*hod', s)
            if m:
                return Cas('ukončení', m.groups()[0], m.groups()[1])

            # obecná časová značka
            m = re.match(r'.*[^0-9]+([0-9]{1,2})\s*[.:]\s*([0-9]{2}).*hod', s)
            if m:
                return Cas('obecně', m.groups()[0], m.groups()[1])

        return None

    # id=r6  & href=https://www.psp.cz/sqw/detail.sqw?id=6452 ...
    # někdy se stane, že není možné identifikovat řečníka, ačkoliv lze určit id řeči
    def najdi_recnika(self, tag):
        if (tag.name == 'a') and tag.attrs and  tag.attrs.get('id') and (re.match(r'^r[0-9]+$', tag.attrs.get('id'))):
            id_rec = re.match(r'^r([0-9]+)$', tag.attrs.get('id')).groups()[0]
            id_osoba = None
            if tag.attrs.get('href'):
                m = re.match(r'\/sqw\/detail.sqw\?id\=([0-9]+)$', tag.attrs.get('href'))
                if m:
                    id_osoba = m.groups()[0]
            return Recnik(id_rec=id_rec, id_osoba=id_osoba)
        return None

    #https://www.psp.cz/sqw/historie.sqw?T=922&O=8
    def najdi_tisk(self, tag):
        if (tag.name == 'a') and tag.attrs.get('href'):
            m = re.match(r'\/sqw\/historie.sqw\?T\=([0-9]+)\&O=([0-9])+$', tag.attrs.get('href'))
            if m:
                return m.groups()[0], m.groups()[1]
        return None

    # https://www.psp.cz/sqw/hlasy.sqw?G=74037
    def najdi_hlasovani(self, tag):
        if (tag.name == 'a') and tag.attrs.get('id') and (re.match(r'^h[0-9]+$', tag.attrs.get('id'))):
            hid = re.match(r'^h([0-9]+)$', tag.attrs.get('id')).groups()[0]
            G = None
            if tag.attrs.get('href'):
                m = re.match(r'\/sqw\/hlasy.sqw\?G\=([0-9]+)$', tag.attrs.get('href'))
                if m:
                    G = m.groups()[0]
            return hid, G
        return None

    def rozloz_tag(self, tag, text, meta):
        for child in tag.contents:
            for fce, klic in [
                [self.najdi_cas, 'cas'],
                [self.najdi_recnika, 'recnici'],
                [self.najdi_tisk, 'tisky'],
                [self.najdi_hlasovani, 'hlasovani'],
                [self.je_poznamka, 'je_poznamka'],
                [self.najdi_poznamky, 'poznamky']
            ]:
                ret = fce(child)
                if ret:
                    meta[klic].append(ret)

                # Promluvy jsou uvozeny jmény řečníků, která je nutné odstranit.
                # Děláme to tady hodně neohrabaně tak, že nastavíme příznak pro odstranění.
                # Samotné odstranění se provádí až ve volající funkci, protože se potřebujeme zbavit ':', která není součástí aktuálního tagu.
                if (klic == 'recnici') and (len(meta['recnici']) > 0) and (len(meta['odstran']) == 0):
                    meta['odstran'].append(self.polish(html2text(child.string)) + ' : ')

                # V jedné promluvě může být víc poznámek
                if (klic == 'poznamky') and (len(meta['poznamky']) > 0):
                    meta['poznamky'] = self.flatten(meta['poznamky'])

            if type(child) == NavigableString:
                t = html2text(child.string)
                text.append(t)
            else:
                self.rozloz_tag(child, text, meta)
        return

    def rozloz_paragraf(self, tag):
        meta = {"recnici": [], "hlasovani": [], "tisky": [], "poznamky": [], "je_poznamka": [], "cas": [], "odstran": [], "text_bez_poznamek": None}
        lines = []

        if tag is None:
            return '', meta

        # analyzuj vnořené tagy
        self.rozloz_tag(tag, lines, meta)
        #log.debug(f"LINES: {lines}")

        text = self.polish(' '.join(lines))

        if (len(meta['recnici']) > 0) and (len(meta['odstran']) > 0):
            for o in meta['odstran']:
                text = re.sub(o, '', text)

        meta['text_s_poznamkami'] = text
        text = re.sub(r"\((.*?)\)", '', text)

        return {"text": text, "meta": meta}

    def get_date(self, body):
        date_tag = body.find("p", class_='date')
        if date_tag is None:
            return
        return date_tag.string.extract()


    def parse_date(self, date):
        months_all = """led únor břez dub květ června července srp zář říj list prosin"""
        mo_beg = months_all.split()
        assert len(mo_beg) == 12

        toks = date.split()
        assert len(toks) == 4
        raw_day, raw_mo, raw_year = toks[1:]

        dt = raw_day.split('.')
        assert len(dt) == 2
        assert dt[1] == ""
        day = int(dt[0])
        assert 0 < day <= 31

        mo = None
        for i, prefix in enumerate(mo_beg):
            if raw_mo.startswith(prefix):
                mo = i + 1
                break
        assert not mo is None
        assert 1 <= mo <= 12

        year = int(raw_year)
        assert 2040 >= year >= 2000

        return "%d-%02d-%02d" % (year, mo, day)

    def zpracuj_stenozaznam(self, filename):
        if not os.path.exists(filename):
            log.error(f"Soubor {filename} neexistuje, přeskakuji.")
            return None

        basename = os.path.basename(filename).split('.')[0]
        soup = self.load_soup(filename)
        body = soup.find("div", id='body')

        if not body:
            log.error(f"V souboru '{filename}' neobsahuje tag 'body', přeskakuji.")
            return None

        date = self.parse_date(self.get_date(body))
        date = pd.to_datetime(date, format="%Y-%m-%d")
        date = date.tz_localize(self.tzn)

        # for every turn [i.e. stenozaznam] we have with a new speaker list
        last_recnik = None

        rows = []
        for p in body.find_all('p', align='justify'):
            row = self.rozloz_paragraf(p)
            row['meta']['date'] = pd.to_datetime(date, format="%Y-%m-%d")
            row['meta']['date'] = row['meta']['date']

            if len(row['meta']['text_s_poznamkami']) == 0: # Check this...
                continue

            if len(row['meta']['recnici']) > 0:
                last_recnik = row['meta']['recnici'][0]
            elif last_recnik != None:
                # Fill in missing speakers for paragraphs.
                # Does not solve missing speakers on turn beginnings, we will have to deal with that separately later.
                row['meta']['recnici'].append(last_recnik)

            rows.append(row)
            #log.debug(f"ROW: {row}")
        return rows

