
# Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1302

from glob import glob
import pytz

import pandas as pd

from snemovna.utility import *

from snemovna.Snemovna import *
from snemovna.PoslanciOsoby import Osoby, Organy, Poslanci

from snemovna.setup_logger import log


# Agenda hlasování eviduje hlasování Poslanecké sněmovny, většinou prováděné hlasovacím zařízením. Ze seznamu hlasování jsou vyjmuta hlasování během neveřejných jednání Poslancké sněmovny.
# Pojem přihlášen znamená přihlášen k hlasovacímu zařízení a teprve v tomto stavu je jeho hlasování bráno v potaz. Zdržel se znamená stisknutí tlačítka X během hlasování, nehlasoval znamená nestisknutí žádného tlačítka během hlasování. Za výsledek hlasování poslance se bere naposledy stisknuté tlačítko hlasovacího zařízení během časového intervalu hlasování (20 sekund), neboli, poslanec může během této doby libovolně změnit svoje hlasování.
# Výsledek hlasování je stav výsledků hlasování jednotlivých poslanců na konci časového intervalu hlasování. Tj. pokud se poslanec během časového intervalu hlasování odhlásí od hlasovacího zařízení, je uveden jako nepřihlášen, poslanci se mohou přihlašovat do hlasovacího zařízení i během časového intervalu hlasování.
# Od účinnosti novely jednacího řádu 90/1995 Sb. se nerozlišuje zdržel se a nehlasoval, tj. příslušné počty se sčítají.
# Pokud skončí v průběhu volebního období poslanci mandát, ihned vzniká mandát jeho náhradníkovi, který se ujímá mandátu po složení slibu na první schůzi, které se zúčastní. Mezitím je ve výsledcích hlasování veden jako nepřihlášen (výsledek 'W').
# Upozornění: data omluv se mohou doplňovat se zpožděním a tedy počty omluvených se mohou lišit. Na výsledek hlasování to nemá žádný vliv, během hlasování není seznam omluvených k dispozici a omluvení poslanci jsou vedeni jako nepřihlášen.

class HlasovaniZipDataMixin(object):

    def __init__(self, *args, **kwargs):
        log.debug("--> HlasovaniZipDataMixin")
        log.debug(f"HlasovaniZipDataMixin args: {args}")
        log.debug(f"HlasovaniZipDataMixin kwargs: {kwargs}")

        # Abychom mohli nastavit cestu, musíme znát volební období.
        stazeno_organy = []
        if 'volebni_obdobi' in kwargs:
            volebni_obdobi = kwargs['volebni_obdobi']
        else:
            org = Organy(*args, **kwargs)
            volebni_obdobi = org._posledni_snemovna().od_organ.year
            kwargs['volebni_obdobi'] = volebni_obdobi
            log.debug(f"HlasovaniZipDataMixin - org.parameters: {org.parameters}")

            if 'stazeno' in org.parameters:
                if 'stazeno' in kwargs:
                    kwargs['stazeno'] += org.parameters['stazeno']
                else:
                    kwargs['stazeno'] = org.parameters['stazeno']
                stazeno_organy = kwargs['stazeno']

        if 'url' not in kwargs:
            kwargs['url'] = f"https://www.psp.cz/eknih/cdrom/opendata/hl-{volebni_obdobi}ps.zip"
        super(HlasovaniZipDataMixin, self).__init__(*args, **kwargs)
        self.parameters['stazeno'] = list(set(self.parameters['stazeno'] + stazeno_organy))

        if 'stazeno' in kwargs:
            self.parameters['stazeno'] += kwargs['stazeno']

        log.debug(f"HlasovaniZipDataMixin2 args: {args}")
        log.debug(f"HlasovaniZipDataMixin2 kwargs: {kwargs}")
        log.debug(f"HlasovaniZipDataMixini2 parameters: {self.parameters}")

        log.debug("<-- HlasovaniZipDataMixin")


class Hlasovani(HlasovaniZipDataMixin, SnemovnaZipDataMixin, SnemovnaDataFrame):

    def __init__(self, *args, **kwargs):
        log.debug("--> Hlasovani")
        log.debug(f"Hlasovani args: {args}")
        log.debug(f"Hlasovani kwargs: {kwargs}")
        super(Hlasovani, self).__init__(*args, **kwargs)
        log.debug(f"Hlasovani2 args: {args}")
        log.debug(f"Hlasovani2 kwargs: {kwargs}")

        if 'stazeno' in self.parameters:
            kwargs['stazeno'] = self.parameters['stazeno']
        o = self.pripoj_data(Organy(*args, **kwargs), jmeno='organy')
        log.debug(f"Hlasovani3 args: {args}")
        log.debug(f"Hlasovani3 kwargs: {kwargs}")


        # Souhrnné informace o hlasování
        self.paths['hlasovani'] = f"{self.parameters['data_dir']}/hl{self.volebni_obdobi}s.unl"
        # Zpochybnění výsledků hlasování a případné opakované hlasování
        self.paths['zpochybneni'] = f"{self.parameters['data_dir']}/hl{self.volebni_obdobi}z.unl"
        # Hlasování, která byla prohlášena za zmatečné, tj. na jejich výsledek nebyl brán zřetel
        self.paths['zmatecne'] = f"{self.parameters['data_dir']}/zmatecne.unl"
        # Vazba mezi stenozázamem a hlasováním
        self.paths['stenozaznam'] = f"{self.parameters['data_dir']}/hl{self.volebni_obdobi}v.unl"

        # Načtení datových tabulek
        self.tbl['hlasovani'], self.tbl['_hlasovani'] = self.nacti_hlasovani()
        self.tbl['zmatecne'], self.tbl['_zmatecne'] = self.nacti_zmatecne()
        self.tbl['zpochybneni'], self.tbl['_zpochybneni'] = self.nacti_zpochybneni()
        self.tbl['vazba_stenozaznam'], self.tbl['_vazba_stenozaznam'] = self.nacti_vazbu_stenozaznamu() # Nemusí být aktuální. Například pro sněmovnu 2017 je tabulka nevyplněná.

        # Zúžení dat na zvolené volební období. Tohle je asi nejjednodušší způsob.
        min_id = self.tbl['hlasovani'].id_hlasovani.min()
        max_id = self.tbl['hlasovani'].id_hlasovani.max()
        self.tbl['zmatecne'] = self.tbl['zmatecne'][
            (self.tbl['zmatecne'].id_hlasovani >= min_id)
            & (self.tbl['zmatecne'].id_hlasovani <= max_id)
        ]
        self.tbl['zpochybneni'] = self.tbl['zpochybneni'][
            (self.tbl['zpochybneni'].id_hlasovani >= min_id)
            & (self.tbl['zpochybneni'].id_hlasovani <= max_id)
        ]
        self.tbl['vazba_stenozaznam'] = self.tbl['vazba_stenozaznam'][
            (self.tbl['vazba_stenozaznam'].id_hlasovani >= min_id)
            & (self.tbl['vazba_stenozaznam'].id_hlasovani <= max_id)
        ]

        # Přidání indikátorů
        self.tbl['hlasovani']['je_zpochybneni'] = self.tbl['hlasovani'].id_hlasovani.isin(self.tbl['zpochybneni'].id_hlasovani.unique())
        self.meta['je_zpochybneni'] = dict(popis='Indikátor zpochybnění hlasování', tabulka='df', vlastni=True)

        self.tbl['hlasovani']['je_zmatecne'] = self.tbl['hlasovani'].id_hlasovani.isin(self.tbl['zmatecne'].id_hlasovani.unique())
        self.meta['je_zmatecne'] = dict(popis='Indikátor zmatečného hlasování', tabulka='df', vlastni=True)

        # Připojení informací o stenozaznamu. Pozor, nemusí být aktuální. Například pro snemovnu 2017 momentálně (16.2.2021) data chybí.
        self.tbl['hlasovani']['ma_stenozaznam'] = self.tbl['hlasovani'].id_hlasovani.isin(self.tbl['vazba_stenozaznam'].id_hlasovani.unique())
        self.meta['ma_stenozaznam'] = dict(popis='Indikátor existence stenozáznamu', tabulka='df', vlastni=True)

        suffix = '__stenozaznam'
        self.tbl['hlasovani'] = pd.merge(
            left=self.tbl['hlasovani'],
            right=self.tbl['vazba_stenozaznam'],
            on="id_hlasovani",
            how="left",
            suffixes=('', suffix)
        )
        self.drop_by_inconsistency(self.tbl['hlasovani'], suffix, 0.1, 'hlasovani', 'stenozaznam', inplace=True)

        self.nastav_dataframe(self.tbl['hlasovani'])

        log.debug("<-- Hlasovani")

    def nacti_hlasovani(self):
        header = {
            'id_hlasovani': MItem('Int64', 'Identifikátor hlasování'),
            'id_organ': MItem('Int64', 'Identifikátor orgánu, viz Organy:id_organ'),
            'schuze': MItem('Int64', 'Číslo schůze'),
            'cislo': MItem('Int64', 'Číslo hlasování'),
            'bod': MItem('Int64', 'Bod pořadu schůze; je-li menší než 1, pak jde o procedurální hlasování nebo o hlasování k bodům, které v době hlasování neměly přiděleno číslo.'),
            'datum__ORIG': MItem('string', 'Datum hlasování [den]'),
            'cas': MItem('string', 'Čas hlasování'),
            "pro": MItem('Int64', 'Počet hlasujících pro'),
            "proti": MItem('Int64', 'Počet hlasujících proti'),
            "zdrzel": MItem('Int64', 'Počet hlasujících zdržel se, tj. stiskl tlačítko X'),
            "nehlasoval": MItem('Int64', 'Počet přihlášených, kteří nestiskli žádné tlačítko'),
            "prihlaseno": MItem('Int64', 'Počet přihlášených poslanců'),
            "kvorum": MItem('Int64', 'Kvórum, nejmenší počet hlasů k přijetí návrhu'),
            "druh_hlasovani__ORIG": MItem('string', 'Druh hlasování: N - normální, R - ruční (nejsou známy hlasování jednotlivých poslanců)'),
            "vysledek__ORIG": MItem('string', 'Výsledek: A - přijato, R - zamítnuto, jinak zmatečné hlasování'),
            "nazev_dlouhy": MItem('string', 'Dlouhý název bodu hlasování'),
            "nazev_kratky": MItem('string', 'Krátký název bodu hlasování')
        }

        # Doporučené kódování 'cp1250' nefunguje, detekované 'ISO-8859-1' také nefunguje, 'ISO-8859-2' funguje.
        _df = pd.read_csv(self.paths['hlasovani'], sep="|", names = header.keys(),  index_col=False, encoding='ISO-8859-2')
        df = pretypuj(_df, header, name='hlasovani')
        self.rozsir_meta(header, tabulka='hlasovani', vlastni=False)

        # Odstraň whitespace z řetězců
        df = strip_all_string_columns(df)

        # Přidej 'datum'
        df['datum'] = pd.to_datetime(df['datum__ORIG'] + ' ' + df['cas'], format='%d.%m.%Y %H:%M')
        df['datum'] = df['datum'].dt.tz_localize(self.tzn)
        self.meta['datum'] =  dict(popis='Datum hlasování', tabulka='hlasovani', vlastni=True)

        # Přepiš 'cas'
        df['cas'] = df['datum'].dt.time
        self.meta['cas'] = dict(popis='Čas hlasování', tabulka='hlasovani', vlastni=False)

        # Interpretuj 'bod pořadu'
        df["bod__KAT"] = df.bod.astype('string').mask(df.bod < 1, 'procedurální nebo bez přiděleného čísla').mask(df.bod >= 1, "normální")
        self.meta['bod__KAT'] = dict(popis='Katogorie bodu hlasování', tabulka='hlasovani', vlastni=True)

        # Interpretuj 'výsledek'
        mask = {'A': "přijato", 'R': 'zamítnuto'}
        df["vysledek"] = mask_by_values(df.vysledek__ORIG, mask).astype('string')
        self.meta['vysledek'] = dict(popis='Výsledek hlasování', tabulka='hlasovani', vlastni=True)

        # Interpretuj 'druh hlasování'
        mask = {'N': 'normální', 'R': 'ruční'}
        df["druh_hlasovani"] = mask_by_values(df.druh_hlasovani__ORIG, mask).astype('string')
        self.meta['druh_hlasovani'] =  dict(popis='Druh hlasování', tabulka='hlasovani', vlastni=True)

        return df, _df

    # Načti tabulku zmatecneho hlasovani
    def nacti_zmatecne(self):
        header = {
            "id_hlasovani": MItem("Int64", 'Identifikátor hlasování.')
        }

        _df = pd.read_csv(self.paths['zmatecne'], sep="|", names = header.keys(),  index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, name='zmatecne')
        self.rozsir_meta(header, tabulka='zmatecne', vlastni=False)

        return df, _df

    # Načti tabulku zpochybneni hlasovani (hl_check)
    def nacti_zpochybneni(self):
        header = {
            "id_hlasovani": MItem('Int64', 'Identifikátor hlasování, viz Hlasovani:id_hlasovani.'),
            "turn": MItem('Int64', 'Číslo stenozáznamu, ve kterém je první zmínka o zpochybnění hlasování.'),
            "mode": MItem('Int64', 'Typ zpochybnění: 0 - žádost o opakování hlasování - v tomto případě se o této žádosti neprodleně hlasuje a teprve je-li tato žádost přijata, je hlasování opakováno; 1 - pouze sdělení pro stenozáznam, není požadováno opakování hlasování.'),
            "id_h2": MItem('Int64', 'Identifikátor hlasování o žádosti o opakování hlasování, viz hl_hlasovani:id_hlasovani. Zaznamenává se poslední takové, které nebylo zpochybněno.'),
            "id_h3": MItem('Int64', 'Identifikátor opakovaného hlasování, viz hl_hlasovani:id_hlasovani a hl_check:id_hlasovani. Zaznamenává se poslední takové, které nebylo zpochybněno.')
        }

        _df = pd.read_csv(self.paths['zpochybneni'], sep="|", names = header.keys(),  index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, name='zpochybneni')
        self.rozsir_meta(header, tabulka='zpochybneni', vlastni=False)

        # 0 - žádost o opakování hlasování - v tomto případě se o této žádosti neprodleně hlasuje a teprve je-li tato žádost přijata, je hlasování opakováno;
        # 1 - pouze sdělení pro stenozáznam, není požadováno opakování hlasování.
        maska = {0: "žádost o opakování hlasování", 1: "pouze sdělení pro stenozáznam"}
        df["mode__KAT"] = mask_by_values(df["mode"], maska).astype('string')
        self.meta['mode__KAT'] = dict(popis='Typ zpochybnění', tabulka='zpochybneni', vlastni=True)

        return df, _df

    def nacti_vazbu_stenozaznamu(self):
        ''' Načte tabulku vazeb hlasovani na stenozaznam.'''

        header = {
            "id_hlasovani": MItem('Int64', 'Identifikátor hlasování, viz hl_hlasovani:id_hlasovani'),
            "turn": MItem('Int64', 'Číslo stenozáznamu'),
            "typ__ORIG": MItem('Int64', 'Typ vazby: 0 - hlasování je v textu explicitně zmíněno a lze tedy vytvořit odkaz přímo na začátek hlasování, 1 - hlasování není v textu explicitně zmíněno, odkaz lze vytvořit pouze na stenozáznam jako celek.')
        }
        _df = pd.read_csv(self.paths['stenozaznam'], sep="|", names = header.keys(),  index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, name='stenozaznam')

        self.rozsir_meta(header, tabulka='stenozaznam', vlastni=False)

        # Interpretuj 'typ'
        df["typ"] = mask_by_values(df.typ__ORIG, {0: "hlasovani zmíněno v stenozáznamu", 1: "hlasování není zmíněno v stenozáznamu"}).astype('string')
        self.meta['typ'] = dict(popis='Typ vazby na stenozáznam.', tabulka='vazba_stenozaznam', vlastni=True)

        return df, _df


class ZmatecneHlasovani(Hlasovani):

    def __init__(self, *args, **kwargs):
        log.debug("--> ZmatecneHlasovani")

        super().__init__(*args, **kwargs)

        suffix = "__hlasovani"
        self.tbl['zmatecne'] = pd.merge(left=self.tbl['zmatecne'], right=self.tbl['hlasovani'], on='id_hlasovani', suffixes = ("", suffix), how='left')
        self.tbl['zmatecne'] = self.drop_by_inconsistency(self.tbl['zmatecne'], suffix, 0.1, 'zmatecne', 'hlasovani')

        self.nastav_dataframe(self.tbl['zmatecne'])

        log.debug("<-- ZmatecneHlasovani")


class ZpochybneniHlasovani(Hlasovani):

    def __init__(self, *args, **kwargs):
        log.debug("--> ZpochybneniHlasovani")

        super().__init__(*args, **kwargs)

        suffix = "__hlasovani"
        self.tbl['zpochybneni'] = pd.merge(left=self.tbl['zpochybneni'], right=self.tbl['hlasovani'], on='id_hlasovani', suffixes = ("", suffix), how='left')
        self.tbl['zpochybneni'] = self.drop_by_inconsistency(self.tbl['zpochybneni'], suffix, 0.1, 'zpochybneni', 'hlasovani')

        self.nastav_dataframe(self.tbl['zpochybneni'])

        log.debug("<-- ZpochybneniHlasovani")


class ZpochybneniPoslancem(ZpochybneniHlasovani, Osoby):

    def __init__(self, *args, **kwargs):
        log.debug("--> ZpochybneniPoslancem")

        super(ZpochybneniPoslancem, self).__init__(*args, **kwargs)

        # Poslanci, kteří oznámili zpochybnění hlasování
        self.paths['zpochybneni_poslancem'] = f"{self.parameters['data_dir']}/hl{self.volebni_obdobi}x.unl"

        self.tbl['zpochybneni_poslancem'], self.tbl['_zpochybneni_poslancem'] = self.nacti_zpochybneni_poslancem()

        # Připojuje se tabulka 'hlasovani', nikoliv 'zpochybneni_hlasovani', protože není možné mapovat řádky 'zpochybneni_hlasovani' na 'zpochybneni_poslancem'. Jedná se zřejmě o nedokonalost datového modelu.
        suffix = "__hlasovani"
        self.tbl['zpochybneni_poslancem'] = pd.merge(left=self.tbl['zpochybneni_poslancem'], right=self.tbl['hlasovani'], on='id_hlasovani', suffixes = ("", suffix), how='left')
        self.drop_by_inconsistency(self.tbl['zpochybneni_poslancem'], suffix, 0.1, 'zpochybneni_poslancem', 'hlasovani', inplace=True)

        # Připoj informace o osobe # TODO: Neměli by se připojovat spíš Poslanci než Osoby?
        suffix = "__osoby"
        self.tbl['zpochybneni_poslancem'] = pd.merge(left=self.tbl['zpochybneni_poslancem'], right=self.tbl['osoby'], on='id_osoba', suffixes = ("", suffix), how='left')
        self.drop_by_inconsistency(self.tbl['zpochybneni_poslancem'], suffix, 0.1, 'zpochybneni_poslancem', 'osoby', inplace=True)

        id_organ_dle_volebniho_obdobi = self.tbl['organy'][(self.tbl['organy'].nazev_organ_cz == 'Poslanecká sněmovna') & (self.tbl['organy'].od_organ.dt.year == self.volebni_obdobi)].iloc[0].id_organ
        self.tbl['zpochybneni_poslancem'] = self.tbl['zpochybneni_poslancem'][self.tbl['zpochybneni_poslancem'].id_organ == id_organ_dle_volebniho_obdobi]

        self.nastav_dataframe(self.tbl['zpochybneni_poslancem'])

        log.debug("<-- ZpochybneniPoslancem")

    def nacti_zpochybneni_poslancem(self):
        header = {
            "id_hlasovani": MItem('Int64', 'Identifikátor hlasování, viz Hlasovani:id_hlasovani a ZpochybneniPoslancem:id_hlasovani, které bylo zpochybněno.'),
            "id_osoba": MItem('Int64', 'Identifikátor poslance, který zpochybnil hlasování; viz Osoby:id_osoba.'),
            "mode": MItem('Int64', 'Typ zpochybnění, viz ZpochybneniHlasovani:mode.')
        }

        _df = pd.read_csv(self.paths['zpochybneni_poslancem'], sep="|", names = header.keys(),  index_col=False, encoding='cp1250')
        df = pretypuj(_df, header)
        self.rozsir_meta(header, tabulka='zpochybneni_poslancem', vlastni=False)

        return df, _df


#class Omluvy(HlasovaniObecne, Poslanci, Organy):
class Omluvy(HlasovaniZipDataMixin, SnemovnaZipDataMixin, SnemovnaDataFrame):

    def __init__(self, *args, **kwargs):
        log.debug("--> Omluvy")

        super(Omluvy, self).__init__(*args, **kwargs)
        if 'stazeno' in self.parameters:
            kwargs['stazeno'] = self.parameters['stazeno']

        p = self.pripoj_data(Poslanci(*args, **kwargs), jmeno='poslanci')
        if 'stazeno' in p.parameters:
            kwargs['stazeno'] = p.parameters['stazeno']

        org = self.pripoj_data(Organy(*args, **kwargs), jmeno='organy')
        self.snemovna = org.snemovna
        if 'stazeno' in org.parameters:
            kwargs['stazeno'] = org.parameters['stazeno']

        self.paths['omluvy'] = f"{self.parameters['data_dir']}/omluvy.unl"

        self.tbl['omluvy'], self.tbl['_omluvy'] = self.nacti_omluvy()

        # Připoj informace o poslanci
        suffix = "__poslanci"
        self.tbl['omluvy'] = pd.merge(left=self.tbl['omluvy'], right=self.tbl['poslanci'], on='id_poslanec', suffixes = ("", suffix), how='left')
        self.tbl['omluvy'] = self.drop_by_inconsistency(self.tbl['omluvy'], suffix, 0.1, 'omluvy', 'poslanci')

        # Připoj Orgány
        suffix = "__organy"
        self.tbl['omluvy'] = pd.merge(left=self.tbl['omluvy'], right=self.tbl['organy'], on='id_organ', suffixes=("", suffix), how='left')
        self.tbl['omluvy'] =  self.drop_by_inconsistency(self.tbl['omluvy'], suffix, 0.1, 'omluvy', 'organy')

        # Zúžení na volební období
        self.tbl['omluvy'] = self.tbl['omluvy'][(self.tbl['omluvy'].id_parlament == self.snemovna.id_organ)]

        self.nastav_dataframe(self.tbl['omluvy'])

        log.debug("<-- Omluvy")

    def nacti_omluvy(self):
        # Tabulka zaznamenává časové ohraničení omluv poslanců z jednání Poslanecké sněmovny.
        # Omluvy poslanců sděluje předsedající na začátku nebo v průběhu jednacího dne.
        # Data z tabulky se použijí pouze k nahrazení výsledku typu '@', tj. pokud výsledek hlasování jednotlivého poslance je nepřihlášen, pak pokud zároveň čas hlasování spadá do časového intervalu omluvy, pak se za výsledek považuje 'M', tj. omluven.
        #Pokud je poslanec omluven a zároveň je přihlášen, pak výsledek jeho hlasování má přednost před omluvou.
        header = {
            "id_organ": MItem('Int64', 'Identifikátor volebního období, viz Organy:id_organ'),
            "id_poslanec": MItem('Int64', 'Identifikátor poslance, viz Poslanci:id_poslanec'),
            "den__ORIG": MItem('string', 'Datum omluvy'),
            "od__ORIG": MItem('string', 'Čas začátku omluvy, pokud je null, pak i omluvy:do je null a jedná se o omluvu na celý jednací den.'),
            "do__ORIG": MItem('string', 'Čas konce omluvy, pokud je null, pak i omluvy:od je null a jedná se o omluvu na celý jednací den.')
        }

        _df = pd.read_csv(self.paths['omluvy'], sep="|", names = header,  index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'omluvy')
        self.rozsir_meta(header, tabulka='omluvy', vlastni=False)

        df['od'] = format_to_datetime_and_report_skips(df, 'od__ORIG', to_format='%H:%M').dt.tz_localize(self.tzn).dt.time
        self.meta['od'] = dict(popis='Čas začátku omluvy.', tabulka='omluvy', vlastni=True)
        df['do'] = format_to_datetime_and_report_skips(df, 'do__ORIG', to_format='%H:%M').dt.tz_localize(self.tzn).dt.time
        self.meta['do'] = dict(popis='Čas konce omluvy.', tabulka='omluvy', vlastni=True)
        df['den'] = format_to_datetime_and_report_skips(df, 'den__ORIG', to_format='%d.%m.%Y').dt.tz_localize(self.tzn)
        self.meta['den'] = dict(popis='Datum omluvy [den].', tabulka='omluvy', vlastni=True)

        # TODO: Přidej sloupec typu 'datum_od', 'datum_do'
        # O začátcích a koncích omluv je možné něco zjistit z tabulky Stentexty, ale nebude to moc spolehlivé.
        #df['datum_od'] = pd.to_datetime(df['den'] + ' ' + df['od'], format='%d.%m.%Y %H:%M')
        #df['datum_do'] = pd.to_datetime(df['den'] + ' ' + df['od'], format='%d.%m.%Y %H:%M')

        return df, _df


#Tabulka zaznamenává výsledek hlasování jednotlivého poslance.
class HlasovaniPoslance(Hlasovani, Poslanci, Organy):

    def __init__(self, *args, **kwargs):
        log.debug("--> HlasovaniPoslance")

        super(HlasovaniPoslance, self).__init__(*args, **kwargs)
        if 'stazeno' in self.parameters:
            kwargs['stazeno'] = self.parameters['stazeno']

        p = self.pripoj_data(Poslanci(*args, **kwargs), jmeno='poslanci')
        self.snemovna = p.snemovna

        # V souborech uložena jako hlXXXXhN.unl, kde XXXX je reference volebního období a N je číslo části. V 6. a 7. volebním období obsahuje část č. 1 hlasování 1. až 50. schůze, část č. 2 hlasování od 51. schůze.
        self.paths['hlasovani_poslance'] = glob(f"{self.parameters['data_dir']}/hl{self.volebni_obdobi}h*.unl")

        self.tbl['hlasovani_poslance'], self.tbl['_hlasovani_poslance'] = self.nacti_hlasovani_poslance()

        # Připoj Poslance
        to_skip = ['web', 'ulice', 'obec', 'psc', 'telefon', 'fax', 'psp_telefon', 'email', 'facebook', 'foto', 'zmena', 'umrti', 'adresa', 'sirka', 'delka']
        self.tbl['hlasovani_poslance'] = pd.merge(left=self.tbl['hlasovani_poslance'], right=self.tbl['poslanci'], on="id_poslanec", suffixes=("", "__poslanci"), how='left')
        self.tbl['hlasovani_poslance'].drop(columns = to_skip, inplace=True)
        self.drop_by_inconsistency(self.tbl['hlasovani_poslance'], "__poslanci", 0.1, 'hlasovani_poslance', 'poslanci', inplace=True)

        self.tbl['hlasovani_poslance'] = self.tbl['hlasovani_poslance'][self.tbl['hlasovani_poslance'].id_parlament == self.snemovna.id_organ]

        self.nastav_dataframe(self.tbl['hlasovani_poslance'])

        log.debug("<-- HlasovaniPoslance")

    def nacti_hlasovani_poslance(self):
        header = {
            'id_poslanec': MItem('Int64', 'Identifikátor poslance, viz Poslanci:id_poslanec'),
            'id_hlasovani': MItem('Int64', 'Identifikátor hlasování, viz Hlasovani:id_hlasovani'),
            'vysledek__ORIG': MItem('string',"Hlasování jednotlivého poslance. 'A' - ano, 'B' nebo 'N' - ne, 'C' - zdržel se (stiskl tlačítko X), 'F' - nehlasoval (byl přihlášen, ale nestiskl žádné tlačítko), '@' - nepřihlášen, 'M' - omluven, 'W' - hlasování před složením slibu poslance, 'K' - zdržel se/nehlasoval. Viz úvodní vysvětlení zpracování výsledků hlasování.")
        }

        # Hlasovani poslance může být ve více souborech
        frames = []
        for f in self.paths['hlasovani_poslance']:
          frames.append(pd.read_csv(f, sep="|", names = header,  index_col=False, encoding='cp1250'))

        _df = pd.concat(frames, ignore_index=True)
        df = pretypuj(_df, header, name='hlasovani_poslance')
        self.rozsir_meta(header, tabulka='hlasovani_poslance', vlastni=False)

        mask = {'A': 'ano', 'B': 'ne', 'N': 'ne', 'C': 'zdržení se', 'F': 'nehlasování', '@': 'nepřihlášení', 'M': 'omluva', 'W': 'hlasování bez slibu', 'K': 'zdržení/nehlasování'}
        df['vysledek'] = mask_by_values(df.vysledek__ORIG, mask)
        self.meta['vysledek'] = dict(popis='Hlasování jednotlivého poslance.', tabulka='hlasovani_poslance', vlastni=True)

        return df, _df
