
import pytz

import pandas as pd

from utility import *

from Snemovna import *
from Osoby import *

from setup_logger import log

class Hlasovani(Organy):

    def __init__(self, volebni_obdobi, data_dir='.', stahni=False):
        super(Hlasovani, self).__init__(data_dir=data_dir, stahni=stahni)

        self.volebni_obdobi = volebni_obdobi

        self.nastav_datovy_zdroj(f"https://www.psp.cz/eknih/cdrom/opendata/hl-{self.volebni_obdobi}ps.zip")

        # Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1302
        # Souhrnné informace o hlasování
        self.paths['hlasovani'] = f"{data_dir}/hl{volebni_obdobi}s.unl"
        # Výsledek hlasování jednotlivého poslance
        #self.paths['hlasovani_poslance'] = [f"{data_dir}/hl{volebni_obdobi}h1.unl", f"{data_dir}/hl{volebni_obdobi}h2.unl"]
        # Časové ohraničení omluv poslanců z jednání Poslanecké sněmovny.
        #self.paths['omluvy'] = f"{data_dir}/omluvy.unl"
        # Zpochybnění výsledků hlasování a případné opakované hlasování
        self.paths['zpochybneni'] = f"{data_dir}/hl{volebni_obdobi}z.unl"
        # Poslanci, kteří oznámili zpochybnění hlasování
        #self.paths['zpochybneni_poslancem'] = f"{data_dir}/hl{volebni_obdobi}x.unl"
        # Hlasování, která byla prohlášena za zmatečné, tj. na jejich výsledek nebyl brán zřetel
        self.paths['zmatecne'] = f"{data_dir}/zmatecne.unl"
        # Vazba mezi stenozázamem a hlasováním, tj. ve kterém stenozáznamu proběhlo hlasování
        self.paths['stenozaznam'] = f"{data_dir}/hl{volebni_obdobi}v.unl"

        if len(self.missing_files()) > 0 or stahni:
            self.stahni()

        # Načti datové tabulky a připrav odvozené dataové tabulky
        self.hlasovani, self._hlasovani = self.nacti_hlasovani()
        self.zmatecne, self._zmatecne = self.nacti_zmatecne()
        self.zpochybneni, self._zpochybneni = self.nacti_zpochybneni()
        self.stenozaznam, self._stenozaznam = self.nacti_stenozaznam() # Tato tabulka je pro současnou sněmovnu (2017) momentálně nevyplněná

        self.hlasovani['zpochybneni_IND'] = self.hlasovani.id_hlasovani.isin(self.zpochybneni.id_hlasovani.unique())

        self.hlasovani['zmatecne_IND'] = self.hlasovani.id_hlasovani.isin(self.zmatecne.id_hlasovani.unique())

        # Připoj informace o stenozaznamu (pro snemovnu 2017 nefunguje, protože tabulka neobsahuje data pro aktualni ids)
        #self.df = pd.merge(left=self.df,right=self.stenozaznam, left_index=True, right_index=True, how='left', indicator="stenozaznam_merge")
        #self.df['stenozaznam_IND'] = self.df.stenozaznam_merge.astype(str).mask(self.df.stenozaznam_merge == 'both', True).mask(self.df.stenozaznam_merge == 'left_only', False)

        self.hlasovani['stenozaznam_IND'] = self.hlasovani.id_hlasovani.isin(self.stenozaznam.id_hlasovani.unique())

        self.df = self.hlasovani

    def nacti_hlasovani(self):
        header = {
            'id_hlasovani': 'Int64',
            'id_organ': 'Int64',
            'schuze': 'Int64',
            'cislo': 'Int64',
            'bod': 'Int64',
            'datum': 'string',
            'cas': 'string',
            "pro": 'Int64',
            "proti": 'Int64',
            "zdrzel": 'Int64',
            "nehlasoval": 'Int64',
            "prihlaseno": 'Int64',
            "kvorum": 'Int64',
            "druh_hlasovani": 'string',
            "vysledek": 'string', # we could use the 'category' dtype, but string might be more failsafe
            "nazev_dlouhy": 'string',
            "nazev_kratky": 'string'
        }

        # doporučené kódování 'cp1250' nefunguje, detekované 'ISO-8859-1' také nefunguje, 'ISO-8859-2' funguje.
        _df = pd.read_csv(self.paths['hlasovani'], sep="|", names = header.keys(),  index_col=False, encoding='ISO-8859-2')
        df = self.pretipuj(_df, header, name='hlasovani')

        # Oveř, že id_hlasovani lze použít jako index
        # Unikátnost identifikátoru hlasování je jednou z nutných podmínek konzistence dat
        #assert df.index.size == df.id_hlasovani.nunique()
        #df = df.set_index('id_hlasovani')

        # Odstraň whitespace z řetězců
        df = strip_all_string_columns(df)

        # Přidej sloupec typu 'datetime'
        df['datetime'] = pd.to_datetime(df['datum'] + ' ' + df['cas'], format='%d.%m.%Y %H:%M')
        tzn = pytz.timezone('Europe/Prague')
        df['datetime'] = df['datetime'].dt.tz_localize(tzn)

        # Bod pořadu schůze; je-li menší než 1, pak jde o procedurální hlasování nebo o hlasování k bodům, které v době hlasování neměly přiděleno číslo.
        df["bod_CAT"] = df.bod.mask(df.bod < 1, 'procedurální nebo bez přiděleného čísla').mask(df.bod >= 1, "normální")

        # Výsledek: A - přijato, R - zamítnuto, jinak zmatečné hlasování
        df["vysledek_CAT"] = df.vysledek.mask(df.vysledek == 'A', 'přijato').mask(df.vysledek == 'R', 'zamítnuto')

        # Druh hlasování: N - normální, R - ruční (nejsou známy hlasování jednotlivých poslanců)
        df["druh_hlasovani_CAT"] = df.druh_hlasovani.mask(df.druh_hlasovani == 'N', 'normální').mask(df.druh_hlasovani == 'R', 'ruční')

        return df, _df

    # Načti tabulku zmatecneho hlasovani
    # Výsledky zmatečného hlasování by se neměly vesměs brát v úvahu
    def nacti_zmatecne(self):
        header = {"id_hlasovani": "Int64"}

        _df = pd.read_csv(self.paths['zmatecne'], sep="|", names = header.keys(),  index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, name='zmatecne')

        return df, _df

    # Načti tabulku zpochybneni hlasovani (hl_check)
    def nacti_zpochybneni(self):
        header = { "id_hlasovani": 'Int64', "turn": 'Int64', "mode": 'Int64', "id_h2": 'Int64', "id_h3": 'Int64'}

        _df = pd.read_csv(self.paths['zpochybneni'], sep="|", names = header.keys(),  index_col=False, encoding='cp1250')
        #df = df.astype(header) # tohle dělá problém v jedné vizualizaci
        #df = _df
        df = df = self.pretipuj(_df, header, name='zpochybneni')

        # 0 - žádost o opakování hlasování - v tomto případě se o této žádosti neprodleně hlasuje a teprve je-li tato žádost přijata, je hlasování opakováno;
        # 1 - pouze sdělení pro stenozáznam, není požadováno opakování hlasování.
        semanticka_maska = {0: "žádost o opakování hlasování", 1: "pouze sdělení pro stenozáznam"}
        df["mode_CAT"] = mask_by_values(df["mode"], semanticka_maska)

        return df, _df

    def nacti_stenozaznam(self):
        # načti tabulku vazeb hlasovani na stenozaznam
        header = { "id_hlasovani": 'int', "turn": 'int', "typ": 'int'}
        _df = pd.read_csv(self.paths['stenozaznam'], sep="|", names = header.keys(),  index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, name='stenozaznam')

        df["typ_CAT"] = mask_by_values(df["typ"], {0: "hlasovani zmíněno v stenozáznamu", 1: "hlasování není zmíněno v stenozáznamu"})

        return df, _df

class ZmatecneHlasovani(Hlasovani):
    def __init__(self, volebni_obdobi, data_dir='.', stahni=False):
        super().__init__(volebni_obdobi, data_dir, stahni)

        suffix = "__hlasovani"
        self.zmatecne = pd.merge(left=self.zmatecne, right=self.hlasovani, on='id_hlasovani', suffixes = ("", suffix), how='left')
        self.zmatecne = drop_by_inconsistency(self.zmatecne, suffix, 0.1)

        # Heuristika, která vyděluje zpochybneni hlasování pro dané volební období.
        # V datech tato informace explicitně není.
        min_id = self.hlasovani.id_hlasovani.min()
        max_id = self.hlasovani.id_hlasovani.max()
        self.zmatecne = self.zmatecne[(self.zmatecne.id_hlasovani >= min_id) & (self.zmatecne.id_hlasovani <= max_id)]

        self.df = self.zmatecne

class ZpochybneniHlasovani(Hlasovani):
    def __init__(self, volebni_obdobi, data_dir='.', stahni=False):
        super().__init__(volebni_obdobi, data_dir, stahni)

        suffix = "__hlasovani"
        self.zpochybneni = pd.merge(left=self.zpochybneni, right=self.hlasovani, on='id_hlasovani', suffixes = ("", suffix), how='left')
        self.zpochybneni = drop_by_inconsistency(self.zpochybneni, suffix, 0.1)

        # Heuristika, která vyděluje zpochybneni hlasování pro dané volební období.
        # V datech tato informace explicitně není.
        min_id = self.hlasovani.id_hlasovani.min()
        max_id = self.hlasovani.id_hlasovani.max()
        self.zpochybneni = self.zpochybneni[(self.zpochybneni.id_hlasovani >= min_id) & (self.zpochybneni.id_hlasovani <= max_id)]

        self.df = self.zpochybneni


class ZpochybneniPoslancem(ZpochybneniHlasovani, Osoby):
    def __init__(self, volebni_obdobi, data_dir='.', stahni=False):
        super(ZpochybneniPoslancem, self).__init__(volebni_obdobi, data_dir=data_dir, stahni=stahni)

        self.volebni_obdobi = volebni_obdobi
        self.nastav_datovy_zdroj(f"https://www.psp.cz/eknih/cdrom/opendata/hl-{volebni_obdobi}ps.zip")

        # Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1302
        # Poslanci, kteří oznámili zpochybnění hlasování
        self.paths['zpochybneni_poslancem'] = f"{data_dir}/hl{volebni_obdobi}x.unl"

        if len(self.missing_files()) > 0 or stahni:
            self.stahni()

        self.zpochybneni_poslancem, self._zpochybneni_poslancem = self.nacti_zpochybneni_poslancem()

        # Připoj informace o zpochybneni hlasovani
        # Připojuje se tabulka 'hlasovani', nikoliv 'zpochybneni_hlasovani', protože není možné mapovat řádky 'zpochybneni_hlasovani' na 'zpochybneni_poslancem'. Jedná se zřejmě o nedokonalost datového modelu.
        suffix = "__hlasovani"
        self.zpochybneni_poslancem = pd.merge(left=self.zpochybneni_poslancem, right=self.hlasovani, on='id_hlasovani', suffixes = ("", suffix), how='left')
        self.zpochybneni_poslancem = drop_by_inconsistency(self.zpochybneni_poslancem, suffix, 0.1)

        # Připoj informace o osobe # TODO: Neměli by se připojovat spíš Poslanci než Osoby?
        suffix = "__osoby"
        self.zpochybneni_poslancem = pd.merge(left=self.zpochybneni_poslancem, right=self.osoby, on='id_osoba', suffixes = ("", suffix), how='left')
        self.zpochybneni_poslancem = drop_by_inconsistency(self.zpochybneni_poslancem, suffix, 0.1)

        id_organu_dle_volebniho_obdobi = self.organy[(self.organy.nazev_organu_cz == 'Poslanecká sněmovna') & (self.organy.od_organ.dt.year == self.volebni_obdobi)].iloc[0].id_organ
        self.zpochybneni_poslancem = self.zpochybneni_poslancem[self.zpochybneni_poslancem.id_organ == id_organu_dle_volebniho_obdobi]

        self.df = self.zpochybneni_poslancem

    def nacti_zpochybneni_poslancem(self):
        header = {
            # Identifikátor hlasování, viz hl_hlasovani:id_hlasovani a hl_check:id_hlasovani, které bylo zpochybněno.
            "id_hlasovani": 'Int64',
            # Identifikátor poslance, který zpochybnil hlasování; viz osoby:id_osoba.
            "id_osoba": 'Int64',
            # Typ zpochybnění, viz hl_check:mode.
            "mode": 'Int64'
        }

        _df = pd.read_csv(self.paths['zpochybneni_poslancem'], sep="|", names = header.keys(),  index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header)

        return df, _df
