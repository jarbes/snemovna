
# Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1302

from glob import glob
import pytz

import pandas as pd

from snemovna.utility import *

from snemovna.Snemovna import Snemovna
from snemovna.PoslanciOsoby import Osoby, Organy, Poslanci

from snemovna.setup_logger import log


# Agenda hlasování eviduje hlasování Poslanecké sněmovny, většinou prováděné hlasovacím zařízením. Ze seznamu hlasování jsou vyjmuta hlasování během neveřejných jednání Poslancké sněmovny.
# Pojem přihlášen znamená přihlášen k hlasovacímu zařízení a teprve v tomto stavu je jeho hlasování bráno v potaz. Zdržel se znamená stisknutí tlačítka X během hlasování, nehlasoval znamená nestisknutí žádného tlačítka během hlasování. Za výsledek hlasování poslance se bere naposledy stisknuté tlačítko hlasovacího zařízení během časového intervalu hlasování (20 sekund), neboli, poslanec může během této doby libovolně změnit svoje hlasování.
# Výsledek hlasování je stav výsledků hlasování jednotlivých poslanců na konci časového intervalu hlasování. Tj. pokud se poslanec během časového intervalu hlasování odhlásí od hlasovacího zařízení, je uveden jako nepřihlášen, poslanci se mohou přihlašovat do hlasovacího zařízení i během časového intervalu hlasování.
# Od účinnosti novely jednacího řádu 90/1995 Sb. se nerozlišuje zdržel se a nehlasoval, tj. příslušné počty se sčítají.
# Pokud skončí v průběhu volebního období poslanci mandát, ihned vzniká mandát jeho náhradníkovi, který se ujímá mandátu po složení slibu na první schůzi, které se zúčastní. Mezitím je ve výsledcích hlasování veden jako nepřihlášen (výsledek 'W').
# Upozornění: data omluv se mohou doplňovat se zpožděním a tedy počty omluvených se mohou lišit. Na výsledek hlasování to nemá žádný vliv, během hlasování není seznam omluvených k dispozici a omluvení poslanci jsou vedeni jako nepřihlášen.

class HlasovaniObecne(Snemovna):

    def __init__(self, *args, **kwargs):
        log.debug("--> HlasovaniObecne")

        super(HlasovaniObecne, self).__init__(*args, **kwargs)

        self.nastav_datovy_zdroj(f"https://www.psp.cz/eknih/cdrom/opendata/hl-{self.volebni_obdobi}ps.zip")

        self.stahni_data()

        log.debug("<-- HlasovaniObecne")


class Hlasovani(HlasovaniObecne, Organy):

    def __init__(self, *args, **kwargs):
        log.debug("--> Hlasovani")
        super(Hlasovani, self).__init__(*args, **kwargs)

        # Souhrnné informace o hlasování
        self.paths['hlasovani'] = f"{self.data_dir}/hl{self.volebni_obdobi}s.unl"
        # Zpochybnění výsledků hlasování a případné opakované hlasování
        self.paths['zpochybneni'] = f"{self.data_dir}/hl{self.volebni_obdobi}z.unl"
        # Hlasování, která byla prohlášena za zmatečné, tj. na jejich výsledek nebyl brán zřetel
        self.paths['zmatecne'] = f"{self.data_dir}/zmatecne.unl"
        # Vazba mezi stenozázamem a hlasováním
        self.paths['stenozaznam'] = f"{self.data_dir}/hl{self.volebni_obdobi}v.unl"

        # Načtení datových tabulek
        self.zmatecne, self._zmatecne = self.nacti_zmatecne()
        self.zpochybneni, self._zpochybneni = self.nacti_zpochybneni()
        self.stenozaznam, self._stenozaznam = self.nacti_stenozaznam() # Nemusí být aktuální. Například pro sněmovnu 2017 je tabulka nevyplněná.
        self.hlasovani, self._hlasovani = self.nacti_hlasovani()

        # Zúžení dat na zvolené volební období. Tohle je asi nejjednodušší způsob.
        min_id = self.hlasovani.id_hlasovani.min()
        max_id = self.hlasovani.id_hlasovani.max()
        self.zmatecne = self.zmatecne[(self.zmatecne.id_hlasovani >= min_id) & (self.zmatecne.id_hlasovani <= max_id)]
        self.zpochybneni = self.zpochybneni[(self.zpochybneni.id_hlasovani >= min_id) & (self.zpochybneni.id_hlasovani <= max_id)]
        self.stenozaznam = self.stenozaznam[(self.stenozaznam.id_hlasovani >= min_id) & (self.stenozaznam.id_hlasovani <= max_id)]

        # Přidání indikátorů
        self.hlasovani['je_zpochybneni'] = self.hlasovani.id_hlasovani.isin(self.zpochybneni.id_hlasovani.unique())
        self.meta['je_zpochybneni'] = dict(popis='Indikátor zpochybnění hlasování', tabulka='df', vlastni=True)

        self.hlasovani['je_zmatecne'] = self.hlasovani.id_hlasovani.isin(self.zmatecne.id_hlasovani.unique())
        self.meta['je_zmatecne'] = dict(popis='Indikátor zmatečného hlasování', tabulka='df', vlastni=True)

        # Připojení informací o stenozaznamu. Pozor, nemusí být aktuální. Například pro snemovnu 2017 momentálně (16.2.2021) data chybí.
        self.hlasovani['ma_stenozaznam'] = self.hlasovani.id_hlasovani.isin(self.stenozaznam.id_hlasovani.unique())
        self.meta['ma_stenozaznam'] = dict(popis='Indikátor existence stenozáznamu', tabulka='df', vlastni=True)

        suffix = '__stenozaznam'
        self.hlasovani = pd.merge(left=self.hlasovani, right=self.stenozaznam, on="id_hlasovani", how="left", suffixes=('', suffix))
        self.drop_by_inconsistency(self.hlasovani, suffix, 0.1, 'hlasovani', 'stenozaznam', inplace=True)

        self.df = self.hlasovani
        self.nastav_meta()

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

    def nacti_stenozaznam(self):
        ''' Načte tabulku vazeb hlasovani na stenozaznam.'''

        header = {
            "id_hlasovani": MItem('Int64', 'Identifikátor hlasování, viz hl_hlasovani:id_hlasovani'),
            "turn": MItem('Int64', 'Číslo stenozáznamu'),
            "typ__ZDROJ": MItem('Int64', 'Typ vazby: 0 - hlasování je v textu explicitně zmíněno a lze tedy vytvořit odkaz přímo na začátek hlasování, 1 - hlasování není v textu explicitně zmíněno, odkaz lze vytvořit pouze na stenozáznam jako celek.')
        }
        _df = pd.read_csv(self.paths['stenozaznam'], sep="|", names = header.keys(),  index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, name='stenozaznam')

        self.rozsir_meta(header, tabulka='stenozaznam', vlastni=False)

        # Interpretuj 'typ'
        df["typ"] = mask_by_values(df.typ__ZDROJ, {0: "hlasovani zmíněno v stenozáznamu", 1: "hlasování není zmíněno v stenozáznamu"}).astype('string')
        self.meta['typ'] = dict(popis='Typ vazby na stenozáznam.', tabulka='stenozaznam', vlastni=True)

        return df, _df


class ZmatecneHlasovani(Hlasovani):

    def __init__(self, *args, **kwargs):
        log.debug("--> ZmatecneHlasovani")

        super().__init__(*args, **kwargs)

        suffix = "__hlasovani"
        self.zmatecne = pd.merge(left=self.zmatecne, right=self.hlasovani, on='id_hlasovani', suffixes = ("", suffix), how='left')
        self.zmatecne = self.drop_by_inconsistency(self.zmatecne, suffix, 0.1, 'zmatecne', 'hlasovani')

        self.df = self.zmatecne
        self.nastav_meta()

        log.debug("<-- ZmatecneHlasovani")


class ZpochybneniHlasovani(Hlasovani):

    def __init__(self, *args, **kwargs):
        log.debug("--> ZpochybneniHlasovani")

        super().__init__(*args, **kwargs)

        suffix = "__hlasovani"
        self.zpochybneni = pd.merge(left=self.zpochybneni, right=self.hlasovani, on='id_hlasovani', suffixes = ("", suffix), how='left')
        self.zpochybneni = self.drop_by_inconsistency(self.zpochybneni, suffix, 0.1, 'zpochybneni', 'hlasovani')

        self.df = self.zpochybneni
        self.nastav_meta()

        log.debug("<-- ZpochybneniHlasovani")


class ZpochybneniPoslancem(ZpochybneniHlasovani, Osoby):

    def __init__(self, *args, **kwargs):
        log.debug("--> ZpochybneniPoslancem")

        super(ZpochybneniPoslancem, self).__init__(*args, **kwargs)

        # Poslanci, kteří oznámili zpochybnění hlasování
        self.paths['zpochybneni_poslancem'] = f"{self.data_dir}/hl{self.volebni_obdobi}x.unl"

        self.zpochybneni_poslancem, self._zpochybneni_poslancem = self.nacti_zpochybneni_poslancem()

        # Připojuje se tabulka 'hlasovani', nikoliv 'zpochybneni_hlasovani', protože není možné mapovat řádky 'zpochybneni_hlasovani' na 'zpochybneni_poslancem'. Jedná se zřejmě o nedokonalost datového modelu.
        suffix = "__hlasovani"
        self.zpochybneni_poslancem = pd.merge(left=self.zpochybneni_poslancem, right=self.hlasovani, on='id_hlasovani', suffixes = ("", suffix), how='left')
        self.drop_by_inconsistency(self.zpochybneni_poslancem, suffix, 0.1, 'zpochybneni_poslancem', 'hlasovani', inplace=True)

        # Připoj informace o osobe # TODO: Neměli by se připojovat spíš Poslanci než Osoby?
        suffix = "__osoby"
        self.zpochybneni_poslancem = pd.merge(left=self.zpochybneni_poslancem, right=self.osoby, on='id_osoba', suffixes = ("", suffix), how='left')
        self.drop_by_inconsistency(self.zpochybneni_poslancem, suffix, 0.1, 'zpochybneni_poslancem', 'osoby', inplace=True)

        id_organu_dle_volebniho_obdobi = self.organy[(self.organy.nazev_organu_cz == 'Poslanecká sněmovna') & (self.organy.od_organ.dt.year == self.volebni_obdobi)].iloc[0].id_organ
        self.zpochybneni_poslancem = self.zpochybneni_poslancem[self.zpochybneni_poslancem.id_organ == id_organu_dle_volebniho_obdobi]

        self.df = self.zpochybneni_poslancem
        self.nastav_meta()

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


class Omluvy(HlasovaniObecne, Poslanci, Organy):

    def __init__(self, *args, **kwargs):
        log.debug("--> Omluvy")

        super(Omluvy, self).__init__(*args, **kwargs)

        self.paths['omluvy'] = f"{self.data_dir}/omluvy.unl"

        self.omluvy, self._omluvy = self.nacti_omluvy()

        # Připoj informace o poslanci
        suffix = "__poslanci"
        self.omluvy = pd.merge(left=self.omluvy, right=self.poslanci, on='id_poslanec', suffixes = ("", suffix), how='left')
        self.omluvy = self.drop_by_inconsistency(self.omluvy, suffix, 0.1, 'omluvy', 'poslanci')

        # Připoj Orgány
        suffix = "__organy"
        self.omluvy = pd.merge(left=self.omluvy, right=self.organy, on='id_organ', suffixes=("", suffix), how='left')
        self.organy =  self.drop_by_inconsistency(self.omluvy, suffix, 0.1, 'omluvy', 'organy')

        # Zúžení na volební období
        id_organu_dle_volebniho_obdobi = self.organy[(self.organy.nazev_organu_cz == 'Poslanecká sněmovna') & (self.organy.od_organ.dt.year == self.volebni_obdobi)].iloc[0].id_organ
        self.omluvy = self.omluvy[self.omluvy.id_obdobi == id_organu_dle_volebniho_obdobi]

        self.df = self.omluvy
        self.nastav_meta()

        log.debug("<-- Omluvy")

    def nacti_omluvy(self):
        # Tabulka zaznamenává časové ohraničení omluv poslanců z jednání Poslanecké sněmovny.
        # Omluvy poslanců sděluje předsedající na začátku nebo v průběhu jednacího dne.
        # Data z tabulky se použijí pouze k nahrazení výsledku typu '@', tj. pokud výsledek hlasování jednotlivého poslance je nepřihlášen, pak pokud zároveň čas hlasování spadá do časového intervalu omluvy, pak se za výsledek považuje 'M', tj. omluven.
        #Pokud je poslanec omluven a zároveň je přihlášen, pak výsledek jeho hlasování má přednost před omluvou.
        header = {
            "id_organ": MItem('Int64', 'Identifikátor volebního období, viz Organy:id_organ'),
            "id_poslanec": MItem('Int64', 'Identifikátor poslance, viz Poslanci:id_poslanec'),
            "den": MItem('string', 'Datum omluvy'),
            "od": MItem('string', 'Čas začátku omluvy, pokud je null, pak i omluvy:do je null a jedná se o omluvu na celý jednací den.'),
            "do": MItem('string', 'Čas konce omluvy, pokud je null, pak i omluvy:od je null a jedná se o omluvu na celý jednací den.')
        }

        _df = pd.read_csv(self.paths['omluvy'], sep="|", names = header,  index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'omluvy')
        self.rozsir_meta(header, tabulka='omluvy', vlastni=False)

        # TODO !!!!
        # Přidej sloupec typu 'datetime_from'
        #df['datetime_from'] = pd.to_datetime(df['den'] + ' ' + df['od'], format='%d.%m.%Y %H:%M')
        #df['datetime_from'] = df['datetime_from'].dt.tz_localize(self.tzn)

        # Přidej sloupec typu 'datetime_to'
        #df['datetime_to'] = pd.to_datetime(df['den'] + ' ' + df['do'], format='%d.%m.%Y %H:%M')
        #df['datetime_to'] = df['datetime_to'].dt.tz_localize(tzn)

        return df, _df


# TODO: not finished at all!!!
# Tabulka hl_poslanec
# Tabulka zaznamenává výsledek hlasování jednotlivého poslance.
class HlasovaniPoslance(Hlasovani, Poslanci, Organy):

    def __init__(self, *args, **kwargs):
        log.debug("--> HlasovaniPoslance")

        super(HlasovaniPoslance, self).__init__(*args, **kwargs)

        # V souborech uložena jako hlXXXXhN.unl, kde XXXX je reference volebního období a N je číslo části. V 6. a 7. volebním období obsahuje část č. 1 hlasování 1. až 50. schůze, část č. 2 hlasování od 51. schůze.
        self.paths['hlasovani_poslance'] = glob(f"{self.data_dir}/hl{self.volebni_obdobi}h*.unl")

        self.hlasovani_poslance, self._hlasovani_poslance = self.nacti_hlasovani_poslance()

        # Připoj Poslance
        to_skip = ['web', 'ulice', 'obec', 'psc', 'telefon', 'fax', 'psp_telefon', 'email', 'facebook', 'foto', 'zmena', 'umrti', 'adresa', 'sirka', 'delka', 'id_obdobi']
        self.hlasovani_poslance = pd.merge(left=self.hlasovani_poslance, right=self.poslanci, on="id_poslanec", suffixes=("", "__poslanci"), how='left')
        self.hlasovani_poslance.drop(columns = to_skip, inplace=True)
        self.drop_by_inconsistency(self.hlasovani_poslance, "__poslanci", 0.1, 'hlasovani_poslance', 'poslanci', inplace=True)

        self.df = self.hlasovani_poslance
        self.nastav_meta()

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
