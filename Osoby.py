
from os import path
import pandas as pd

from utility import *

from Snemovna import *

from setup_logger import log


class TypOrganu(Snemovna):

    def __init__(self, data_dir=".", stahni=False):
        log.debug("--> TypOrganu")
        super(TypOrganu, self).__init__(data_dir=data_dir)

        self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        # Organy - cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1301
        # Orgány mají svůj typ, tyto typy mají hiearchickou strukturu.
        self.paths['typ_organu'] = f"{data_dir}/typ_organu.unl"

        if len(self.missing_files()) > 0 or stahni:
            self.stahni()

        self.typ_organu, self._typ_organu = self.nacti_typ_organu()

        self.df = self.typ_organu

        log.debug("<-- TypOrganu")

    def nacti_typ_organu(self):
        header = {
            # Identifikátor typu orgánu
            'id_typ_org': 'Int64',
            # Identifikátor nadřazeného typu orgánu (typ_organu:id_typ_org), pokud je null či nevyplněno, pak nemá nadřazený typ
            'typ_id_typ_org': 'Int64',
            # Název typu orgánu v češtině
            'nazev_typ_org_cz': 'string',
            # Název typu orgánu v angličtině
            'nazev_typ_org_en': 'string',
            # Obecný typ orgánu, pokud je vyplněný, odpovídá záznamu v typ_organu:id_typ_org. Pomocí tohoto sloupce lze najít např. všechny výbory v různých typech zastupitelských sborů.
            'typ_org_obecny': 'Int64',
            # Priorita při výpisu
            'priorita': 'Int64'
        }

        _df = pd.read_csv(self.paths['typ_organu'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, name='typ_organu')

        # Oveř, že id_typ_org lze použít jako index
        # Unikátnost identifikátoru hlasování je jednou z nutných podmínek konzistence dat
        #assert df.index.size == df.id_typ_org.nunique()
        #df = df.set_index('id_typ_org')

        return df, _df

class Organy(TypOrganu):
    def __init__(self, data_dir='.', stahni=False):
        print("--> Organy")
        super(Organy, self).__init__(data_dir=data_dir, stahni=stahni)

        self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        # Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1301
        #Záznam mezi orgánem a typem funkce, názvy v funkce:nazev_funkce_LL se používají pouze interně,
        # slouží k definování pořadí funkcionářů v případech, kdy je toto pořadí určeno.
        self.paths['funkce'] = f"{data_dir}/funkce.unl"
        # Některé orgány mají nadřazený orgán a pak je položka organy:organ_id_organ vyplněna,
        # přičemž pouze v některých případech se tyto vazby využívají.
        self.paths['organy'] = f"{data_dir}/organy.unl"

        if len(self.missing_files()) > 0 or stahni:
            self.stahni()

        self.organy, self._organy = self.nacti_organy()

        # Připoj Typu orgánu
        suffix = "__typ_organu"
        self.organy = pd.merge(left=self.organy, right=self.typ_organu, left_on="id_typ_organu", right_on="id_typ_org", suffixes=("",suffix), how='left')
        self.organy = drop_by_inconsistency(self.organy, suffix, 0.1)

        self.df = self.organy

        print("<-- Organy")

    def nacti_organy(self):
        header = {
            # Identifikátor orgánu
            "id_organ": 'Int64',
            # Identifikátor nadřazeného orgánu, viz organy:id_organ
            "organ_id_organ": 'Int64',
            # Typ orgánu, viz typ_organu:id_typ_organu
            "id_typ_organu": 'Int64',
            # Zkratka orgánu, bez diakritiky, v některých připadech se zkratka při zobrazení nahrazuje jiným názvem
            "zkratka": 'string',
            # Název orgánu v češtině
            "nazev_organu_cz": "string",
            # Název orgánu v angličtině
            "nazev_organu_en": "string",
            # Ustavení orgánu
            "od_organ":  'datetime64[ns]',
            # Ukončení orgánu
            "do_organ":  'datetime64[ns]',
            # Priorita výpisu orgánů
            "priorita":  'Int64',
            # Pokud je nastaveno na 1, pak při výpisu členů se nezobrazují záznamy v tabulkce zarazeni kde cl_funkce == 0.
            # Toto chování odpovídá tomu, že v některých orgánech nejsou členové a teprve z nich se volí funkcionáři, ale přímo se volí do určité funkce.
            "cl_organ_base": 'Int64'
        }

        _df = pd.read_csv(self.paths['organy'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, name='organy')

        return df, _df

class TypFunkce(TypOrganu):
    def __init__(self, data_dir=".", stahni=False):
        print("--> TypFunkce")
        super(TypFunkce, self).__init__(data_dir=data_dir, stahni=False)

        self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        # Organy - cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1301
        # Orgány mají svůj typ, tyto typy mají hiearchickou strukturu.
        self.paths['typ_funkce'] = f"{data_dir}/typ_funkce.unl"
        print(self.paths)
        if len(self.missing_files()) > 0 or stahni:
            self.stahni()

        self.typ_funkce, self._typ_funkce = self.nacti_typ_funkce()

        semanticka_maska = {1: "předseda", 2: "místopředseda", 3: "ověřovatel"}
        self.typ_funkce['typ_funkce_obecny_CAT'] = mask_by_values(self.typ_funkce.typ_funkce_obecny, semanticka_maska)

        #print(f"Odstraňuji sloupce: {set(self.typ_funkce.columns).intersection(self.typ_organu.columns)}")
        #labels = ["priorita"]
        #self.typ_funkce.drop(labels=labels, inplace=True, axis=1)
        # Připoj Typu orgánu
        suffix="__typ_organu"
        self.typ_funkce = pd.merge(left=self.typ_funkce, right=self.typ_organu, on="id_typ_org", suffixes=("", suffix), how='left')
        self.typ_funkce = drop_by_inconsistency(self.typ_funkce, suffix, 0.1)

        #self.typ_funkce.drop(labels=labels, inplace=True, axis=1)

        self.df = self.typ_funkce
        print("<-- TypFunkce")

    def nacti_typ_funkce(self):
        header = {
            # Identifikator typu funkce
            'id_typ_funkce': 'Int64',
            # Identifikátor typu orgánu, viz typ_organu:id_typ_org
            'id_typ_org': 'Int64',
            # Název typu funkce v češtině
            'typ_funkce_cz': 'string',
            # Název typu funkce v angličtině
            'typ_funkce_en': 'string',
            # Priorita při výpisu
            'priorita': 'Int64',
            # Obecný typ funkce, 1 - předseda, 2 - místopředseda, 3 - ověřovatel, jiné hodnoty se nepoužívají.
            'typ_funkce_obecny': 'Int64',

        }

        _df = pd.read_csv(self.paths['typ_funkce'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, name='typ_funkce')

        # Oveř, že id_typ_org lze použít jako index
        # Unikátnost identifikátoru hlasování je jednou z nutných podmínek konzistence dat
        assert df.index.size == df.id_typ_funkce.nunique()
        df = df.set_index('id_typ_org')

        return df, _df

class Funkce(Organy, TypFunkce):
    def __init__(self, data_dir='.', stahni=False):
        print("--> Funkce")
        super(Funkce, self).__init__(data_dir=data_dir, stahni=stahni)

        self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        # Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1301
        #Záznam mezi orgánem a typem funkce, názvy v funkce:nazev_funkce_LL se používají pouze interně,
        # slouží k definování pořadí funkcionářů v případech, kdy je toto pořadí určeno.
        self.paths['funkce'] = f"{data_dir}/funkce.unl"

        if len(self.missing_files()) > 0 or stahni:
            self.stahni()

        self.funkce, self._funkce = self.nacti_funkce()

        # Připoj Orgány
        suffix = "__organy"
        self.funkce = pd.merge(left=self.funkce, right=self.organy, on='id_organ', suffixes=("", suffix), how='left')
        self.funkce =  drop_by_inconsistency(self.funkce, suffix, 0.1)

        # Připoj Typ funkce
        suffix = "__typ_funkce"
        self.funkce = pd.merge(left=self.funkce, right=self.typ_funkce, on="id_typ_funkce", suffixes=("", suffix), how='left')
        self.funkce = drop_by_inconsistency(self.funkce, suffix, 0.1)

        self.df = self.funkce

        print("<-- Funkce")

    def nacti_funkce(self):
        header = {
            # Identifikátor funkce, používá se v zarazeni:id_fo
            "id_funkce": 'Int64',
            # Identifikátor orgánu, viz organy:id_organ
            "id_organ": 'Int64',
            # Typ funkce, viz typ_funkce:id_typ_funkce
            "id_typ_funkce": 'Int64',
            # Název funkce, pouze pro interní použití
            "nazev_funkce_cz": 'string',
            # Priorita výpisu
            "priorita":  'Int64',
        }

        _df = pd.read_csv(self.paths['funkce'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, name='funkce')

        return df, _df

class Osoby(Snemovna):
    def __init__(self, data_dir='.', stahni=False):
        print("--> Osoby")
        super(Osoby, self).__init__(data_dir=data_dir)

        self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        # Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1301
        # Jména osob, které jsou zařazeni v orgánech.
        # Vzhledem k tomu, že k jednoznačnému rozlišení osob často není dostatek informací,
        # je možné, že ne všechny záznamy odkazují na jedinečné osoby, tj. některé osoby jsou v tabulce vícekrát.
        self.paths['osoby'] = f"{data_dir}/osoby.unl"
        # Zařazení v orgánu nebo data funkcí osoby v orgánu. Pokud je zarazeni:do_o typu null, pak jde o aktuální zařazení.
        #self.paths['osoby_zarazeni'] = f"{data_dir}/zarazeni.unl"
        # Obsahuje vazby na externí systémy. Je-li typ = 1, pak jde o vazbu na evidenci senátorů na senat.cz
        self.paths['osoba_extra'] = f"{data_dir}/osoba_extra.unl"

        if len(self.missing_files()) > 0 or stahni:
            self.stahni()

        self.osoby, self._osoby = self.nacti_osoby()
        self.osoba_extra, self.osoba_extra = self.nacti_osoba_extra()

        self.df = self.osoby

        print("<-- Osoby")

    def nacti_osoby(self):
        # Obsahuje jména osob, které jsou zařazeni v orgánech.
        # Vzhledem k tomu, že k jednoznačnému rozlišení osob často není dostatek informací, je možné, že ne všechny záznamy odkazují na jedinečné osoby, tj. některé osoby jsou v tabulce vícekrát.
        header = {
            "id_osoba": "Int64",
            "pred": 'string',
            "prijmeni": 'string',
            "jmeno": 'string',
            "za": 'string',
            "narozeni": 'datetime64[ns]',
            'pohlavi': 'string', # Pohlaví, "M" jako muž, ostatní hodnoty žena
            "zmena": 'datetime64[ns]',
            "umrti": 'datetime64[ns]'
        }
        _df = pd.read_csv(self.paths['osoby'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'osoby')

        return df, _df


    def nacti_osoba_extra(self):
    # Tabulka obsahuje vazby na externí systémy. Je-li typ = 1, pak jde o vazbu na evidenci senátorů na senat.cz
        header = {
            # Identifikátor osoby, viz osoba:id_osoba
            'id_osoba': 'Int64',
            # Identifikátor orgánu, viz org:id_org
            'id_org': 'Int64',
            # Typ záznamu, viz výše. ??? Asi chtěli napsat níže ....
            'typ': 'Int64',
            # Je-li typ = 1, pak jde o číslo senátního obvodu
            'obvod': 'Int64',
            # Je-li typ = 1, pak jde o název volební strany/hnutí či označení nezávislého kandidáta
            'strana': 'string',
            # Je-li typ = 1, pak je to identifikátor senátora na senat.cz
            'id_external': 'Int64'
        }

        _df = pd.read_csv(self.paths['osoba_extra'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'osoba_extra')

        return df, _df

class Poslanec(Osoby, Organy):

    def __init__(self, volebni_obdobi, data_dir='.', stahni=False):
        print("--> Poslanec")
        super(Poslanec, self).__init__(data_dir=data_dir)

        self.volebni_obdobi = volebni_obdobi

        self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        # Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1301
        # Další informace o poslanci vzhledem k volebnímu období: kontaktní údaje, adresa regionální kanceláře a podobně.
        # Některé údaje jsou pouze v aktuálním volebním období.
        self.paths['poslanec'] = f"{data_dir}/poslanec.unl"
        # Obsahuje GPS souřadnice regionálních kanceláří poslanců.
        self.paths['pkgps'] = f"{data_dir}/pkgps.unl"

        if len(self.missing_files()) > 0 or stahni:
            self.stahni()

        self.poslanec, self._poslanec = self.nacti_poslance()
        self.pkgps, self._pkgps = self.nacti_pkgps()

        #self.poslanec = pd.merge(self.poslanec, self.osoby,  on='id_osoba')
        #poslanci_df = pd.merge(poslanci_df, osoby_zarazeni_df,  on='id_osoba')

        #posledni_volebni_obdobi = poslanci_df.id_obdobi.max()
        #poslanci_df = poslanci_df[poslanci_df.id_obdobi == posledni_volebni_obdobi]
        # Připoj informace o osobe
        suffix = "__osoby"
        self.poslanec = pd.merge(left=self.poslanec, right=self.osoby, on='id_osoba', suffixes = ("", suffix), how='left')
        self.poslanec = drop_by_inconsistency(self.poslanec, suffix, 0.1)

        # Připoj informace o kanceláři
        suffix = "__pkgps"
        self.poslanec = pd.merge(left=self.poslanec, right=self.pkgps, on='id_poslanec', suffixes = ("", suffix), how='left')
        self.poslanec = drop_by_inconsistency(self.poslanec, suffix, 0.1)

        # Zúžení na volební období
        id_organu_dle_volebniho_obdobi = self.organy[(self.organy.nazev_organu_cz == 'Poslanecká sněmovna') & (self.organy.od_organ.dt.year == self.volebni_obdobi)].iloc[0].id_organ
        self.poslanec = self.poslanec[self.poslanec.id_obdobi == id_organu_dle_volebniho_obdobi]

        self.df = self.poslanec

        print("<-- Poslanec")

    def nacti_poslance(self):
        header = {
            "id_poslanec": 'Int64',
            "id_osoba":  'Int64',
            "id_kraj":  'Int64',
            "id_kandidatka":  'Int64',
            "id_obdobi":  'Int64',
            "web": 'string',
            "ulice": 'string',
            "obec": 'string',
            "psc": 'string',
            "email": 'string',
            "telefon": 'string',
            "fax": 'string',
            "psp_telefon": 'string',
            "facebook": 'string',
            "foto":  'Int64'
        }

        _df = pd.read_csv(self.paths['poslanec'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'poslanec')

        return df, _df


    def nacti_pkgps(self):
    # Tabulka obsahuje GPS souřadnice regionálních kanceláří poslanců.
        header = {
            # Identifikátor poslance, viz poslanec:id_poslanec
            'id_poslanec': 'Int64',
            # Adresa kanceláře, jednotlivé položky jsou odděleny středníkem
            'adresa': 'string',
            # Severní šířka, WGS 84, formát GG.AABBCCC, GG = stupně, AA - minuty, BB - vteřiny, CCC - tisíciny vteřin
            'sirka': 'string',
            # Východní délka, WGS 84, formát GG.AABBCCC, GG = stupně, AA - minuty, BB - vteřiny, CCC - tisíciny vteřin
            'delka': 'string'
        }
        _df = pd.read_csv(self.paths['pkgps'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'pkgps')

        return df, _df
