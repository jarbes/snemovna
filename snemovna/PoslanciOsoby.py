
# Agenda eviduje osoby, jejich zařazení do orgánů a jejich funkce v orgánech a orgány jako takové.
# Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1301

from os import path
import pandas as pd

from snemovna.utility import *

from snemovna.Snemovna import Snemovna

from snemovna.setup_logger import log


class PoslanciOsobyObecne(Snemovna):

    def __init__(self, *args, **kwargs):
        log.debug("--> PoslanciOsobyObecne")
        super(PoslanciOsobyObecne, self).__init__(*args, **kwargs)
        self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        self.stahni_data()
        log.debug("<-- PoslanciOsobyObecne")


class TypOrganu(PoslanciOsobyObecne):

    def __init__(self, *args, **kwargs):
        log.debug("--> TypOrganu")
        super(TypOrganu, self).__init__(*args, **kwargs)

        # Orgány mají svůj typ, tyto typy mají hiearchickou strukturu.
        self.paths['typ_organu'] = f"{self.data_dir}/typ_organu.unl"

        #self.stahni_data()

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
    def __init__(self,  *args, **kwargs):
        log.debug("--> Organy")
        super(Organy, self).__init__(*args, **kwargs)

        #self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        # Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1301
        #Záznam mezi orgánem a typem funkce, názvy v funkce:nazev_funkce_LL se používají pouze interně,
        # slouží k definování pořadí funkcionářů v případech, kdy je toto pořadí určeno.
        self.paths['funkce'] = f"{self.data_dir}/funkce.unl"
        # Některé orgány mají nadřazený orgán a pak je položka organy:organ_id_organ vyplněna,
        # přičemž pouze v některých případech se tyto vazby využívají.
        self.paths['organy'] = f"{self.data_dir}/organy.unl"

        #self.stahni_data()

        self.organy, self._organy = self.nacti_organy()

        # Připoj Typu orgánu
        suffix = "__typ_organu"
        self.organy = pd.merge(left=self.organy, right=self.typ_organu, left_on="id_typ_organu", right_on="id_typ_org", suffixes=("",suffix), how='left')
        # Odstraň nedůležité sloupce 'priorita', protože se vzájemně vylučují a nejspíš ani k ničemu nejsou
        self.organy.drop(columns=["priorita", "priorita__typ_organu"], inplace=True)
        self.organy = drop_by_inconsistency(self.organy, suffix, 0.1, 'organy', 'typ_organu')

        self.df = self.organy

        if self.volebni_obdobi == None:
            self.volebni_obdobi = self.posledni_poslanecka_snemovna().od_organ.year
            log.info(f"Nastavuji začátek volebního období na: {self.volebni_obdobi}.")

        log.debug("<-- Organy")

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

    def posledni_poslanecka_snemovna(self):
        p =  self.df[(self.df.nazev_organu_cz == 'Poslanecká sněmovna') & (self.df.do_organ.isna())].sort_values(by=["od_organ"])
        assert len(p) == 1
        return p.iloc[-1]


class TypFunkce(TypOrganu):
    def __init__(self, *args, **kwargs):
        log.debug("--> TypFunkce")
        super(TypFunkce, self).__init__(*args, **kwargs)

        #self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        # Organy - cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1301
        # Orgány mají svůj typ, tyto typy mají hiearchickou strukturu.
        self.paths['typ_funkce'] = f"{self.data_dir}/typ_funkce.unl"
        #self.stahni_data()

        self.typ_funkce, self._typ_funkce = self.nacti_typ_funkce()

        semanticka_maska = {1: "předseda", 2: "místopředseda", 3: "ověřovatel"}
        self.typ_funkce['typ_funkce_obecny_CAT'] = mask_by_values(self.typ_funkce.typ_funkce_obecny, semanticka_maska)

        # Připoj Typu orgánu
        suffix="__typ_organu"
        self.typ_funkce = pd.merge(left=self.typ_funkce, right=self.typ_organu, on="id_typ_org", suffixes=("", suffix), how='left')
        self.typ_funkce = drop_by_inconsistency(self.typ_funkce, suffix, 0.1, 'typ_funkce', 'typ_organu')

        self.df = self.typ_funkce

        log.debug("<-- TypFunkce")

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

    def __init__(self, *args, **kwargs):
        log.debug("--> Funkce")
        super(Funkce, self).__init__(*args, **kwargs)

        #self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        # Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1301
        #Záznam mezi orgánem a typem funkce, názvy v funkce:nazev_funkce_LL se používají pouze interně,
        # slouží k definování pořadí funkcionářů v případech, kdy je toto pořadí určeno.
        self.paths['funkce'] = f"{self.data_dir}/funkce.unl"

        #self.stahni_data()

        self.funkce, self._funkce = self.nacti_funkce()

        # Připoj Orgány
        suffix = "__organy"
        self.funkce = pd.merge(left=self.funkce, right=self.organy, on='id_organ', suffixes=("", suffix), how='left')
        self.funkce =  drop_by_inconsistency(self.funkce, suffix, 0.1, 'funkce', 'organy')

        # Připoj Typ funkce
        suffix = "__typ_funkce"
        self.funkce = pd.merge(left=self.funkce, right=self.typ_funkce, on="id_typ_funkce", suffixes=("", suffix), how='left')
        self.funkce = drop_by_inconsistency(self.funkce, suffix, 0.1, 'funkce', 'typ_funkce')

        self.df = self.funkce

        log.debug("<-- Funkce")

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


class Osoby(PoslanciOsobyObecne):

    def __init__(self, *args, **kwargs):
        log.debug("--> Osoby")
        super(Osoby, self).__init__(*args, **kwargs)

        #self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        # Cesty k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1301
        # Jména osob, které jsou zařazeni v orgánech.
        # Vzhledem k tomu, že k jednoznačnému rozlišení osob často není dostatek informací,
        # je možné, že ne všechny záznamy odkazují na jedinečné osoby, tj. některé osoby jsou v tabulce vícekrát.
        self.paths['osoby'] = f"{self.data_dir}/osoby.unl"
        # Zařazení v orgánu nebo data funkcí osoby v orgánu. Pokud je zarazeni:do_o typu null, pak jde o aktuální zařazení.
        #self.paths['osoby_zarazeni'] = f"{self.data_dir}/zarazeni.unl"
        # Obsahuje vazby na externí systémy. Je-li typ = 1, pak jde o vazbu na evidenci senátorů na senat.cz
        self.paths['osoba_extra'] = f"{self.data_dir}/osoba_extra.unl"

        #self.stahni_data()

        self.osoby, self._osoby = self.nacti_osoby()
        self.osoba_extra, self.osoba_extra = self.nacti_osoba_extra()

        self.df = self.osoby

        log.debug("<-- Osoby")

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


class OsobyZarazeni(Funkce, Organy, Osoby):
    def __init__(self, *args, **kwargs):
        log.debug("--> OsobyZarazeni")
        super(OsobyZarazeni, self).__init__(*args, **kwargs)

        #self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        self.paths['osoby_zarazeni'] = f"{self.data_dir}/zarazeni.unl"

        self.osoby_zarazeni, self._osoby_zarazeni = self.nacti_osoby_zarazeni()

        # Připoj Osoby
        suffix = "__osoby"
        self.osoby_zarazeni = pd.merge(left=self.osoby_zarazeni, right=self.osoby, on='id_osoba', suffixes = ("", suffix), how='left')
        self.osoby_zarazeni = drop_by_inconsistency(self.osoby_zarazeni, suffix, 0.1, 'osoby_zarazeni', 'osoby')

        # Připoj orgány
        suffix = "__organy"
        sub1 = self.osoby_zarazeni[self.osoby_zarazeni.cl_funkce == 0].reset_index()
        m1 = pd.merge(left=sub1, right=self.organy, left_on='id_of', right_on='id_organ', suffixes=("", suffix), how='left')
        m1 = drop_by_inconsistency(m1, suffix, 0.1, 'osoby_zarazeni', 'organy')

        # Připoj Funkce
        sub2 = self.osoby_zarazeni[self.osoby_zarazeni.cl_funkce == 1].reset_index()
        m2 = pd.merge(left=sub2, right=self.funkce, left_on='id_of', right_on='id_funkce', suffixes=("", suffix), how='left')
        m2 = drop_by_inconsistency(m2, suffix, 0.1, 'osoby_zarazeni', 'funkce')

        self.osoby_zarazeni = pd.concat([m1, m2], axis=0, ignore_index=True).set_index('index').sort_index()

        self.df = self.osoby_zarazeni
        log.debug("<-- OsobyZarazeni")

    def nacti_osoby_zarazeni(self):
        header = {
            'id_osoba': 'Int64', # Identifikátor osoby, viz osoba:id_osoba
            'id_of':  'Int64', # Identifikátor orgánu či funkce: pokud je zároveň nastaveno zarazeni:cl_funkce == 0, pak id_o odpovídá organy:id_organ, pokud cl_funkce == 1, pak odpovídá funkce:id_funkce.
            'cl_funkce': 'Int64', # Status členství nebo funce: pokud je rovno 0, pak jde o členství, pokud 1, pak jde o funkci.
            'od_o': 'string', # datetime(year to hour) Zařazení od
            'do_o':  'string', #datetime(year to hour)  Zařazení do
            'od_f': 'string', # date  Mandát od. Nemusí být vyplněno a pokud je vyplněno, pak určuje datum vzniku mandátu a zarazeni:od_o obsahuje datum volby.
            'do_f': 'string'# date  Mandát do. Nemusí být vyplněno a pokud je vyplněno, určuje datum konce mandátu a zarazeni:do_o obsahuje datum ukončení zařazení.
        }

        _df = pd.read_csv(self.paths['osoby_zarazeni'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'osoby_zarazeni')

        df['cl_funkce_CAT'] = df.cl_funkce.astype('string').mask(df.cl_funkce == 0, 'členství').mask(df.cl_funkce == 1, 'funkce')

        df['od_o_DT'] = format_to_datetime_and_report_skips(df, 'od_o', '%Y-%m-%d %H').dt.tz_localize(self.tzn)
        # Fix known errors
        #df['do_o'] = df.do_o.mask(df.do_o == '0205-06-09 00',  '2005-06-09 00')
        df['do_o_DT'] = format_to_datetime_and_report_skips(df, 'do_o', '%Y-%m-%d %H').dt.tz_localize(self.tzn)
        df['od_f_DT'] = format_to_datetime_and_report_skips(df, 'od_f', '%d.%m.%Y').dt.tz_localize(self.tzn)
        df['do_f_DT'] = format_to_datetime_and_report_skips(df, 'do_f', '%d.%m.%Y').dt.tz_localize(self.tzn)

        return df, _df


class Poslanci(Osoby, Organy):

    def __init__(self, *args, **kwargs):
        log.debug("--> Poslanci")
        super(Poslanci, self).__init__(*args, **kwargs)

        # Další informace o poslanci vzhledem k volebnímu období: kontaktní údaje, adresa regionální kanceláře a podobně.
        # Některé údaje jsou pouze v aktuálním volebním období.
        self.paths['poslanci'] = f"{self.data_dir}/poslanec.unl"
        # Obsahuje GPS souřadnice regionálních kanceláří poslanců.
        self.paths['pkgps'] = f"{self.data_dir}/pkgps.unl"

        #self.stahni_data()

        self.poslanci, self._poslanci = self.nacti_poslance()
        self.pkgps, self._pkgps = self.nacti_pkgps()

        #self.poslanec = pd.merge(self.poslanec, self.osoby,  on='id_osoba')
        #poslanci_df = pd.merge(poslanci_df, osoby_zarazeni_df,  on='id_osoba')

        #posledni_volebni_obdobi = poslanci_df.id_obdobi.max()
        #poslanci_df = poslanci_df[poslanci_df.id_obdobi == posledni_volebni_obdobi]
        # Připoj informace o osobe
        suffix = "__osoby"
        self.poslanci = pd.merge(left=self.poslanci, right=self.osoby, on='id_osoba', suffixes = ("", suffix), how='left')
        self.poslanci = drop_by_inconsistency(self.poslanci, suffix, 0.1, 'poslanci', 'osoby')

        # Připoj informace o kanceláři
        suffix = "__pkgps"
        self.poslanci = pd.merge(left=self.poslanci, right=self.pkgps, on='id_poslanec', suffixes = ("", suffix), how='left')
        self.poslanci = drop_by_inconsistency(self.poslanci, suffix, 0.1, 'poslanci', 'pkgps')

        # Zúžení na volební období
        id_organu_dle_volebniho_obdobi = self.organy[(self.organy.nazev_organu_cz == 'Poslanecká sněmovna') & (self.organy.od_organ.dt.year == self.volebni_obdobi)].iloc[0].id_organ
        self.poslanci = self.poslanci[self.poslanci.id_obdobi == id_organu_dle_volebniho_obdobi]

        self.df = self.poslanci

        log.debug("<-- Poslanci")

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

        _df = pd.read_csv(self.paths['poslanci'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'poslanci')

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

