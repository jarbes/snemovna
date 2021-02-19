
# Poslanci a Osoby
# Agenda eviduje osoby, jejich zařazení do orgánů a jejich funkce v orgánech a orgány jako takové.
# Informace viz https://www.psp.cz/sqw/hp.sqw?k=1301.

# Poznámka: Většina značení se drží konvencí, které byly zvoleny na uvedené stránce. Výjimkou jsou sloupce 'id_organu' (v tabulkách též jako 'id_org') a id_typ_organu (v tabulkách též jako 'id_typ_org'), pro něž jsme značení sjednotili a používéme vždy první variantu.

from os import path

import pandas as pd
import numpy as np

from snemovna.utility import *

from snemovna.Snemovna import Snemovna

from snemovna.setup_logger import log


class PoslanciOsobyObecne(Snemovna):
    """Obecná třída pro dceřiné třídy (Osoby, Organy, Poslanci)."""

    def __init__(self, *args, **kwargs):
        log.debug("--> PoslanciOsobyObecne")

        super(PoslanciOsobyObecne, self).__init__(*args, **kwargs)

        self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        self.stahni_data()

        log.debug("<-- PoslanciOsobyObecne")

# Orgány mají svůj typ, tyto typy mají hiearchickou strukturu.
# TODO: Je zde nějaká časová závislost na volebním období?
class TypOrganu(PoslanciOsobyObecne):
    """
    Pomocná třída, která nese informace o typech orgánů a jejich hierarchiích.


    Methods
    -------
    nacti_typ_organu()
        Načte tabulku typ_organu do pandas a přetypuje sloupce
    """

    def __init__(self, *args, **kwargs):
        log.debug("--> TypOrganu")

        super(TypOrganu, self).__init__(*args, **kwargs)

        self.paths['typ_organu'] = f"{self.data_dir}/typ_organu.unl"

        self.typ_organu, self._typ_organu = self.nacti_typ_organu()

        self.df = self.typ_organu
        self.nastav_meta()

        log.debug("<-- TypOrganu")

    def nacti_typ_organu(self):
        header = {
            'id_typ_organu': MItem('Int64', 'Identifikátor typu orgánu'),
            'typ_id_typ_organu': MItem('Int64', 'Identifikátor nadřazeného typu orgánu (TypOrganu:id_typ_organu), pokud je null či nevyplněno, pak nemá nadřazený typ'),
            'nazev_typ_organu_cz': MItem('string', 'Název typu orgánu v češtině'),
            'nazev_typ_organu_en': MItem('string', 'Název typu orgánu v angličtině'),
            'typ_organu_obecny': MItem('Int64', 'Obecný typ orgánu, pokud je vyplněný, odpovídá záznamu v TypOrganu:id_typ_organu. Pomocí tohoto sloupce lze najít např. všechny výbory v různých typech zastupitelských sborů.'),
            'priorita': MItem('Int64', 'Priorita při výpisu')
        }

        _df = pd.read_csv(self.paths['typ_organu'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, name='typ_organu')
        self.rozsir_meta(header, tabulka='typ_organu', vlastni=False)

        return df, _df


# TODO: Je zde nějaká časová závislost na volebním období?
class Organy(TypOrganu):

    def __init__(self,  *args, **kwargs):
        log.debug("--> Organy")

        super(Organy, self).__init__(*args, **kwargs)

        # Záznam mezi orgánem a typem funkce, názvy v funkce:nazev_funkce_LL se používají pouze interně.
        # Slouží k definování pořadí funkcionářů v případech, kdy je toto pořadí určeno.
        self.paths['funkce'] = f"{self.data_dir}/funkce.unl"
        # Některé orgány mají nadřazený orgán a pak je položka organy:organ_id_organu vyplněna,
        # přičemž pouze v některých případech se tyto vazby využívají.
        self.paths['organy'] = f"{self.data_dir}/organy.unl"

        self.organy, self._organy = self.nacti_organy()

        # Připoj Typu orgánu
        suffix = "__typ_organu"
        self.organy = pd.merge(left=self.organy, right=self.typ_organu, left_on="id_typ_organu", right_on="id_typ_organu", suffixes=("",suffix), how='left')

        # Odstraň nedůležité sloupce 'priorita', protože se vzájemně vylučují a nejspíš ani k ničemu nejsou.
        # Tímto se vyhneme varování funkce 'drop_by_inconsistency'.
        self.organy.drop(columns=["priorita", "priorita__typ_organu"], inplace=True)
        self.organy = self.drop_by_inconsistency(self.organy, suffix, 0.1, 'organy', 'typ_organu')

        if self.volebni_obdobi == None:
            self.volebni_obdobi = self.posledni_poslanecka_snemovna().od_organ.year
            log.info(f"Nastavuji začátek volebního období na: {self.volebni_obdobi}.")

        if self.volebni_obdobi != -1:
           organ = self.organy[(self.organy.nazev_organu_cz == 'Poslanecká sněmovna') & (self.organy.od_organ.dt.year == self.volebni_obdobi)].iloc[0]
           self.id_organu = organ.id_organu
           self.volebni_obdobi_od = organ.od_organ
           self.volebni_obdobi_do = organ.do_organ

           ## Zúžení na aktuální volební období
           podminka = (self.organy.od_organ >= self.volebni_obdobi_od)
           if pd.notna(self.volebni_obdobi_do):
               podminka = podminka & (self.organy.do_organ <= self.volebni_obdobi_do)
           self.organy = self.organy[podminka]

        self.df = self.organy
        self.nastav_meta()

        log.debug("<-- Organy")

    def nacti_organy(self):
        header = {
            "id_organu": MItem('Int64', 'Identifikátor orgánu'),
            "organ_id_organu": MItem('Int64', 'Identifikátor nadřazeného orgánu, viz Organy:id_organu'),
            "id_typ_organu": MItem('Int64', 'Typ orgánu, viz TypOrganu:id_typ_organu'),
            "zkratka": MItem('string', 'Zkratka orgánu, bez diakritiky, v některých připadech se zkratka při zobrazení nahrazuje jiným názvem'),
            "nazev_organu_cz": MItem("string", 'Název orgánu v češtině'),
            "nazev_organu_en": MItem("string", 'Název orgánu v angličtině'),
            "od_organ": MItem('string', 'Ustavení orgánu'),
            "do_organ": MItem('string', 'Ukončení orgánu'),
            "priorita": MItem('Int64', 'Priorita výpisu orgánů'),
            "cl_organ_base": MItem('Int64', 'Pokud je nastaveno na 1, pak při výpisu členů se nezobrazují záznamy v tabulkce zarazeni kde cl_funkce == 0. Toto chování odpovídá tomu, že v některých orgánech nejsou členové a teprve z nich se volí funkcionáři, ale přímo se volí do určité funkce.')
        }

        _df = pd.read_csv(self.paths['organy'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, name='organy')
        self.rozsir_meta(header, tabulka='organy', vlastni=False)

        df['od_organ'] = format_to_datetime_and_report_skips(df, 'od_organ', '%d.%m.%Y').dt.tz_localize(self.tzn)
        df['do_organ'] = format_to_datetime_and_report_skips(df, 'do_organ', '%d.%m.%Y').dt.tz_localize(self.tzn)

        return df, _df

    def posledni_poslanecka_snemovna(self):
        p =  self.organy[(self.organy.nazev_organu_cz == 'Poslanecká sněmovna') & (self.organy.do_organ.isna())].sort_values(by=["od_organ"])
        assert len(p) == 1
        return p.iloc[-1]


# Tabulka definuje typ funkce v orgánu - pro každý typ orgánu jsou definovány typy funkcí. Texty názvů typu funkce se používají při výpisu namísto textů v Funkce:nazev_funkce_LL .
# TODO: Je zde nějaká časová závislost na volebním období?
class TypFunkce(TypOrganu):
    def __init__(self, *args, **kwargs):
        log.debug("--> TypFunkce")
        super(TypFunkce, self).__init__(*args, **kwargs)

        # Orgány mají svůj typ, tyto typy mají hiearchickou strukturu.
        self.paths['typ_funkce'] = f"{self.data_dir}/typ_funkce.unl"

        self.typ_funkce, self._typ_funkce = self.nacti_typ_funkce()

        # Připoj Typu orgánu
        suffix="__typ_organu"
        self.typ_funkce = pd.merge(left=self.typ_funkce, right=self.typ_organu, on="id_typ_organu", suffixes=("", suffix), how='left')
        # Odstraň nedůležité sloupce 'priorita', protože se vzájemně vylučují a nejspíš ani k ničemu nejsou.
        # Tímto se vyhneme varování v 'drop_by_inconsistency'.
        self.typ_funkce.drop(columns=["priorita", "priorita__typ_organu"], inplace=True)
        self.typ_funkce = self.drop_by_inconsistency(self.typ_funkce, suffix, 0.1, t1_name='typ_funkce', t2_name='typ_organu', t1_on='id_typ_organu', t2_on='id_typ_organu')

        self.df = self.typ_funkce
        self.nastav_meta()

        log.debug("<-- TypFunkce")

    def nacti_typ_funkce(self):
        header = {
            'id_typ_funkce': MItem('Int64', 'Identifikator typu funkce'),
            'id_typ_organu': MItem('Int64', 'Identifikátor typu orgánu, viz TypOrganu:id_typ_organu'),
            'typ_funkce_cz': MItem('string', 'Název typu funkce v češtině'),
            'typ_funkce_en': MItem('string', 'Název typu funkce v angličtině'),
            'priorita': MItem('Int64', 'Priorita při výpisu'),
            'typ_funkce_obecny__ORIG': MItem('Int64', 'Obecný typ funkce, 1 - předseda, 2 - místopředseda, 3 - ověřovatel, jiné hodnoty se nepoužívají.')

        }

        _df = pd.read_csv(self.paths['typ_funkce'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, name='typ_funkce')
        self.rozsir_meta(header, tabulka='typ_funkce', vlastni=False)

        mask = {1: "předseda", 2: "místopředseda", 3: "ověřovatel"}
        df['typ_funkce_obecny'] = mask_by_values(df.typ_funkce_obecny__ORIG, mask).astype('string')
        self.meta['typ_funkce_obecny'] = dict(popis='Obecný typ funkce.', tabulka='typ_funkce', vlastni=True)

        return df, _df


# TODO: Je zde nějaká časová závislost na volebním období?
class Funkce(Organy, TypFunkce):

    def __init__(self, *args, **kwargs):
        log.debug("--> Funkce")
        super(Funkce, self).__init__(*args, **kwargs)

        #Záznam mezi orgánem a typem funkce, názvy v funkce:nazev_funkce_LL se používají pouze interně,
        # slouží k definování pořadí funkcionářů v případech, kdy je toto pořadí určeno.
        self.paths['funkce'] = f"{self.data_dir}/funkce.unl"

        self.funkce, self._funkce = self.nacti_funkce()

        # Připoj Orgány
        suffix = "__organy"
        self.funkce = pd.merge(left=self.funkce, right=self.organy, on='id_organu', suffixes=("", suffix), how='left')
        self.funkce =  self.drop_by_inconsistency(self.funkce, suffix, 0.1, 'funkce', 'organy')

        # Připoj Typ funkce
        suffix = "__typ_funkce"
        self.funkce = pd.merge(left=self.funkce, right=self.typ_funkce, on="id_typ_funkce", suffixes=("", suffix), how='left')
        self.funkce = self.drop_by_inconsistency(self.funkce, suffix, 0.1, 'funkce', 'typ_funkce', t1_on='id_typ_funkce', t2_on='id_typ_funkce')

        # Zúžení na aktuální volební období
        if self.volebni_obdobi != -1:
            podminka = (self.funkce.od_organ >= self.volebni_obdobi_od)
            if pd.notna(self.volebni_obdobi_do):
                podminka = podminka & (self.funkce.do_organ <= self.volebni_obdobi_do)
            self.funkce = self.funkce[podminka]

        self.df = self.funkce
        self.nastav_meta()

        log.debug("<-- Funkce")

    def nacti_funkce(self):
        header = {
            "id_funkce": MItem('Int64', 'Identifikátor funkce, používá se v OsobyZarazeni:id_fo'),
            "id_organu": MItem('Int64', 'Identifikátor orgánu, viz Organy:id_organu'),
            "id_typ_funkce": MItem('Int64', 'Typ funkce, viz typ_funkce:id_typ_funkce'),
            "nazev_funkce_cz": MItem('string', 'Název funkce, pouze pro interní použití'),
            "priorita": MItem('Int64', 'Priorita výpisu')
        }

        _df = pd.read_csv(self.paths['funkce'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, name='funkce')
        self.rozsir_meta(header, tabulka='funkce', vlastni=False)

        return df, _df


# TODO: Je zde nějaká časová závislost na volebním období?
class Osoby(PoslanciOsobyObecne):

    def __init__(self, *args, **kwargs):
        log.debug("--> Osoby")
        super(Osoby, self).__init__(*args, **kwargs)

        # Jména osob, které jsou zařazeni v orgánech.
        # Vzhledem k tomu, že k jednoznačnému rozlišení osob často není dostatek informací,
        # je možné, že ne všechny záznamy odkazují na jedinečné osoby, tj. některé osoby jsou v tabulce vícekrát.
        self.paths['osoby'] = f"{self.data_dir}/osoby.unl"
        # Obsahuje vazby na externí systémy. Je-li typ = 1, pak jde o vazbu na evidenci senátorů na senat.cz
        self.paths['osoba_extra'] = f"{self.data_dir}/osoba_extra.unl"

        self.osoba_extra, self.osoba_extra = self.nacti_osoba_extra()
        self.osoby, self._osoby = self.nacti_osoby()

        #suffix='__osoba_extra'
        #self.osoby = pd.merge(left=self.osoby, right=self.osoba_extra, on="id_osoba", how="left", suffixes=('', suffix))
        #self.drop_by_inconsistency(self.osoby, suffix, 0.1, 'hlasovani', 'osoba_extra', inplace=True)

        self.df = self.osoby
        self.nastav_meta()

        log.debug("<-- Osoby")

    def nacti_osoby(self):
        # Obsahuje jména osob, které jsou zařazeni v orgánech.
        # Vzhledem k tomu, že k jednoznačnému rozlišení osob často není dostatek informací, je možné, že ne všechny záznamy odkazují na jedinečné osoby, tj. některé osoby jsou v tabulce vícekrát.
        header = {
            "id_osoba": MItem("Int64", 'Identifikátor osoby'),
            "pred": MItem('string', 'Titul pred jmenem'),
            "prijmeni": MItem('string', 'Příjmení, v některých případech obsahuje i dodatek typu "st.", "ml."'),
            "jmeno": MItem('string', 'Jméno'),
            "za": MItem('string', 'Titul za jménem'),
            "narozeni": MItem('datetime64[ns]', 'Datum narození, pokud neznámo, pak 1.1.1900.'),
            'pohlavi__ORIG': MItem('string', 'Pohlaví, "M" jako muž, ostatní hodnoty žena'),
            "zmena": MItem('datetime64[ns]', 'Datum posledni změny'),
            "umrti": MItem('datetime64[ns]', 'Datum úmrtí')
        }
        _df = pd.read_csv(self.paths['osoby'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'osoby')
        self.rozsir_meta(header, tabulka='osoby', vlastni=False)

        df["pohlavi"] = mask_by_values(df.pohlavi__ORIG, {'M': "muž", 'Z': 'žena', 'Ž': 'žena'}).astype('string')
        self.meta['pohlavi'] = dict(popis='Pohlaví.', tabulka='osoby', vlastni=True)

        df['narozeni'] = pd.to_datetime(df['narozeni'], format='%Y-%m-%d').dt.tz_localize(self.tzn)
        df['narozeni'] = df.narozeni.mask(df.narozeni == '1900-01-01', pd.NaT)

        df['umrti'] = pd.to_datetime(df['umrti'], format='%Y-%m-%d').dt.tz_localize(self.tzn)
        df['zmena'] = pd.to_datetime(df['zmena'], format='%Y-%m-%d').dt.tz_localize(self.tzn)

        return df, _df

    def nacti_osoba_extra(self):
    # Tabulka obsahuje vazby na externí systémy. Je-li typ = 1, pak jde o vazbu na evidenci senátorů na senat.cz
        header = {
            'id_osoba': MItem('Int64', 'Identifikátor osoby, viz Osoba:id_osoba'),
            'id_organu': MItem('Int64', 'Identifikátor orgánu, viz Organy:id_organu'),
            'typ': MItem('Int64', 'Typ záznamu, viz výše. [??? Asi chtěli napsat níže ...]'),
            'obvod': MItem('Int64', 'Je-li typ = 1, pak jde o číslo senátního obvodu.'),
            'strana': MItem('string', 'Je-li typ = 1, pak jde o název volební strany/hnutí či označení nezávislého kandidáta'),
            'id_external': MItem('Int64', 'Je-li typ = 1, pak je to identifikátor senátora na senat.cz')
        }

        _df = pd.read_csv(self.paths['osoba_extra'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'osoba_extra')
        self.rozsir_meta(header, tabulka='osoba_extra', vlastni=False)

        return df, _df


# TODO: Je zde nějaká časová závislost na volebním období?
class OsobyZarazeni(Funkce, Organy, Osoby):
    def __init__(self, *args, **kwargs):
        log.debug("--> OsobyZarazeni")

        super(OsobyZarazeni, self).__init__(*args, **kwargs)

        self.paths['osoby_zarazeni'] = f"{self.data_dir}/zarazeni.unl"

        self.osoby_zarazeni, self._osoby_zarazeni = self.nacti_osoby_zarazeni()

        # Připoj Osoby
        suffix = "__osoby"
        self.osoby_zarazeni = pd.merge(left=self.osoby_zarazeni, right=self.osoby, on='id_osoba', suffixes = ("", suffix), how='left')
        self.osoby_zarazeni = self.drop_by_inconsistency(self.osoby_zarazeni, suffix, 0.1, 'osoby_zarazeni', 'osoby')

        # Připoj orgány
        suffix = "__organy"
        sub1 = self.osoby_zarazeni[self.osoby_zarazeni.cl_funkce == 'členství'].reset_index()
        m1 = pd.merge(left=sub1, right=self.organy, left_on='id_of', right_on='id_organu', suffixes=("", suffix), how='left')
        m1 = self.drop_by_inconsistency(m1, suffix, 0.1, 'osoby_zarazeni', 'organy')

        # Připoj Funkce
        sub2 = self.osoby_zarazeni[self.osoby_zarazeni.cl_funkce == 'funkce'].reset_index()
        m2 = pd.merge(left=sub2, right=self.funkce, left_on='id_of', right_on='id_funkce', suffixes=("", suffix), how='left')
        m2 = self.drop_by_inconsistency(m2, suffix, 0.1, 'osoby_zarazeni', 'funkce')

        self.osoby_zarazeni = pd.concat([m1, m2], axis=0, ignore_index=True).set_index('index').sort_index()

        ## Zúžení na aktuální volební období
        if self.volebni_obdobi != -1:
            interval_start = np.maximum(self.osoby_zarazeni.od_o, self.volebni_obdobi_od)
            interval_end = np.minimum(self.osoby_zarazeni.do_o, self.volebni_obdobi_do)

            # TODO: Co kdyz je interval_start null ?
            # TODO: Jak je to s konci intervalů?
            # TODO: Kdy použít od_f místo od_o, resp. do_f místo do_o?
            assert self.osoby_zarazeni[interval_start.isna()].size == 0

            if pd.isna(self.volebni_obdobi_do):
                podminka = ((interval_start <= self.osoby_zarazeni.do_o) |  (self.osoby_zarazeni.do_o.isna()))
            else:
              podminka = ((interval_start <= interval_end) | ((interval_start <= self.volebni_obdobi_do) & (self.osoby_zarazeni.do_o.isna())))

            self.osoby_zarazeni= self.osoby_zarazeni[podminka]

        self.df = self.osoby_zarazeni
        self.nastav_meta()

        log.debug("<-- OsobyZarazeni")

    def nacti_osoby_zarazeni(self):
        header = {
            'id_osoba': MItem('Int64', 'Identifikátor osoby, viz Osoby:id_osoba'),
            'id_of': MItem('Int64', 'Identifikátor orgánu či funkce: pokud je zároveň nastaveno zarazeni:cl_funkce == 0, pak id_o odpovídá Organy:id_organu, pokud cl_funkce == 1, pak odpovídá Funkce:id_funkce.'),
            'cl_funkce__ORIG': MItem('Int64', 'Status členství nebo funce: pokud je rovno 0, pak jde o členství, pokud 1, pak jde o funkci.'),
            'od_o': MItem('string', 'datetime(year to hour) Zařazení od.'),
            'do_o':  MItem('string', 'datetime(year to hour)  Zařazení do.'),
            'od_f': MItem('string', 'Mandát od. Nemusí být vyplněno a pokud je vyplněno, pak určuje datum vzniku mandátu a zarazeni:od_o obsahuje datum volby.'),
            'do_f': MItem('string', 'Mandát do. Nemusí být vyplněno a pokud je vyplněno, určuje datum konce mandátu a zarazeni:do_o obsahuje datum ukončení zařazení.')
        }

        _df = pd.read_csv(self.paths['osoby_zarazeni'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'osoby_zarazeni')
        self.rozsir_meta(header, tabulka='osoby_zarazeni', vlastni=False)

        df['od_o'] = format_to_datetime_and_report_skips(df, 'od_o', '%Y-%m-%d %H').dt.tz_localize(self.tzn)
        # Fix known errors
        #df['do_o'] = df.do_o.mask(df.do_o == '0205-06-09 00',  '2005-06-09 00')
        df['do_o'] = format_to_datetime_and_report_skips(df, 'do_o', '%Y-%m-%d %H').dt.tz_localize(self.tzn)
        df['od_f'] = format_to_datetime_and_report_skips(df, 'od_f', '%d.%m.%Y').dt.tz_localize(self.tzn)
        df['do_f'] = format_to_datetime_and_report_skips(df, 'do_f', '%d.%m.%Y').dt.tz_localize(self.tzn)

        mask = {0: 'členství', 1: 'funkce'}
        df['cl_funkce'] = mask_by_values(df.cl_funkce__ORIG, mask).astype('string')
        self.meta['cl_funkce'] = dict(popis='Status členství nebo funkce.', tabulka='osoby_zarazeni', vlastni=True)

        return df, _df


class Poslanci(OsobyZarazeni, Organy):

    def __init__(self, *args, **kwargs):
        log.debug("--> Poslanci")

        super(Poslanci, self).__init__(*args, **kwargs)

        # Další informace o poslanci vzhledem k volebnímu období: kontaktní údaje, adresa regionální kanceláře a podobně.
        # Některé údaje jsou pouze v aktuálním volebním období.
        self.paths['poslanci'] = f"{self.data_dir}/poslanec.unl"
        # Obsahuje GPS souřadnice regionálních kanceláří poslanců.
        self.paths['pkgps'] = f"{self.data_dir}/pkgps.unl"

        self.pkgps, self._pkgps = self.nacti_pkgps()
        self.poslanci, self._poslanci = self.nacti_poslance()

        # Zúžení na volební období
        if self.volebni_obdobi != -1:
            self.poslanci = self.poslanci[self.poslanci.id_organu == self.id_organu]

        # Připojení informace o osobě, např. jméno a příjmení
        suffix = "__osoby"
        self.poslanci = pd.merge(left=self.poslanci, right=self.osoby, on='id_osoba', suffixes = ("", suffix), how='left')
        self.poslanci = self.drop_by_inconsistency(self.poslanci, suffix, 0.1, 'poslanci', 'osoby')

        # Připoj informace o kanceláři
        suffix = "__pkgps"
        self.poslanci = pd.merge(left=self.poslanci, right=self.pkgps, on='id_poslanec', suffixes = ("", suffix), how='left')
        self.drop_by_inconsistency(self.poslanci, suffix, 0.1, 'poslanci', 'pkgps', inplace=True)

        ## Merge osoby_zarazeni
        strany = self.osoby_zarazeni[(self.osoby_zarazeni.id_osoba.isin(self.poslanci.id_osoba)) & (self.osoby_zarazeni.nazev_typ_organu_cz == "Klub") & (self.osoby_zarazeni.do_o.isna()) & (self.osoby_zarazeni.cl_funkce=='členství')].copy()
        strany.rename(columns={'id_organu': 'id_strany', 'nazev_organu_cz': 'nazev_strany_cz', 'zkratka': 'zkratka_strany'}, inplace=True)
        self.poslanci = pd.merge(self.poslanci, strany[['id_osoba', 'id_strany', 'nazev_strany_cz', 'zkratka_strany']], on='id_osoba', how="left")
        self.poslanci = self.drop_by_inconsistency(self.poslanci, suffix, 0.1, 'poslanci', 'osoby_zarazeni')
        self.meta['id_strany'] = {"popis": 'Identifikátor strany, do které je aktuálně poslanec zařazen, viz Organy:id_organu', 'tabulka': 'df', 'vlastni': True}
        self.meta['nazev_strany_cz'] = {"popis": 'Název strany, do které je aktuálně poslanec zařazen, viz Organy:nazev_organu_cz', 'tabulka': 'df', 'vlastni': True}
        self.meta['zkratka_strany'] = {"popis": 'Zkratka strany, do které je aktuálně poslanec zařazen, viz Organy:zkratka', 'tabulka': 'df', 'vlastni': True}

        to_drop = ['']

        self.df = self.poslanci
        self.nastav_meta()

        log.debug("<-- Poslanci")

    def nacti_poslance(self):
        header = {
            "id_poslanec": MItem('Int64', 'Identifikátor poslance'),
            "id_osoba": MItem('Int64', 'Identifikátor osoby, viz Osoby:id_osoba'),
            "id_kraj": MItem('Int64', 'Volební kraj, viz Organy:id_organu'),
            "id_kandidatka": MItem('Int64', 'Volební strana/hnutí, viz Organy:id_organu, pouze odkazuje na stranu/hnutí, za kterou byl zvolen a nemusí mít souvislost s členstvím v poslaneckém klubu.'),
            "id_organu":  MItem('Int64', 'Volební období, viz Organy:id_organu'),
            "web": MItem('string', 'URL vlastních stránek poslance'),
            "ulice": MItem('string', 'Adresa regionální kanceláře, ulice.'),
            "obec": MItem('string', 'Adresa regionální kanceláře, obec.'),
            "psc": MItem('string', 'Adresa regionální kanceláře, PSČ.'),
            "email": MItem('string', 'E-mailová adresa poslance, případně obecná posta@psp.cz.'),
            "telefon": MItem('string', 'Adresa regionální kanceláře, telefon.'),
            "fax": MItem('string', 'Adresa regionální kanceláře, fax.'),
            "psp_telefon": MItem('string', 'Telefonní číslo do kanceláře v budovách PS.'),
            "facebook": MItem('string', 'URL stránky služby Facebook.'),
            "foto": MItem('Int64', 'Pokud je rovno 1, pak existuje fotografie poslance.')
        }

        _df = pd.read_csv(self.paths['poslanci'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'poslanci')
        self.rozsir_meta(header, tabulka='poslanci', vlastni=False)

        return df, _df

    def nacti_pkgps(self):
    # Tabulka obsahuje GPS souřadnice regionálních kanceláří poslanců.
        header = {
            'id_poslanec': MItem('Int64', 'Identifikátor poslance, viz Poslanci:id_poslanec'),
            'adresa': MItem('string', 'Adresa kanceláře, jednotlivé položky jsou odděleny středníkem'),
            'sirka': MItem('string', 'Severní šířka, WGS 84, formát GG.AABBCCC, GG = stupně, AA - minuty, BB - vteřiny, CCC - tisíciny vteřin'),
            'delka': MItem('string', 'Východní délka, WGS 84, formát GG.AABBCCC, GG = stupně, AA - minuty, BB - vteřiny, CCC - tisíciny vteřin')
        }
        _df = pd.read_csv(self.paths['pkgps'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'pkgps')
        self.rozsir_meta(header, tabulka='pkgps', vlastni=False)

        return df, _df

