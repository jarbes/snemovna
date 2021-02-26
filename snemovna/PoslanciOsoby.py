
# Poslanci a Osoby
# Agenda eviduje osoby, jejich zařazení do orgánů a jejich funkce v orgánech a orgány jako takové.
# Informace viz https://www.psp.cz/sqw/hp.sqw?k=1301.

# Poznámka: Většina značení se drží konvencí, které byly zvoleny na uvedené stránce. Výjimkou jsou sloupce 'id_organ' (v tabulkách též jako 'id_org') a id_typ_organ (v tabulkách též jako 'id_typ_org'), pro něž jsme značení sjednotili a používéme vždy první variantu.

from os import path

import pandas as pd
import numpy as np

from snemovna.utility import *
from snemovna.Snemovna import *
from snemovna.setup_logger import log


class PoslanciOsobyObecne(Snemovna):
    """Obecná třída pro dceřiné třídy (Osoby, Organy, Poslanci, etc.)"""

    def __init__(self, *args, **kwargs):
        log.debug("--> PoslanciOsobyObecne")

        super(PoslanciOsobyObecne, self).__init__(*args, **kwargs)

        self.nastav_datovy_zdroj("https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip")

        self.stahni_data()

        log.debug("<-- PoslanciOsobyObecne")

# Orgány mají svůj typ, tyto typy mají hiearchickou strukturu.
# Třída TypOrgan nebere v úvahu závislost na volebnim obdobi, protože tu je možné získat až pomocí dceřinných tříd (Orgány, ZarazeniOsoby).
class TypOrgan(PoslanciOsobyObecne):
    """
    Pomocná třída, která nese informace o typech orgánů a jejich hierarchiích.


    Methods
    -------
    nacti_typ_organu()
        Načte tabulku typ_organu do pandas a přetypuje sloupce
    """

    def __init__(self, *args, **kwargs):
        log.debug("--> TypOrgan")

        super(TypOrgan, self).__init__(*args, **kwargs)

        self.paths['typ_organu'] = f"{self.data_dir}/typ_organu.unl"

        self.tbl['typ_organu'], self.tbl['_typ_organu'] = self.nacti_typ_organu()

        self.nastav_dataframe(self.tbl['typ_organu'])

        log.debug("<-- TypOrgan")

    def nacti_typ_organu(self):
        header = {
            'id_typ_organ': MItem('Int64', 'Identifikátor typu orgánu'),
            'typ_id_typ_organ': MItem('Int64', 'Identifikátor nadřazeného typu orgánu (TypOrgan:id_typ_organ), pokud je null či nevyplněno, pak nemá nadřazený typ'),
            'nazev_typ_organ_cz': MItem('string', 'Název typu orgánu v češtině'),
            'nazev_typ_organ_en': MItem('string', 'Název typu orgánu v angličtině'),
            'typ_organu_obecny': MItem('Int64', 'Obecný typ orgánu, pokud je vyplněný, odpovídá záznamu v TypOrgan:id_typ_organ. Pomocí tohoto sloupce lze najít např. všechny výbory v různých typech zastupitelských sborů.'),
            'priorita': MItem('Int64', 'Priorita při výpisu')
        }

        _df = pd.read_csv(self.paths['typ_organu'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, name='typ_organu')
        self.rozsir_meta(header, tabulka='typ_organu', vlastni=False)

        return df, _df


class Organy(TypOrgan):
    def __init__(self,  *args, **kwargs):
        log.debug("--> Organy")

        super(Organy, self).__init__(*args, **kwargs)

        # Záznam mezi orgánem a typem funkce, názvy v funkce:nazev_funkce_LL se používají pouze interně.
        # Slouží k definování pořadí funkcionářů v případech, kdy je toto pořadí určeno.
        self.paths['funkce'] = f"{self.data_dir}/funkce.unl"
        # Některé orgány mají nadřazený orgán a pak je položka organy:organ_id_organ vyplněna,
        # přičemž pouze v některých případech se tyto vazby využívají.
        self.paths['organy'] = f"{self.data_dir}/organy.unl"

        self.tbl['organy'], self.tbl['_organy'] = self.nacti_organy()

        # Připoj Typu orgánu
        suffix = "__typ_organu"
        self.tbl['organy'] = pd.merge(left=self.tbl['organy'], right=self.tbl['typ_organu'], on="id_typ_organ", suffixes=("",suffix), how='left')
        # Odstraň nedůležité sloupce 'priorita', protože se vzájemně vylučují a nejspíš k ničemu nejsou.
        # Tímto se vyhneme varování funkce 'drop_by_inconsistency.
        self.tbl['organy'].drop(columns=["priorita", "priorita__typ_organu"], inplace=True)
        self.tbl['organy'] = self.drop_by_inconsistency(self.tbl['organy'], suffix, 0.1, 'organy', 'typ_organu')

        # Nastav volební období, pokud chybí
        if self.volebni_obdobi == None:
            self.volebni_obdobi = self._posledni_snemovna().od_organ.year
            log.info(f"Nastavuji začátek volebního období na: {self.volebni_obdobi}.")

        if self.volebni_obdobi != -1:
            x = self.tbl['organy'][
                (self.tbl['organy'].nazev_organ_cz == 'Poslanecká sněmovna')
                & (self.tbl['organy'].od_organ.dt.year == self.volebni_obdobi)
            ]
            if len(x) == 1:
                self.snemovna = x.iloc[0]
            else:
                log.error('Bylo nalezeno více sněmoven pro dané volební období!')
                raise ValueError

        self.tbl['organy'] = self.vyber_platne_organy()
        self.nastav_dataframe(self.tbl['organy'])

        log.debug("<-- Organy")

    def nacti_organy(self):
        header = {
            "id_organ": MItem('Int64', 'Identifikátor orgánu'),
            "organ_id_organ": MItem('Int64', 'Identifikátor nadřazeného orgánu, viz Organy:id_organ'),
            "id_typ_organ": MItem('Int64', 'Typ orgánu, viz TypOrgan:id_typ_organ'),
            "zkratka": MItem('string', 'Zkratka orgánu, bez diakritiky, v některých připadech se zkratka při zobrazení nahrazuje jiným názvem'),
            "nazev_organ_cz": MItem("string", 'Název orgánu v češtině'),
            "nazev_organ_en": MItem("string", 'Název orgánu v angličtině'),
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

    def vyber_platne_organy(self, df=None):
        if df == None:
            df = self.tbl['organy']

        if self.volebni_obdobi == -1:
            return df

        #ids_snemovnich_organu = find_children_ids([], 'id_organ', df, 'organ_id_organ', [self.snemovna.id_organ], 0)
        ids_snemovnich_organu = expand_hierarchy(df, 'id_organ', 'organ_id_organ', [self.snemovna.id_organ])

        # TODO: Kdy použít od_f místo od_o, resp. do_f místo do_o?
        interval_start = df.od_organ\
            .mask(df.od_organ.isna(), self.snemovna.od_organ)\
            .mask(~df.od_organ.isna(), np.maximum(df.od_organ, self.snemovna.od_organ))

        # Pozorování: volebni_obdobi_od není nikdy NaT => interval_start není nikdy NaT
        if pd.isna(self.snemovna.do_organ): # příznak posledního volebního období
            podminka_interval = (
                (interval_start.dt.date <= df.do_organ.dt.date) # Nutná podmínka pro True: (interval_start != NaT, splněno vždy) a (do_organ != NaT)
                |  df.do_organ.isna() # Nutná podmínka pro True: (interval_start != NaT, splněno vždy) a (do_organ == NaT)
            )
        else: # Pozorování: předchozí volební období => interval_end není nikdy NaT
            interval_end = df.do_organ\
                .mask(df.do_organ.isna(), self.snemovna.do_organ)\
                .mask(~df.do_organ.isna(), np.minimum(df.do_organ, self.snemovna.do_organ))
            podminka_interval = (interval_start.dt.date <= interval_end.dt.date)

        ids_jinych_snemoven = []

        x = self._predchozi_snemovna()
        if x is not None:
            ids_jinych_snemoven.append(x.id_organ)

        x = self._nasledujici_snemovna()
        if x is not None:
            ids_jinych_snemoven.append(x.id_organ)

        #ids_jinych_snemovnich_organu = find_children_ids(ids_jinych_snemoven, 'id_organ', df, 'organ_id_organ', ids_jinych_snemoven, 0)
        ids_jinych_snemovnich_organu = expand_hierarchy(df, 'id_organ', 'organ_id_organ', ids_jinych_snemoven)
        podminka_nepatri_do_jine_snemovny = ~df.id_organ.isin(ids_jinych_snemovnich_organu)

        df = df[
            (df.id_organ.isin(ids_snemovnich_organu) == True)
            | (podminka_interval & podminka_nepatri_do_jine_snemovny)
        ]

        return df

    def _posledni_snemovna(self):
        """Pomocná funkce, vrací data poslední sněmovny"""
        p =  self.tbl['organy'][(self.tbl['organy'].nazev_organ_cz == 'Poslanecká sněmovna') & (self.tbl['organy'].do_organ.isna())].sort_values(by=["od_organ"])
        if len(p) == 1:
            return p.iloc[0]
        else:
            return None

    def _predchozi_snemovna(self, id_organ=None):
        """Pomocná funkce, vrací data předchozí sněmovny"""

        # Pokud nebylo zadáno id_orgánu, implicitně vezmi id_organ dané sněmovny.
        if id_organ == None:
            id_organ = self.snemovna.id_organ

        snemovny = self.tbl['organy'][self.tbl['organy'].nazev_organ_cz == 'Poslanecká sněmovna'].sort_values(by="do_organ").copy()
        snemovny['id_predchozi_snemovny'] = snemovny.id_organ.shift(1)
        idx = snemovny[snemovny.id_organ == id_organ].iloc[0].id_predchozi_snemovny
        p = snemovny[snemovny.id_organ == idx]

        assert len(p) <= 1

        if len(p) == 1:
          return p.iloc[0]
        else:
          return None

    def _nasledujici_snemovna(self, id_organ=None):
        """Pomocná funkce, vrací data následující sněmovny"""

        # Pokud nebylo zadáno id_orgánu, implicitně vezmi id_organ dané sněmovny.
        if id_organ == None:
            id_organ = self.snemovna.id_organ

        snemovny = self.tbl['organy'][self.tbl['organy'].nazev_organ_cz == 'Poslanecká sněmovna'].sort_values(by="do_organ").copy()
        snemovny['id_nasledujici_snemovny'] = snemovny.id_organ.shift(-1)
        idx = snemovny[snemovny.id_organ == id_organ].iloc[0].id_nasledujici_snemovny
        p = snemovny[snemovny.id_organ == idx]

        assert len(p) <= 1

        if len(p) == 1:
          return p.iloc[0]
        else:
          return None


# Tabulka definuje typ funkce v orgánu - pro každý typ orgánu jsou definovány typy funkcí. Texty názvů typu funkce se používají při výpisu namísto textů v Funkce:nazev_funkce_LL .
# Třída TypFunkce nebere v úvahu závislost na volebnim obdobi, protože tu je možné získat až pomocí dceřinných tříd (ZarazeniOsoby).
class TypFunkce(TypOrgan):
    def __init__(self, *args, **kwargs):
        log.debug("--> TypFunkce")
        super(TypFunkce, self).__init__(*args, **kwargs)

        # Orgány mají svůj typ, tyto typy mají hiearchickou strukturu.
        self.paths['typ_funkce'] = f"{self.data_dir}/typ_funkce.unl"

        self.tbl['typ_funkce'], self.tbl['_typ_funkce'] = self.nacti_typ_funkce()

        # Připoj Typu orgánu
        suffix="__typ_organu"
        self.tbl['typ_funkce'] = pd.merge(
            left=self.tbl['typ_funkce'],
            right=self.tbl['typ_organu'],
            on="id_typ_organ",
            suffixes=("", suffix),
            how='left'
        )
        # Odstraň nedůležité sloupce 'priorita', protože se vzájemně vylučují a nejspíš ani k ničemu nejsou.
        # Tímto se vyhneme varování v 'drop_by_inconsistency'.
        self.tbl['typ_funkce'].drop(columns=["priorita", "priorita__typ_organu"], inplace=True)
        self.tbl['typ_funkce'] = self.drop_by_inconsistency(self.tbl['typ_funkce'], suffix, 0.1, t1_name='typ_funkce', t2_name='typ_organu', t1_on='id_typ_organ', t2_on='id_typ_organ')

        self.nastav_dataframe(self.tbl['typ_funkce'])

        log.debug("<-- TypFunkce")

    def nacti_typ_funkce(self):
        header = {
            'id_typ_funkce': MItem('Int64', 'Identifikator typu funkce'),
            'id_typ_organ': MItem('Int64', 'Identifikátor typu orgánu, viz TypOrgan:id_typ_organ'),
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


class Funkce(Organy, TypFunkce):

    def __init__(self, *args, **kwargs):
        log.debug("--> Funkce")
        super(Funkce, self).__init__(*args, **kwargs)

        #Záznam mezi orgánem a typem funkce, názvy v funkce:nazev_funkce_LL se používají pouze interně,
        # slouží k definování pořadí funkcionářů v případech, kdy je toto pořadí určeno.
        self.paths['funkce'] = f"{self.data_dir}/funkce.unl"

        self.tbl['funkce'], self.tbl['_funkce'] = self.nacti_funkce()

        # Zúžení
        self.vyber_platne_funkce()

        # Připoj Orgány
        suffix = "__organy"
        self.tbl['funkce'] = pd.merge(
            left=self.tbl['funkce'],
            right=self.tbl['organy'],
            on='id_organ',
            suffixes=("", suffix),
            how='left'
        )
        self.tbl['funkce'] =  self.drop_by_inconsistency(self.tbl['funkce'], suffix, 0.1, 'funkce', 'organy')

        # Připoj Typ funkce
        suffix = "__typ_funkce"
        self.tbl['funkce'] = pd.merge(left=self.tbl['funkce'], right=self.tbl['typ_funkce'], on="id_typ_funkce", suffixes=("", suffix), how='left')
        self.tbl['funkce'] = self.drop_by_inconsistency(self.tbl['funkce'], suffix, 0.1, 'funkce', 'typ_funkce', t1_on='id_typ_funkce', t2_on='id_typ_funkce')

        if self.volebni_obdobi != -1:
            assert len(self.tbl['funkce'][self.tbl['funkce'].id_organ.isna()]) == 0

        self.nastav_dataframe(self.tbl['funkce'])

        log.debug("<-- Funkce")

    def nacti_funkce(self):
        header = {
            "id_funkce": MItem('Int64', 'Identifikátor funkce, používá se v ZarazeniOsoby:id_fo'),
            "id_organ": MItem('Int64', 'Identifikátor orgánu, viz Organy:id_organ'),
            "id_typ_funkce": MItem('Int64', 'Typ funkce, viz typ_funkce:id_typ_funkce'),
            "nazev_funkce_cz": MItem('string', 'Název funkce, pouze pro interní použití'),
            "priorita": MItem('Int64', 'Priorita výpisu')
        }

        _df = pd.read_csv(self.paths['funkce'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, name='funkce')
        self.rozsir_meta(header, tabulka='funkce', vlastni=False)

        return df, _df

    def vyber_platne_funkce(self):
        if self.volebni_obdobi != -1:
            self.tbl['funkce'] = self.tbl['funkce'][self.tbl['funkce'].id_organ.isin(self.tbl['organy'].id_organ)]


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

        self.tbl['osoba_extra'], self.tbl['osoba_extra'] = self.nacti_osoba_extra()
        self.tbl['osoby'], self.tbl['_osoby'] = self.nacti_osoby()

        #suffix='__osoba_extra'
        #self.tbl['osoby'] = pd.merge(left=self.tbl['osoby'], right=self.tbl['osoba_extra'], on="id_osoba", how="left", suffixes=('', suffix))
        #self.drop_by_inconsistency(self.tbl['osoby'], suffix, 0.1, 'hlasovani', 'osoba_extra', inplace=True)

        self.nastav_dataframe(self.tbl['osoby'])

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
            "narozeni": MItem('string', 'Datum narození, pokud neznámo, pak 1.1.1900.'),
            'pohlavi__ORIG': MItem('string', 'Pohlaví, "M" jako muž, ostatní hodnoty žena'),
            "zmena": MItem('string', 'Datum posledni změny'),
            "umrti": MItem('string', 'Datum úmrtí')
        }
        _df = pd.read_csv(self.paths['osoby'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'osoby')
        self.rozsir_meta(header, tabulka='osoby', vlastni=False)

        df["pohlavi"] = mask_by_values(df.pohlavi__ORIG, {'M': "muž", 'Z': 'žena', 'Ž': 'žena'}).astype('string')
        self.meta['pohlavi'] = dict(popis='Pohlaví.', tabulka='osoby', vlastni=True)

        # Parsuj narození, meta informace není třeba přidávat, jsou v hlavičce
        #df['narozeni'] = pd.to_datetime(df['narozeni'], format="%d.%m.%Y", errors='coerce').dt.tz_localize(self.tzn)
        df['narozeni'] = format_to_datetime_and_report_skips(df, 'narozeni', to_format="%d.%m.%Y").dt.tz_localize(self.tzn)
        df['narozeni'] = df.narozeni.mask(df.narozeni.dt.strftime("%d.%m.%Y") == '01.01.1900', pd.NaT)

        # Parsuj úmrtí, meta informace není třeba přidávat, jsou v hlavičce
        df['umrti'] = format_to_datetime_and_report_skips(df, 'umrti', to_format="%d.%m.%Y").dt.tz_localize(self.tzn)
        df['umrti'] = df.umrti.mask(df.umrti.dt.strftime("%d.%m.%Y") == '01.01.1900', pd.NaT)
        # Parsuj datum poslední změny záznamu, meta informace není třeba přidávat, jsou v hlavičce
        df['zmena'] = format_to_datetime_and_report_skips(df, 'zmena', to_format="%d.%m.%Y").dt.tz_localize(self.tzn)

        return df, _df

    def nacti_osoba_extra(self):
    # Tabulka obsahuje vazby na externí systémy. Je-li typ = 1, pak jde o vazbu na evidenci senátorů na senat.cz
        header = {
            'id_osoba': MItem('Int64', 'Identifikátor osoby, viz Osoba:id_osoba'),
            'id_organ': MItem('Int64', 'Identifikátor orgánu, viz Organy:id_organ'),
            'typ': MItem('Int64', 'Typ záznamu, viz výše. [??? Asi chtěli napsat níže ...]'),
            'obvod': MItem('Int64', 'Je-li typ = 1, pak jde o číslo senátního obvodu.'),
            'strana': MItem('string', 'Je-li typ = 1, pak jde o název volební strany/hnutí či označení nezávislého kandidáta'),
            'id_external': MItem('Int64', 'Je-li typ = 1, pak je to identifikátor senátora na senat.cz')
        }

        _df = pd.read_csv(self.paths['osoba_extra'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'osoba_extra')
        self.rozsir_meta(header, tabulka='osoba_extra', vlastni=False)

        return df, _df


class ZarazeniOsoby(Funkce, Organy, Osoby):
    def __init__(self, *args, **kwargs):
        log.debug("--> ZarazeniOsoby")

        super(ZarazeniOsoby, self).__init__(*args, **kwargs)

        self.paths['zarazeni_osoby'] = f"{self.data_dir}/zarazeni.unl"

        self.tbl['zarazeni_osoby'], self.tbl['_zarazeni_osoby'] = self.nacti_zarazeni_osoby()

        # Připoj Osoby
        suffix = "__osoby"
        self.tbl['zarazeni_osoby'] = pd.merge(left=self.tbl['zarazeni_osoby'], right=self.tbl['osoby'], on='id_osoba', suffixes = ("", suffix), how='left')
        self.tbl['zarazeni_osoby'] = self.drop_by_inconsistency(self.tbl['zarazeni_osoby'], suffix, 0.1, 'zarazeni_osoby', 'osoby')

        # Připoj orgány
        suffix = "__organy"
        sub1 = self.tbl['zarazeni_osoby'][self.tbl['zarazeni_osoby'].cl_funkce == 'členství'].reset_index()
        if self.volebni_obdobi == -1:
            m1 = pd.merge(left=sub1, right=self.tbl['organy'], left_on='id_of', right_on='id_organ', suffixes=("", suffix), how='left')
        else:
            # Pozor, how='left' nestačí, 'inner' se podílí na zúžení na danou sněmovnu
            m1 = pd.merge(left=sub1, right=self.tbl['organy'], left_on='id_of', right_on='id_organ', suffixes=("", suffix), how='inner')
        m1 = self.drop_by_inconsistency(m1, suffix, 0.1, 'zarazeni_osoby', 'organy')

        # Připoj Funkce
        sub2 = self.tbl['zarazeni_osoby'][self.tbl['zarazeni_osoby'].cl_funkce == 'funkce'].reset_index()
        if self.volebni_obdobi == -1:
            m2 = pd.merge(left=sub2, right=self.tbl['funkce'], left_on='id_of', right_on='id_funkce', suffixes=("", suffix), how='left')
        else:
            # Pozor, how='left' nestačí, 'inner' se podílí na zúžení na danou sněmovnu
            m2 = pd.merge(left=sub2, right=self.tbl['funkce'], left_on='id_of', right_on='id_funkce', suffixes=("", suffix), how='inner')
        m2 = self.drop_by_inconsistency(m2, suffix, 0.1, 'zarazeni_osoby', 'funkce')

        self.tbl['zarazeni_osoby'] = pd.concat([m1, m2], axis=0, ignore_index=True).set_index('index').sort_index()

        # Zúžení na dané volební období
        self.vyber_platne_zarazeni_osoby()

        self.nastav_dataframe(self.tbl['zarazeni_osoby'])

        log.debug("<-- ZarazeniOsoby")

    def nacti_zarazeni_osoby(self):
        header = {
            'id_osoba': MItem('Int64', 'Identifikátor osoby, viz Osoby:id_osoba'),
            'id_of': MItem('Int64', 'Identifikátor orgánu či funkce: pokud je zároveň nastaveno zarazeni:cl_funkce == 0, pak id_o odpovídá Organy:id_organ, pokud cl_funkce == 1, pak odpovídá Funkce:id_funkce.'),
            'cl_funkce__ORIG': MItem('Int64', 'Status členství nebo funce: pokud je rovno 0, pak jde o členství, pokud 1, pak jde o funkci.'),
            'od_o': MItem('string', 'Zařazení od. [year to hour]'),
            'do_o':  MItem('string', 'Zařazení do. [year to hour]'),
            'od_f': MItem('string', 'Mandát od. Nemusí být vyplněno a pokud je vyplněno, pak určuje datum vzniku mandátu a ZarazeniOsoby:od_o obsahuje datum volby. [date]'),
            'do_f': MItem('string', 'Mandát do. Nemusí být vyplněno a pokud je vyplněno, určuje datum konce mandátu a ZarazeniOsoby:do_o obsahuje datum ukončení zařazení. [date]')
        }

        _df = pd.read_csv(self.paths['zarazeni_osoby'], sep="|", names = header.keys(), index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'zarazeni_osoby')
        self.rozsir_meta(header, tabulka='zarazeni_osoby', vlastni=False)

        df['od_o'] = format_to_datetime_and_report_skips(df, 'od_o', '%Y-%m-%d %H').dt.tz_localize(self.tzn)
        # Fix known errors
        #df['do_o'] = df.do_o.mask(df.do_o == '0205-06-09 00',  '2005-06-09 00')
        df['do_o'] = format_to_datetime_and_report_skips(df, 'do_o', '%Y-%m-%d %H').dt.tz_localize(self.tzn)
        df['od_f'] = format_to_datetime_and_report_skips(df, 'od_f', '%d.%m.%Y').dt.tz_localize(self.tzn)
        df['do_f'] = format_to_datetime_and_report_skips(df, 'do_f', '%d.%m.%Y').dt.tz_localize(self.tzn)

        mask = {0: 'členství', 1: 'funkce'}
        df['cl_funkce'] = mask_by_values(df.cl_funkce__ORIG, mask).astype('string')
        self.meta['cl_funkce'] = dict(popis='Status členství nebo funkce.', tabulka='zarazeni_osoby', vlastni=True)

        return df, _df

    def vyber_platne_zarazeni_osoby(self):
        if self.volebni_obdobi != -1:
            interval_start = self.tbl['zarazeni_osoby'].od_o\
                .mask(self.tbl['zarazeni_osoby'].od_o.isna(), self.snemovna.od_organ)\
                .mask(~self.tbl['zarazeni_osoby'].od_o.isna(), np.maximum(self.tbl['zarazeni_osoby'].od_o, self.snemovna.od_organ))

            # Pozorování: volebni_obdobi_od není nikdy NaT => interval_start není nikdy NaT
            if pd.isna(self.snemovna.do_organ): # příznak posledního volebního období
                podminka_interval = (
                    (interval_start.dt.date <= self.tbl['zarazeni_osoby'].do_o.dt.date) # Nutná podmínka pro True: (interval_start != NaT, splněno vždy) a (do_o != NaT)
                    |  (self.tbl['zarazeni_osoby'].do_o.isna()) # Nutná podmínka pro True: (interval_start != NaT, splněno vždy) a (do_o == NaT)
                )
            else: # Pozorování: předchozí volební období => interval_end není nikdy NaT
                interval_end = self.tbl['zarazeni_osoby'].do_o\
                    .mask(self.tbl['zarazeni_osoby'].do_o.isna(), self.snemovna.do_organ)\
                    .mask(~self.tbl['zarazeni_osoby'].do_o.isna(), np.minimum(self.tbl['zarazeni_osoby'].do_o, self.snemovna.do_organ))
                podminka_interval = (interval_start.dt.date <= interval_end.dt.date)

            self.tbl['zarazeni_osoby'] = self.tbl['zarazeni_osoby'][podminka_interval]

class Poslanci(ZarazeniOsoby, Organy):

    def __init__(self, *args, **kwargs):
        log.debug("--> Poslanci")

        super(Poslanci, self).__init__(*args, **kwargs)

        # Další informace o poslanci vzhledem k volebnímu období: kontaktní údaje, adresa regionální kanceláře a podobně.
        # Některé údaje jsou pouze v aktuálním volebním období.
        self.paths['poslanci'] = f"{self.data_dir}/poslanec.unl"
        # Obsahuje GPS souřadnice regionálních kanceláří poslanců.
        self.paths['pkgps'] = f"{self.data_dir}/pkgps.unl"

        self.tbl['pkgps'], self.tbl['_pkgps'] = self.nacti_pkgps()
        self.tbl['poslanci'], self.tbl['_poslanci'] = self.nacti_poslance()

        # Zúžení na dané volební období
        if self.volebni_obdobi != -1:
            self.tbl['poslanci'] = self.tbl['poslanci'][self.tbl['poslanci'].id_organ == self.snemovna.id_organ]

        # Připojení informace o osobě, např. jméno a příjmení
        suffix = "__osoby"
        self.tbl['poslanci'] = pd.merge(left=self.tbl['poslanci'], right=self.tbl['osoby'], on='id_osoba', suffixes = ("", suffix), how='left')
        self.tbl['poslanci'] = self.drop_by_inconsistency(self.tbl['poslanci'], suffix, 0.1, 'poslanci', 'osoby')

        # Připoj informace o kanceláři
        suffix = "__pkgps"
        self.tbl['poslanci'] = pd.merge(left=self.tbl['poslanci'], right=self.tbl['pkgps'], on='id_poslanec', suffixes = ("", suffix), how='left')
        self.drop_by_inconsistency(self.tbl['poslanci'], suffix, 0.1, 'poslanci', 'pkgps', inplace=True)

        # Připoj informace o kandidátce
        suffix = "__organy"
        self.tbl['poslanci'] = pd.merge(left=self.tbl['poslanci'], right=self.tbl['organy'][["id_organ", "nazev_organ_cz", "zkratka"]], left_on='id_kandidatka', right_on='id_organ', suffixes = ("", suffix), how='left')
        self.tbl['poslanci'].drop(columns=['id_organ__organy'], inplace=True)
        self.tbl['poslanci'].rename(columns={'nazev_organ_cz': 'nazev_kandidatka_cz', 'zkratka': 'zkratka_kandidatka'}, inplace=True)
        self.drop_by_inconsistency(self.tbl['poslanci'], suffix, 0.1, 'poslanci', 'organy', t1_on='id_organ', t2_on='id_kandidatka', inplace=True)
        self.meta['nazev_kandidatka_cz'] = {"popis": 'Název strany, za kterou poslanec kandidoval, viz Organy:nazev_organ_cz', 'tabulka': 'df', 'vlastni': True}
        self.meta['zkratka_kandidatka'] = {"popis": 'Zkratka strany, za kterou poslanec kandidoval, viz Organy:nazev_organ_cz', 'tabulka': 'df', 'vlastni': True}

        # Připoj informace o kraji
        suffix = "__organy"
        self.tbl['poslanci'] = pd.merge(left=self.tbl['poslanci'], right=self.tbl['organy'][["id_organ", "nazev_organ_cz", "zkratka"]], left_on='id_kraj', right_on='id_organ', suffixes = ("", suffix), how='left')
        self.tbl['poslanci'].drop(columns=['id_organ__organy'], inplace=True)
        self.tbl['poslanci'].rename(columns={'nazev_organ_cz': 'nazev_kraj_cz', 'zkratka': 'zkratka_kraj'}, inplace=True)
        self.drop_by_inconsistency(self.tbl['poslanci'], suffix, 0.1, 'poslanci', 'organy', t1_on='id_kraj', t2_on='id_organ', inplace=True)
        self.meta['nazev_kraj_cz'] = {"popis": 'Název kraje, za který poslanec kandidoval, viz Organy:nazev_organ_cz', 'tabulka': 'df', 'vlastni': True}
        self.meta['zkratka_kraj'] = {"popis": 'Zkratka kraje, za který poslanec kandidoval, viz Organy:nazev_organ_cz', 'tabulka': 'df', 'vlastni': True}

        # Pripoj data nastoupení do parlamentu, příp. odstoupení z parlamentu
        parlament = self.tbl['zarazeni_osoby'][(self.tbl['zarazeni_osoby'].id_osoba.isin(self.tbl['poslanci'].id_osoba)) & (self.tbl['zarazeni_osoby'].nazev_typ_organ_cz == "Parlament") & (self.tbl['zarazeni_osoby'].cl_funkce=='členství')].copy()
        #parlament = parlament.sort_values(['id_osoba', 'od_o']).groupby('id_osoba').tail(1).reset_index()
        parlament = parlament.sort_values(['id_osoba', 'od_o']).groupby('id_osoba').tail(1).reset_index()
        parlament.rename(columns={'id_organ': 'id_parlament', 'od_o': 'od_parlament', 'do_o': 'do_parlament'}, inplace=True)
        self.tbl['poslanci'] = pd.merge(self.tbl['poslanci'], parlament[['id_osoba', 'id_parlament', 'od_parlament', 'do_parlament']], on='id_osoba', how="left")
        self.tbl['poslanci'] = self.drop_by_inconsistency(self.tbl['poslanci'], suffix, 0.1, 'poslanci', 'zarazeni_osoby')
        self.meta['id_parlament'] = {"popis": 'Identifikátor parlamentu, jehož byli poslanci členy, viz Organy:id_organ', 'tabulka': 'df', 'vlastni': True}
        self.meta['od_parlament'] = {"popis": 'Datum začátku zařazení poslanců do parlamentu, viz Organy:od_o', 'tabulka': 'df', 'vlastni': True}
        self.meta['do_parlament'] = {"popis": 'Datum konce zařazení poslanců do parlamentu, viz Organy:do_o', 'tabulka': 'df', 'vlastni': True}

        # Připoj informace o posledním poslaneckém klubu z 'zarazeni_osoby'.
        kluby = self.tbl['zarazeni_osoby'][(self.tbl['zarazeni_osoby'].id_osoba.isin(self.tbl['poslanci'].id_osoba)) & (self.tbl['zarazeni_osoby'].nazev_typ_organ_cz == "Klub") & (self.tbl['zarazeni_osoby'].cl_funkce=='členství')].copy()
        kluby = kluby.sort_values(['id_osoba', 'od_o']).groupby('id_osoba').tail(1).reset_index()
        kluby.rename(columns={'id_organ': 'id_klub', 'nazev_organ_cz': 'nazev_klub_cz', 'zkratka': 'zkratka_klub', 'od_o': 'od_klub', 'do_o': 'do_klub'}, inplace=True)
        self.tbl['poslanci'] = pd.merge(self.tbl['poslanci'], kluby[['id_osoba', 'id_klub', 'nazev_klub_cz', 'zkratka_klub', 'od_klub', 'do_klub']], on='id_osoba', how="left")
        self.tbl['poslanci'] = self.drop_by_inconsistency(self.tbl['poslanci'], suffix, 0.1, 'poslanci', 'zarazeni_osoby')
        self.meta['id_klub'] = {"popis": 'Identifikátor posledního klubu, do něhož byli poslanci zařazeni, viz Organy:id_organ', 'tabulka': 'df', 'vlastni': True}
        self.meta['nazev_klub_cz'] = {"popis": 'Název posledního klubu, do něhož byli poslanci zařazeni, viz Organy:nazev_organ_cz', 'tabulka': 'df', 'vlastni': True}
        self.meta['zkratka_klub'] = {"popis": 'Zkratka posledního klubu, do něhož byli poslanci zařazeni, viz Organy:zkratka', 'tabulka': 'df', 'vlastni': True}
        self.meta['od_klub'] = {"popis": 'Datum začátku zařazení poslanců do posledního klubu, viz Organy:od_o', 'tabulka': 'df', 'vlastni': True}
        self.meta['do_klub'] = {"popis": 'Datum konce zařazení poslanců do posledního klubu, viz Organy:do_o', 'tabulka': 'df', 'vlastni': True}

        self.nastav_dataframe(self.tbl['poslanci'])

        log.debug("<-- Poslanci")

    def nacti_poslance(self):
        header = {
            "id_poslanec": MItem('Int64', 'Identifikátor poslance'),
            "id_osoba": MItem('Int64', 'Identifikátor osoby, viz Osoby:id_osoba'),
            "id_kraj": MItem('Int64', 'Volební kraj, viz Organy:id_organ'),
            "id_kandidatka": MItem('Int64', 'Volební strana/hnutí, viz Organy:id_organ, pouze odkazuje na stranu/hnutí, za kterou byl zvolen a nemusí mít souvislost s členstvím v poslaneckém klubu.'),
            "id_organ":  MItem('Int64', 'Volební období, viz Organy:id_organ'),
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

