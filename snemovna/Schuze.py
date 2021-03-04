
import pandas as pd

from snemovna.utility import *

from snemovna.Snemovna import *
from snemovna.PoslanciOsoby import *

from snemovna.setup_logger import log

# Agenda Schůze obsahuje data schůzí Poslanecké sněmovny: schůze, body pořadu schůze a související data.
# Informace k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1308

class SchuzeObecne(object):
    def __init__(self, *args, **kwargs):
        super(SchuzeObecne, self).__init__(*args, **kwargs)
        log.debug('--> SchuzeObecne')

        self.nastav_datovy_zdroj(f"https://www.psp.cz/eknih/cdrom/opendata/schuze.zip")
        self.stahni_data()

        log.debug('<-- SchuzeObecne')


class TabulkaSchuzeMixin(object):
    def nacti_schuze(self):
        # Obsahuje záznamy o schůzích.
        # Pro každou schůzi jsou v tabulce nejvýše dva záznamy, jeden vztahující se k návrhu pořadu, druhý ke schválenému pořadu.
        # I v případě neschválení pořadu schůze jsou dva záznamy, viz schuze:pozvanka a schuze_stav:stav.
        path = f"{self.parameters['data_dir']}/schuze.unl"
        header = {
          'id_schuze': MItem('Int64', 'Identifikátor schůze, není to primární klíč, je nutno používat i položku schuze:pozvanka. Záznamy schůzí stejného orgánu a stejného čísla (tj. schuze:id_org a schuze:schuze), mají stejné schuze:id_schuze a liší se pouze v schuze:pozvanka.'),
          'id_org': MItem('Int64', 'Identifikátor orgánu, viz Organy:id_org.'),
          'schuze': MItem('Int64', 'Číslo schůze.'),
          'od_schuze': MItem('string', 'Předpokládaný začátek schůze; viz též tabulka schuze_stav'),
          'do_schuze': MItem('string', 'Konec schůze. V případě schuze:pozvanka == 1 se nevyplňuje.'),
          'aktualizace': MItem('string', 'Datum a čas poslední aktualizace.'),
          'pozvanka__ORIG': MItem('Int64', 'Druh záznamu: null - schválený pořad, 1 - navržený pořad.')
        }

        _df = pd.read_csv(path, sep="|", names = header,  index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'schuze')
        self.rozsir_meta(header, tabulka='schuze', vlastni=False)

        # Oprava známých chybných hodnot (očividných překlepů)
        df.at[768, 'od_schuze'] = "2020-05-31 09:00"

        # Přidej sloupec 'od_schuze' typu datetime
        df['od_schuze'] = pd.to_datetime(df['od_schuze'], format='%Y-%m-%d %H:%M')
        df['od_schuze'] = df['od_schuze'].dt.tz_localize(self.tzn)


        # Přidej sloupec 'do_schuze' typu datetime
        df['do_schuze'] = pd.to_datetime(df['do_schuze'], format='%Y-%m-%d %H:%M')
        df['do_schuze'] = df['do_schuze'].dt.tz_localize(self.tzn)

        mask = {None: 'schválený pořad', 1: 'navržený pořad'}
        df['pozvanka'] = mask_by_values(df.pozvanka__ORIG, mask)
        self.meta['pozvanka'] = dict(popis='Druh záznamu.', tabulka='schuze', vlastni=True)

        self.tbl['schuze'], self.tbl['_schuze'] = df, _df

class TabulkaSchuzeStavMixin(object):
    def nacti_schuze_stav(self):
        self.paths['schuze_stav'] = f"{self.parameters['data_dir']}/schuze_stav.unl"
        header = {
            'id_schuze': MItem('Int64', 'Identifikátor schůze, viz Schuze:id_schuze.'),
            'stav__ORIG': MItem('Int64', 'Stav schůze: 1 - OK, 2 - pořad schůze nebyl schválen a schůze byla ukončena.'),
            'typ__ORIG': MItem('Int64', 'Typ schůze: 1 - řádná, 2 - mimořádná (navržená skupinou poslanců). Dle jednacího řádu nelze měnit navržený pořad mimořádné schůze.'),
            'text_dt': MItem('string', 'Zvláštní určení začátku schůze: pokud je vyplněno, použije se namísto Schuze:od_schuze.'),
            'text_st': MItem('string', 'Text stavu schůze, obvykle informace o přerušení.'),
            'tm_line': MItem('string', 'Podobné jako SchuzeStav:text_st, pouze psáno na začátku s velkým písmenem a ukončeno tečkou.')
        }

        _df = pd.read_csv(self.paths['schuze_stav'], sep="|", names = header,  index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'schuze_stav')
        self.rozsir_meta(header, tabulka='schuze_stav', vlastni=False)

        assert df.id_schuze.size == df.id_schuze.nunique(), "Schůze může mít určen pouze jeden stav!"

        df['stav'] = df.stav__ORIG.astype(str).mask(df.stav__ORIG == 1, "OK").mask(df.stav__ORIG == 2, "pořad neschválen, schůze ukončena")
        self.meta['stav'] = dict(popis='Stav schůze.', tabulka='schuze_stav', vlastni=True)

        df['typ'] = df.typ__ORIG.astype(str).mask(df.typ__ORIG == 1, "řádná").mask(df.typ__ORIG == 2, "mimořádná")
        self.meta['typ'] = dict(popis='Typ schůze.', tabulka='schuze_stav', vlastni=True)

        self.tbl['schuze_stav'], self.tbl['_schuze_stav'] = df, _df

class Schuze(TabulkaSchuzeMixin, TabulkaSchuzeStavMixin, SnemovnaZipDataMixin, SnemovnaDataFrame):
    def __init__(self, stahni=True, *args, **kwargs):
        log.debug('--> Schuze')

        org = Organy(*args, **kwargs)
        volebni_obdobi = org.volebni_obdobi
        kwargs['volebni_obdobi'] = volebni_obdobi

        super(Schuze, self).__init__(*args, **kwargs)

        if stahni == True:
            self.stahni_zip_data(f"schuze")

        organy = self.pripoj_data(org, jmeno='organy')

        self.nacti_schuze()
        self.nacti_schuze_stav()

        # Připoj informace o stavu schůze
        suffix = "__schuze_stav"
        self.tbl['schuze'] = pd.merge(left=self.tbl['schuze'], right=self.tbl['schuze_stav'], on='id_schuze', suffixes = ("", suffix), how='left')
        self.drop_by_inconsistency(self.tbl['schuze'], suffix, 0.1, 'schuze', 'schuze_stav', inplace=True)

        id_organ_dle_volebniho_obdobi = organy[(organy.nazev_organ_cz == 'Poslanecká sněmovna') & (organy.od_organ.dt.year == self.volebni_obdobi)].iloc[0].id_organ
        self.tbl['schuze'] = self.tbl['schuze'][self.tbl['schuze'].id_org == id_organ_dle_volebniho_obdobi]

        self.nastav_dataframe(self.tbl['schuze'])

        log.debug('<-- Schuze')


class TabulkaBodStavMixin(object):
    def nacti_bod_stav(self):
        path = f"{self.parameters['data_dir']}/bod_stav.unl"
        header = {
            'id_bod_stav': MItem('Int64', 'Typ stavu bodu schůze: typ 3 - neprojednatelný znamená vyřazen z pořadu či neprojednatelný z důvodu legislativního procesu.'),
            'popis': MItem('string', 'Popis stavu bodu.')
        }

        _df = pd.read_csv(path, sep="|", names = header,  index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'bod_stav')
        self.rozsir_meta(header, tabulka='bod_stav', vlastni=False)

        df['id_bod_stav__KAT'] = df.id_bod_stav.astype(str).mask(df.id_bod_stav == 3, 'neprojednatelný')
        self.meta['id_bod_stav__KAT'] = dict(popis='Typ stavu bodu schůze.', tabulka='bod_stav', vlastni=True)

        self.tbl['bod_stav'], self.tbl['_bod_stav'] = df, _df

# Tabulka bod_stav
# Obsahuje typy stavů bodu pořadu schůze.
class BodStav(TabulkaBodStavMixin, SnemovnaZipDataMixin, SnemovnaDataFrame):
    def __init__(self, stahni=True, *args, **kwargs):
        log.debug('--> BodSatv')

        super(BodStav, self).__init__(*args, **kwargs)

        if stahni == True:
            self.stahni_zip_data(f"schuze")

        self.nacti_bod_stav()
        self.nastav_dataframe(self.tbl['bod_stav'])

        log.debug('<-- BodStav')


# Obsahuje záznamy o bodech pořadu schůze. Body typu odpověď na písemnou interpelaci (bod_schuze:id_typ == 6) se obvykle nezobrazují, viz dále.
#Při zobrazení bodu se použijí položky bod_schuze:uplny_naz. Pokud je bod_schuze:id_tisk nebo bod_schuze:id_sd vyplněno, pak se dále použije bod_schuze:uplny_kon, případně text závislý na bod_schuze.id_typ. Poté následuje bod_schuze:poznamka.
class TabulkaBodSchuzeMixin(object):
    def nacti_bod_schuze(self):
        path = f"{self.parameters['data_dir']}/bod_schuze.unl"
        header = {
            'id_bod': MItem('Int64', 'Identifikátor bodu pořadu schůze, není to primární klíč, je nutno používat i položku bod_schuze:pozvanka. Záznamy se stejným id_bod odkazují na stejný bod, i když číslo bodu může být rozdílné (během schvalování pořadu schůze se pořadí bodů může změnit).'),
            'id_schuze': MItem('Int64', 'Identifikátor schůze, viz Schuze:id_schuze a též schuze:pozvanka.'),
            'id_tisk': MItem('Int64', 'Identifikátor tisku, pokud se bod k němu vztahuje. V tomto případě lze využít bod_schuze:uplny_kon.'),
            'id_typ': MItem('Int64', 'Typ bodu, resp. typ projednávání. Kromě bod_schuze:id_typ == 6, se jedná o typ stavu, viz stavy:id_typ a tabulka níže. Je-li bod_schuze:id_typ == 6, jedná se o jednotlivou odpověď na písemnou interpelaci a tento záznam se obykle nezobrazuje (navíc má stejné id_bodu jako bod odpovědi na písemné interpelace a může mít různé číslo bodu).'),
            'bod': MItem('Int64', 'Číslo bodu. Pokud je menší než jedna, pak se při výpisu číslo bodu nezobrazuje.'),
            'uplny_naz': MItem('string', 'Úplný název bodu.'),
            'uplny_kon': MItem('string', 'Koncovka názvu bodu s identifikací čísla tisku nebo čísla sněmovního dokumentu, pokud jsou používány, viz BodSchuze:id_tisk a BodSchuze:id_sd.'),
            'poznamka': MItem('string', 'Poznámka k bodu - obvykle obsahuje informaci o pevném zařazení bodu.'),
            'id_bod_stav': MItem('Int64', 'Stav bodu pořadu, viz BodStav:id_bod_stav. U bodů návrhu pořadu se nepoužije.'),
            'pozvanka': MItem('Int64', 'Rozlišení záznamu, viz Schuze:pozvanka'),
            'rj':  MItem('Int64', 'Režim dle par. 90, odst. 2 jednacího řádu.'),
            'pozn2': MItem('string', 'Poznámka k bodu, zkrácený zápis'),
            'druh_bodu': MItem('Int64', 'Druh bodu: 0 nebo null: normální, 1: odpovědi na ústní interpelace, 2: odpovědi na písemné interpelace, 3: volební bod'),
            'id_sd': MItem('Int64', 'Identifikátor sněmovního dokumentu, viz sd_dokument:id_dokument. Pokud není null, při výpisu se zobrazuje BodSchuze:uplny_kon.'),
            'zkratka': MItem('string', 'Zkrácený název bodu, neoficiální.')
        }

        _df = pd.read_csv(path, sep="|", names = header,  index_col=False, encoding='cp1250')
        df = pretypuj(_df, header, 'bod_schuze')
        self.rozsir_meta(header, tabulka='bod_schuze', vlastni=False)

        self.tbl['bod_schuze'], self.tbl['_bod_schuze'] = df, _df

class BodSchuze(TabulkaBodSchuzeMixin, SnemovnaZipDataMixin, SnemovnaDataFrame):
    def __init__(self, stahni=True, *args, **kwargs):
        log.debug('--> BodSchuze')

        super(BodSchuze, self).__init__(*args, **kwargs)

        if stahni == True:
            self.stahni_zip_data(f"schuze")
        kwargs['stahni'] =  False

        bod_stav = self.pripoj_data(BodStav(*args, **kwargs), jmeno='bod_stav')

        self.nacti_bod_schuze()

        # Připoj informace o stavu bodu
        suffix = "__bod_stav"
        self.tbl['bod_schuze'] = pd.merge(left=self.tbl['bod_schuze'], right=self.tbl['bod_stav'], on='id_bod_stav', suffixes = ("", suffix), how='left')
        self.drop_by_inconsistency(self.tbl['bod_schuze'], suffix, 0.1, 'bod_schuze', 'bod_stav', inplace=True)

        self.nastav_dataframe(self.tbl['bod_schuze'])

        log.debug('<-- BodSchuze')

