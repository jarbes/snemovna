
import pandas as pd

from utility import *

from Snemovna import *
from Osoby import *

from setup_logger import log

# Agenda Schůze obsahuje data schůzí Poslanecké sněmovny: schůze, body pořadu schůze a související data.
# Informace k tabulkám, viz. https://www.psp.cz/sqw/hp.sqw?k=1308

class SchuzeObecne(Snemovna):
    def __init__(self, *args, **kwargs):
        super(SchuzeObecne, self).__init__(*args, **kwargs)
        log.debug('--> SchuzeObecne')

        self.nastav_datovy_zdroj(f"https://www.psp.cz/eknih/cdrom/opendata/schuze.zip")
        log.debug('<-- SchuzeObecne')


class Schuze(SchuzeObecne, Organy):
    def __init__(self, *args, **kwargs):
        log.debug('--> Schuze')
        super(Schuze, self).__init__(*args, **kwargs)

        self.paths['schuze'] = f"{self.data_dir}/schuze.unl"
        self.paths['schuze_stav'] = f"{self.data_dir}/schuze_stav.unl"
        self.stahni_data()

        self.schuze, self._schuze = self.nacti_schuze()
        self.schuze_stav, self._schuze_stav = self.nacti_schuze_stav()

        # Připoj informace o stavu schůze
        suffix = "__schuze_stav"
        self.schuze = pd.merge(left=self.schuze, right=self.schuze_stav, on='id_schuze', suffixes = ("", suffix), how='left')
        self.schuze = drop_by_inconsistency(self.schuze, suffix, 0.1, 'schuze', 'schuze_stav')

        id_organu_dle_volebniho_obdobi = self.organy[(self.organy.nazev_organu_cz == 'Poslanecká sněmovna') & (self.organy.od_organ.dt.year == self.volebni_obdobi)].iloc[0].id_organ
        self.schuze = self.schuze[self.schuze.id_org == id_organu_dle_volebniho_obdobi]

        self.df = self.schuze
        log.debug('<-- Schuze')

    def nacti_schuze(self):
        # Obsahuje záznamy o schůzích.
        # Pro každou schůzi jsou v tabulce nejvýše dva záznamy, jeden vztahující se k návrhu pořadu, druhý ke schválenému pořadu.
        # I v případě neschválení pořadu schůze jsou dva záznamy, viz schuze:pozvanka a schuze_stav:stav.
        header = {
          # Identifikátor schůze, není to primární klíč, je nutno používat i položku schuze:pozvanka. Záznamy schůzí stejného orgánu a stejného čísla (tj. schuze:id_org a schuze:schuze), mají stejné schuze:id_schuze a liší se pouze v schuze:pozvanka.
          'id_schuze': 'Int64',
          # Identifikátor orgánu, viz org:id_org.
          'id_org': 'Int64',
          # Číslo schůze.
          'schuze': 'Int64',
          # Předpokládaný začátek schůze; viz též tabulka schuze_stav
          'od_schuze': 'string',
          # Konec schůze. V případě schuze:pozvanka == 1 se nevyplňuje.
          'do_schuze': 'string',
          # Datum a čas poslední aktualizace.
          'aktualizace': 'string',
          # Druh záznamu: null - schválený pořad, 1 - navržený pořad.
          'pozvanka': 'Int64'
        }

        _df = pd.read_csv(self.paths['schuze'], sep="|", names = header,  index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'schuze')

        # Oprava známých chybných hodnot (očividných překlepů)
        df.at[768, 'od_schuze'] = "2020-05-31 09:00"

        # Přidej sloupec 'od_schuze' typu datetime
        df['od_schuze_DT'] = pd.to_datetime(df['od_schuze'], format='%Y-%m-%d %H:%M')
        df['od_schuze_DT'] = df['od_schuze_DT'].dt.tz_localize(self.tzn)


        # Přidej sloupec 'do_schuze' typu datetime
        df['do_schuze_DT'] = pd.to_datetime(df['do_schuze'], format='%Y-%m-%d %H:%M')
        df['do_schuze_DT'] = df['do_schuze_DT'].dt.tz_localize(self.tzn)

        return df, _df

    def nacti_schuze_stav(self):
        header = {
            # Identifikátor schůze, viz schuze:id_schuze.
            'id_schuze': 'Int64',
            # Stav schůze: 1 - OK, 2 - pořad schůze nebyl schválen a schůze byla ukončena.
            'stav': 'Int64',
            # Typ schůze: 1 - řádná, 2 - mimořádná (navržená skupinou poslanců). Dle jednacího řádu nelze měnit navržený pořad mimořádné schůze.
            'typ': 'Int64',
            # Zvláštní určení začátku schůze: pokud je vyplněno, použije se namísto schuze:od_schuze.
            'text_dt': 'string',
            # Text stavu schůze, obvykle informace o přerušení.
            'text_st': 'string',
            # Podobné jako schuze_stav:text_st, pouze psáno na začátku s velkým písmenem a ukončeno tečkou.
            'tm_line': 'string'
        }

        _df = pd.read_csv(self.paths['schuze_stav'], sep="|", names = header,  index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'schuze_stav')

        assert df.id_schuze.size == df.id_schuze.nunique(), "Schůze může mít určen pouze jeden stav!"

        df['stav_CAT'] = df.stav.astype(str).mask(df.stav == 1, "OK").mask(df.stav == 2, "pořad neschválen, schůze ukončena")

        df['typ_CAT'] = df.typ.astype(str).mask(df.typ == 1, "řádná").mask(df.typ == 2, "mimořádná")

        return df, _df


# Tabulka bod_stav
# Obsahuje typy stavů bodu pořadu schůze.
class BodStav(SchuzeObecne):

    def __init__(self, *args, **kwargs):
        super(BodStav, self).__init__(*args, **kwargs)
        log.debug('--> BodStav')

        self.paths['bod_stav'] = f"{self.data_dir}/bod_stav.unl"
        self.stahni_data()
        self.bod_stav, self._bod_stav = self.nacti_bod_stav()

        self.df = self.bod_stav
        log.debug('<-- BodStav')

    def nacti_bod_stav(self):
        header = {
            # Typ stavu bodu schůze: typ 3 - neprojednatelný znamená vyřazen z pořadu či neprojednatelný z důvodu legislativního procesu.
            'id_bod_stav': 'Int64',
            # Popis stavu bodu.
            'popis': 'string'
        }

        _df = pd.read_csv(self.paths['bod_stav'], sep="|", names = header,  index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'bod_stav')

        return df, _df


# Obsahuje záznamy o bodech pořadu schůze. Body typu odpověď na písemnou interpelaci (bod_schuze:id_typ == 6) se obvykle nezobrazují, viz dále.
#Při zobrazení bodu se použijí položky bod_schuze:uplny_naz. Pokud je bod_schuze:id_tisk nebo bod_schuze:id_sd vyplněno, pak se dále použije bod_schuze:uplny_kon, případně text závislý na bod_schuze.id_typ. Poté následuje bod_schuze:poznamka.

class BodSchuze(BodStav):
    def __init__(self, *args, **kwargs):
        super(BodSchuze, self).__init__(*args, **kwargs)
        log.debug('--> BodSchuze')

        self.paths['bod_schuze'] = f"{self.data_dir}/bod_schuze.unl"
        self.stahni_data()
        self.bod_schuze, self._bod_schuze = self.nacti_bod_schuze()

        # Připoj informace o stavu bodu
        suffix = "__bod_stav"
        self.bod_schuze = pd.merge(left=self.bod_schuze, right=self.bod_stav, on='id_bod_stav', suffixes = ("", suffix), how='left')
        self.bod_schuze = drop_by_inconsistency(self.bod_schuze, suffix, 0.1, 'bod_schuze', 'bod_stav')

        self.df = self.bod_schuze
        log.debug('<-- BodSchuze')

    def nacti_bod_schuze(self):
        header = {
            # Identifikátor bodu pořadu schůze, není to primární klíč, je nutno používat i položku bod_schuze:pozvanka. Záznamy se stejným id_bod odkazují na stejný bod, i když číslo bodu může být rozdílné (během schvalování pořadu schůze se pořadí bodů může změnit).
            'id_bod': 'Int64',
             #Identifikátor schůze, viz schuze:id_schuze a též schuze:pozvanka.
            'id_schuze': 'Int64',
            # Identifikátor tisku, pokud se bod k němu vztahuje. V tomto případě lze využít bod_schuze:uplny_kon.
            'id_tisk': 'Int64',
            # Typ bodu, resp. typ projednávání. Kromě bod_schuze:id_typ == 6, se jedná o typ stavu, viz stavy:id_typ a tabulka níže. Je-li bod_schuze:id_typ == 6, jedná se o jednotlivou odpověď na písemnou interpelaci a tento záznam se obykle nezobrazuje (navíc má stejné id_bodu jako bod odpovědi na písemné interpelace a může mít různé číslo bodu).
            'id_typ': 'Int64',
            # Číslo bodu. Pokud je menší než jedna, pak se při výpisu číslo bodu nezobrazuje.
            'bod': 'Int64',
            # Úplný název bodu.
            'uplny_naz': 'string',
            # Koncovka názvu bodu s identifikací čísla tisku nebo čísla sněmovního dokumentu, pokud jsou používány, viz bod_schuze:id_tisk a bod_schuze:id_sd.
            'uplny_kon': 'string',
            # Poznámka k bodu - obvykle obsahuje informaci o pevném zařazení bodu.
            'poznamka': 'string',
            # Stav bodu pořadu, viz bod_stav:id_bod_stav. U bodů návrhu pořadu se nepoužije.
            'id_bod_stav': 'Int64',
            # Rozlišení záznamu, viz schuze:pozvanka
            'pozvanka': 'Int64',
            # Režim dle par. 90, odst. 2 jednacího řádu.
            'rj':  'Int64',
            # Poznámka k bodu, zkrácený zápis
            'pozn2': 'string',
            # Druh bodu: 0 nebo null: normální, 1: odpovědi na ústní interpelace, 2: odpovědi na písemné interpelace, 3: volební bod
            'druh_bodu': 'Int64',
            # Identifikátor sněmovního dokumentu, viz sd_dokument:id_dokument. Pokud není null, při výpisu se zobrazuje bod_schuze:uplny_kon.
            'id_sd': 'Int64',
            # Zkrácený název bodu, neoficiální.
            'zkratka': 'string'
        }

        _df = pd.read_csv(self.paths['bod_schuze'], sep="|", names = header,  index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'bod_schuze')

        return df, _df
