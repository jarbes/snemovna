
from Snemovna import *
from Osoby import *
from Schuze import *

from setup_logger import log

# Stenozáznamy jsou těsnopisecké záznamy jednání Poslanecké sněmovny a jejích orgánů. V novějších volebních období obsahují časový úsek řádově 10 minut (případně mimo doby přerušení a podobně). Jsou číslovány v číselné řadě od začátku schůze.

class StenoObecne(Snemovna):
    def __init__(self, *args, **kwargs):
        super(StenoObecne, self).__init__(*args, **kwargs)

        self.nastav_datovy_zdroj(f"https://www.psp.cz/eknih/cdrom/opendata/steno.zip")


# Tabulka steno
# Obsahuje záznamy o jednotlivých stenozáznamech (turnech). Položky od_t a do_t nemusí ve všech případech obsahovat správná data, zvláště v případech písařských chyb a obvykle se v dohledné době opraví.

class Steno(StenoObecne, Organy):
    def __init__(self, *args, **kwargs):
        super(Steno, self).__init__(*args, **kwargs)

        self.paths['steno'] = f"{self.data_dir}/steno.unl"
        #self.paths['steno_bod'] = f"{self.data_dir}/steno_bod.unl"
        self.stahni_data()

        self.steno, self._steno = self.nacti_steno()

        id_organu_dle_volebniho_obdobi = self.organy[(self.organy.nazev_organu_cz == 'Poslanecká sněmovna') & (self.organy.od_organ.dt.year == self.volebni_obdobi)].iloc[0].id_organ
        self.steno = self.steno[self.steno.id_org == id_organu_dle_volebniho_obdobi]

        self.df = self.steno

    def nacti_steno(self):
        header = {
            'id_steno': 'Int64', #Identifikátor stenozáznamu
            'id_org': 'Int64', # Identifikátor orgánu stenozáznamu (v případě PS je to volební období), viz org:id_org.
            'schuze': 'Int64', # Číslo schůze.
            'turn': 'Int64', # Číslo stenozáznamu (turn). Pokud číselná řada je neúplná, tj. obsahuje mezery, pak chybějící obsahují záznam z neveřejného jednání. V novějších volebních období se i v těchto případech "stenozáznamy" vytvářejí, ale obsahují pouze informaci o neveřejném jednání.
            'od_steno': 'string', # Datum začátku stenozáznamu.
            'jd': 'Int64', # Číslo jednacího dne v rámci schůze (používá se např. při konstrukci URL na index stenozáznamu dle dnů).
            'od_t': 'Int64', # Čas začátku stenozáznamu v minutách od začátku kalendářního dne; pokud je null či menší než nula, není známo. Tj. převod na čas typu H:M je pomocí H = div(od_t, 60), M = mod(od_t, 60).
            'do_t': 'Int64' # Čas konce stenozáznamu v minutách od začátku kalendářního dne; pokud je null či menší než nula, není známo. V některých případech může být od_t == do_t; v některých případech může být i od_t > do_t -- platné pouze v případě, že během stena dojde k změně kalendářního dne (například 23:50 - 00:00).
        }

        _df = pd.read_csv(self.paths['steno'], sep="|", names = header,  index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'steno')

        # Přidej sloupec 'od_schuze' typu datetime
        df['od_steno_DT'] = pd.to_datetime(df['od_steno'], format='%Y-%m-%d')
        df['od_steno_DT'] = df['od_steno_DT'].dt.tz_localize(self.tzn)

        return df, _df


# Tabulka steno_bod
# Obsahuje záznamy o začátku či pokračování projednávání bodu schůze. Nelze úplně předpokládat, že text stenozáznamu mezi dvěma po sobě následujícími začátky projednávání bodů pořadu schůze budou obsahovat pouze jednání o prvním bodu, tj. projednávání bodu může skončit a poté může následovat procedurální jednání či vystoupení mimo body pořadu schůze.

class StenoBod(Steno, Organy):
    def __init__(self, *args, **kwargs):
        super(StenoBod, self).__init__(*args, **kwargs)

        self.paths['steno_bod'] = f"{self.data_dir}/steno_bod.unl"
        self.stahni_data()

        self.steno_bod, self._steno_bod = self.nacti_steno_bod()

        # Merge steno
        suffix = "__steno"
        self.steno_bod = pd.merge(left=self.steno_bod, right=self.steno, on='id_steno', suffixes = ("", suffix), how='left')
        self.steno_bod = drop_by_inconsistency(self.steno_bod, suffix, 0.1)

        id_organu_dle_volebniho_obdobi = self.organy[(self.organy.nazev_organu_cz == 'Poslanecká sněmovna') & (self.organy.od_organ.dt.year == self.volebni_obdobi)].iloc[0].id_organ
        self.steno_bod = self.steno_bod[self.steno_bod.id_org == id_organu_dle_volebniho_obdobi]

        self.df = self.steno_bod

    def nacti_steno_bod(self):
        header = {
                'id_steno': 'Int64', #Identifikátor stenozáznamu, viz steno:id_steno.
                'aname': 'Int64', # Pozice v indexu jednacího dne.
                'id_bod': 'Int64', # Identifikace bodu pořadu schůze, viz bod_schuze:id_bod. Je-li null či 0, pak pro daný úsek stenozáznamů není známo číslo bodu (např. každé přerušení schůze znamená při automatickém zpracování neznámé číslo bodu).
        }

        _df = pd.read_csv(self.paths['steno_bod'], sep="|", names = header,  index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'steno_bod')

        return df, _df


# Tabulka rec
# Obsahuje záznamy o vystoupení řečníka.
# Obvykle se ve vystoupení střídá předsedající a řečník, v některých případech (např. při schvalování pozměňovacích návrhů ve třetím čtení vystupují po předsedajícím zpravodaj a zástupce navrhovatele).
# Zvláštní situace nastává např. v okamžiku střídání předsedajících schůze či pokud po sobě následují vystoupení dvou poslanců, kteří mohou být v daný okamžik předsedajícími schůze (předseda, místopředseda a poslanec určený řízením ustavující schůze do okamžiku zvolení předsedy). V položce druh je pak nastavena role řečníka.
# Pokud je druh == 4, tj. předsedající, nemusí to automaticky znamenat, že v rámci jeho vystoupení se bude jednat pouze o řízení schůze - ačkoliv by řídící schůze se měl vyvarovat projevů jiných než k řízení schůze, může se stát, že pokud to nikdo nerozporuje, může vystoupit i s jiným projevem (např. za situace, kdy není k dispozici žádný místopředseda či předseda PS, který by za něj převzal řízení schůze).
# Záznamy v druh typu ověřeno jsou zkontrolovány na základě automatického vyhledání záznamů o vystoupení, které neodpovídají jejich obvyklému řazení.


class StenoRec(Steno, Osoby, BodSchuze):
    def __init__(self, *args, **kwargs):
        super(StenoRec, self).__init__(*args, **kwargs)

        self.paths['steno_rec'] = f"{self.data_dir}/rec.unl"
        self.stahni_data()

        self.steno_rec, self._steno_rec = self.nacti_steno_rec()

        # Merge steno
        suffix = "__steno"
        self.steno_rec = pd.merge(left=self.steno_rec, right=self.steno, on='id_steno', suffixes = ("", suffix), how='left')
        self.steno_rec = drop_by_inconsistency(self.steno_rec, suffix, 0.1)

        id_organu_dle_volebniho_obdobi = self.organy[(self.organy.nazev_organu_cz == 'Poslanecká sněmovna') & (self.organy.od_organ.dt.year == self.volebni_obdobi)].iloc[0].id_organ
        self.steno_rec = self.steno_rec[self.steno_rec.id_org == id_organu_dle_volebniho_obdobi]

        # Merge osoby
        suffix = "__osoby"
        self.steno_rec = pd.merge(left=self.steno_rec, right=self.osoby, on='id_osoba', suffixes = ("", suffix), how='left')
        self.steno_rec = drop_by_inconsistency(self.steno_rec, suffix, 0.1)

        # Merge bod schuze
        #suffix = "__bod_schuze"
        #self.steno_rec = pd.merge(left=self.steno_rec, right=self.bod_schuze, on='id_bod', suffixes = ("", suffix), how='left')
        #self.steno_rec = drop_by_inconsistency(self.steno_rec, suffix, 0.1)

        self.df = self.steno_rec

    def nacti_steno_rec(self):
        header = {
                'id_steno': 'Int64', #Identifikátor stenozáznamu, viz steno:id_steno.
                'id_osoba': 'Int64', #Identifikátor osoby, viz osoba:id_osoba.
                'aname': 'Int64', # Identifikace vystoupení v rámci stenozáznamu.
                'id_bod': 'Int64', # Identifikace bodu pořadu schůze, viz bod_schuze:id_bod. Je-li null či 0, pak pro daný úsek stenozáznamů není známo číslo bodu (např. každé přerušení schůze znamená při automatickém zpracování neznámé číslo bodu).
                'druh': 'Int64' #Druh vystoupení řečníka: 0 či null - neznámo, 1 - nezpracováno, 2 - předsedající (ověřeno), 3 - řečník (ověřeno), 4 - předsedající, 5 - řečník.
        }

        _df = pd.read_csv(self.paths['steno_rec'], sep="|", names = header,  index_col=False, encoding='cp1250')
        df = self.pretipuj(_df, header, 'steno_rec')

        df['druh_CAT'] = df.druh.astype(str).\
            mask(df.druh.isin([0, None]), 'neznámo').\
            mask(df.druh == 1, 'nezpracováno').\
            mask(df.druh == 2, 'předsedající (ověřeno)').\
            mask(df.druh == 3, 'řečník (ověřeno)').\
            mask(df.druh == 4, 'předsedající').\
            mask(df.druh == 5, 'řečník')

        return df, _df
