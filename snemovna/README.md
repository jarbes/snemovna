<b>Jmenné konvence:</b>
* tabulky a sloupce tabulek česky dle konvence v zdrojových datech
* proměnné, které se semanticky vztahují k dění v poslanecké sněmovně, česky bez diakritiky nebo anglicky
* ostatní proměnné anglicky
* komentáře a vysvětlení česky s diakritikou
* funkce anglicky

Snemovna(object):

PoslanciOsobyObecne(Snemovna):
TypOrganu(PoslanciOsobyObecne):
Organy(TypOrganu):
TypFunkce(TypOrganu):
Funkce(Organy, TypFunkce):
Osoby(PoslanciOsobyObecne):
OsobyZarazeni(Funkce, Organy, Osoby):
Poslanci(Osoby, Organy):

HlasovaniObecne(Snemovna):
Hlasovani(HlasovaniObecne, Organy):
ZmatecneHlasovani(Hlasovani):
ZpochybneniHlasovani(Hlasovani):
! ZpochybneniHlasovaniPoslancem(ZpochybneniHlasovani, Osoby):
! OmluvyPoslance(HlasovaniObecne, Poslanci, Organy):
HlasovaniPoslance(Hlasovani, Poslanci, Organy):

SchuzeObecne(Snemovna):
Schuze(SchuzeObecne, Organy):
! BodSchuzeStav(SchuzeObecne):
BodSchuze(BodStav):


! StenozaznamyObecne(Snemovna):
! Stenozaznamy(StenoObecne, Organy):
! BodStenozaznamu(Steno, Organy):
! RecnikStenozaznamu(Steno, Osoby, BodSchuze):

Stenotexty(StenoRec, OsobyZarazeni):

Meta(object):

