all: test test_nb

.PHONY: test test%

test:
	python -m unittest discover -s tests

test_nb: test_nb_poslanci_osoby test_nb_hlasovani test_nb_schuze test_nb_stenozaznamy test_nb_stenotexty

test_nb_poslanci_osoby:
	jupyter nbconvert --to notebook --execute PoslanciOsoby\ -\ popis.ipynb --stdout 1>/dev/null

test_nb_hlasovani:
	jupyter nbconvert --to notebook --execute Hlasovani\ -\ popis.ipynb --stdout 1>/dev/null

test_nb_schuze:
	jupyter nbconvert --to notebook --execute Schuze\ -\ popis.ipynb --stdout 1>/dev/null

test_nb_stenozaznamy:
	jupyter nbconvert --to notebook --execute Stenozaznamy\ -\ popis.ipynb --stdout 1>/dev/null

test_nb_stenotexty:
	jupyter nbconvert --to notebook --execute Stenotexty\ -\ popis.ipynb --stdout 1>/dev/null
