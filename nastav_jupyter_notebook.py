
def nastav_pandas():
    import pandas as pd

    pd.options.display.max_colwidth = 1000
    pd.options.display.max_rows = 100

def nastav_google_colab():
    if 'google.colab' in str(get_ipython()):
        print('Instalace modulů v rámci Google Colab.')
        #! rm -r snemovna

        from getpass import getpass
        import os, sys

        user = getpass('Github user')
        password = getpass('Github password')
        os.environ['GITHUB_AUTH'] = user + ':' + password

        res = os.system("git clone \"https://$GITHUB_AUTH@github.com/jarbes/snemovna.git\" --branch master")
        print(f"Git clone command returned value: {res}")

        sys.path.insert(0,'snemovna')
