import os
import tableauserverclient as TSC
from dotenv import load_dotenv
load_dotenv()


class Environment:
    DEV = 'dev'
    PROD = 'prod'


def authenticate_tableau(env):
    #
    # Tableau Authentication
    #
    if env not in (Environment.DEV, Environment.PROD):
        print('Wrong environment supplied to authenticate_tableau function. Expected "dev" or "prod"')

    TOKEN_NAME = os.getenv('TABLEAU_PAT_NAME')
    TOKEN = None
    SERVER_URL = None

    if env == 'dev':
        TOKEN = os.getenv('TABLEAU_DEV_PAT')
        SERVER_URL = os.getenv('TABLEAU_DEV_SERVER_URL')

    if env == 'prod':
        TOKEN = os.getenv('TABLEAU_PROD_PAT')
        SERVER_URL = os.getenv('TABLEAU_PROD_SERVER_URL')

    auth = TSC.PersonalAccessTokenAuth(
        token_name=TOKEN_NAME,
        personal_access_token=TOKEN
    )

    server = TSC.Server(
        server_address=SERVER_URL,
        use_server_version=True
    )

    return auth, server
