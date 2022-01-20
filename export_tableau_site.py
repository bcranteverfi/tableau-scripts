from utils.authentication import authenticate_tableau, Environment
from utils.tableau_object_helpers import get_tableau_metadata_dict
from utils.tableau_download_helpers import download_datasources, download_workbooks


#
# Export Tableau Site
#
# Download all Workbook and Datasource files locally from a given Tableau Site.
#
def export_tableau_site():
    #
    # Tableau Authentication
    #
    AUTHENTICATION, SERVER = authenticate_tableau(Environment.PROD)

    #
    # Combine Tableau Server Object Metadata
    #
    meta_dict = get_tableau_metadata_dict(AUTHENTICATION, SERVER)

    #
    # Download Each Tableau Server Object to its Parent Folder
    #
    download_datasources(AUTHENTICATION, SERVER, meta_dict)
    download_workbooks(AUTHENTICATION, SERVER, meta_dict)


if __name__ == "__main__":
    export_tableau_site()
