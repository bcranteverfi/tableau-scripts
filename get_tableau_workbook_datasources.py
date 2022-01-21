import pprint
import pandas as pd
from utils.authentication import authenticate_tableau, Environment


def get_tableau_workbook_datasources():
    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau(Environment.PROD)

    # Initialize dictionaries to store outputs
    datasources_store = list()

    # Log in to Tableau Server and query all Data Sources on Server
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        all_workbooks, pagination_item = SERVER.workbooks.get()
        print('There are {} data sources on site...'.format(pagination_item.total_available))

        # Get initial metadata about Datasources on Server
        for workbook in all_workbooks:

            # Populate connection information about each workbook
            SERVER.workbooks.populate_connections(workbook)

            for connection in workbook.connections:
                datasources_store.append(
                    {
                        'workbook_name': workbook.name,
                        'workbook_url': workbook.webpage_url,
                        'workbook_content_url': workbook.content_url,
                        'datasource_name': connection.datasource_name
                    }
                )

    pprint.pprint(datasources_store)

    #
    # OUTPUT: CSV
    #
    data = list()
    for row in datasources_store:
        data.append(list(row.values()))

    columns = [
        'workbook_name',
        'workbook_url',
        'workbook_content_url',
        'datasource_name'
    ]

    df = pd.DataFrame(data, columns=columns)
    df.to_csv('./tableau_workbook_datasources.csv', encoding='utf-8', index=False)
    print(df)


if __name__ == "__main__":
    get_tableau_workbook_datasources()
