import pprint
import pandas as pd
from utils.authentication import authenticate_tableau, Environment


def get_tableau_workbook_datasources():
    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau(Environment.PROD)

    # Initialize dictionaries to store outputs
    connections_dict = dict()

    # Log in to Tableau Server and query all Data Sources on Server
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        all_workbooks, pagination_item = SERVER.workbooks.get()
        print('There are {} data sources on site...'.format(pagination_item.total_available))

        # Get initial metadata about Datasources on Server
        for workbook in all_workbooks:

            # Populate connection information about each workbook
            SERVER.workbooks.populate_connections(workbook)

            datasources_list = list()
            for connection in workbook.connections:
                datasources_list.append(connection.datasource_name)

            connections_dict[workbook.name] = {
                'workbook_name': workbook.name,
                'workbook_url': workbook.webpage_url,
                'workbook_content_url': workbook.content_url,
                'datasources': datasources_list,
            }

    pprint.pprint(connections_dict)

    #
    # OUTPUT: CSV
    #
    data = list()
    for _, wb in connections_dict.items():
        data.append(list(wb.values()))

    columns = [
        'workbook_name',
        'workbook_url',
        'workbook_content_url',
        'datasources'
    ]

    df = pd.DataFrame(data, columns=columns)
    df.to_csv('./tableau_workbook_datasources.csv', encoding='utf-8', index=False)
    print(df)


if __name__ == "__main__":
    get_tableau_workbook_datasources()
