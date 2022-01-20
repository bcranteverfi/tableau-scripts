from tableau_file_helpers import unzip_packaged_tableau_file
from tableau_object_helpers import get_tsc_obj_path

# # Limit 10 downloads per object type for testing
# TEST_LIMIT = slice(0, 10)

# # Download all objects
TEST_LIMIT = slice(-1)


def download_datasources(tsc_auth, tsc_server, metadata):
    # Log in to Tableau tsc_server and query all Data Sources on tsc_server
    with tsc_server.auth.sign_in_with_personal_access_token(tsc_auth):
        all_datasources, pagination_item = tsc_server.datasources.get()
        print('Downloading {} datasources...'.format(pagination_item.total_available))

        # Filter out text files
        text_filters = ('hyper', 'webdata-direct', 'textscan')

        # Download each Data Source, extract the .tds files, and store path to each .tds in dictionary.
        for datasource in all_datasources[TEST_LIMIT]:
            datasource_path = get_tsc_obj_path(datasource, metadata)
            if datasource.datasource_type not in text_filters:
                # Download Packaged Tableau Data Source file (.tdsx)
                zipped_ds_path = tsc_server.datasources.download(
                    datasource.id,
                    filepath=datasource_path,
                    include_extract=False
                )
                print(zipped_ds_path)

                # Extract Tableau Data Source file (.tds)
                if zipped_ds_path.rsplit('.')[-1] == 'tdsx':
                    print('Found Packaged Datasource (.tdsx). Extracting Tableau Datasource file (.tds)...')
                    unzip_packaged_tableau_file(
                        zipped_file=zipped_ds_path,
                        output_file_type='tds',
                        output_dir=datasource_path,
                        obj_name=datasource.name
                    )

                    # # Remove all .tdsx from temporary directory
                    # delete_tmp_files_of_type('tdsx', datasource_path)

    return


def download_workbooks(tsc_auth, tsc_server, metadata):

    # Log in to Tableau tsc_server and query all Workbooks on tsc_server
    with tsc_server.auth.sign_in_with_personal_access_token(tsc_auth):
        all_workbooks, pagination_item = tsc_server.workbooks.get()
        print('Downloading {} workbooks...'.format(pagination_item.total_available))

        # Download each Workbook, extract the .twb files, and store path to each .twb in dictionary.
        for workbook in all_workbooks[TEST_LIMIT]:
            workbook_path = get_tsc_obj_path(workbook, metadata)

            # Download Packaged Tableau Workbook file (.twbx)
            zipped_wb_path = tsc_server.workbooks.download(
                workbook.id,
                filepath=workbook_path,
                include_extract=False
            )
            print(zipped_wb_path)

            # Extract Tableau Workbook file (.twb)
            if zipped_wb_path.rsplit('.')[-1] == 'twbx':
                print('Found Packaged Workbook (.twbx). Extracting Tableau Workbook file (.twb)...')
                unzip_packaged_tableau_file(
                    zipped_file=zipped_wb_path,
                    output_file_type='twb',
                    output_dir=workbook_path,
                    obj_name=workbook.name
                )

                # # Remove all .twbx from temporary directory
                # delete_tmp_files_of_type('twbx', workbook_path)

    return
