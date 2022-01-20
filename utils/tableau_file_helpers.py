import os
import zipfile
from pathlib import Path


#
# TSC Helpers - File Handling
#

def convert_tableau_file_to_xml(file_path):
    """
    Converts a given Tableau file to XML

    Args:
        file_path (str): The path to the file to convert
    Returns:
        converted_file_path (str): The path to the converted file

    """
    in_file_path = Path(os.getcwd() + '/' + file_path)
    converted_file_path = in_file_path.rename(in_file_path.with_suffix('.xml'))
    return converted_file_path


def unzip_packaged_tableau_file(zipped_file, output_file_type, output_dir, obj_name=None):
    """
    Tableau Packaged Workbooks (.twbx) and Packaged Datasources (.tdsx) are zip archives.
    Here we extract the given Workbook (.twb) or Datasource (.tds) only and return a path to the unzipped file.

    Args:
        zipped_file      (str): A path generated from the Download method of the Tableau Python Server Client Library
        output_file_type (str): The desired file extension ('twb', 'tds')
        output_dir       (str): Name of directory to store unzipped file ('tmp', 'tableau_files')
        obj_name         (str): The name of object downloaded from a Tableau Server
    Returns:
        unzipped_path    (str): The path to the unzipped file

    """

    #
    # Input Validations
    #

    # File Types
    valid_output_file_types = ('tds', 'twb')
    if output_file_type not in valid_output_file_types:
        print('Bad output_file_type provided. Expected inputs: \"tds\" or \"twb\".')

    # File Names
    valid_obj_name = obj_name.replace('/', '|')

    # Output directory
    if './' not in output_dir:
        output_dir = str('./' + output_dir)

    #
    # Unzip packaged files (.twbx & .tdsx)
    #
    with zipfile.ZipFile(zipped_file) as packaged_file:
        for f in packaged_file.namelist():
            if str('.' + output_file_type) in f:
                unzipped_path = packaged_file.extract(f, output_dir)
                readable_file_path = unzipped_path.replace(f, str(valid_obj_name + '.' + output_file_type))
                os.rename(unzipped_path, readable_file_path)

                return readable_file_path


def delete_tmp_files_of_type(filetype_str, dir_name):
    """
    Delete files of a given extension type and directory from file system

    Args:
        filetype_str (str): The file extension of the file(s) to be removed: ex. 'html'
        dir_name (str): The directory containing the file(s) to be removed: ex. 'tmp'
    Return:
        None
    """
    tmp_dir = os.getcwd() + '/' + dir_name
    for f in os.listdir(Path(tmp_dir)):
        if str('.' + filetype_str) in f:
            os.remove(Path(tmp_dir + '/' + f))
