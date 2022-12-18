"""
    Author: Matt Pain
    Version: 1.0
    Date Updated: 21/11/2022
"""

# System level libraries imports
import os

# Third party libraries imports
import pandas as pd
from pathlib import Path


class ProcessIMDData:
    """
    This class takes the raw IMD data in a given directory and then converts it to a database ready format.
    """
    # Class Fields
    working_directory = os.getcwd()

    def __init__(self):
        """
        :param self.parent_path:
            gets absolute path of working directory
        :type self.parent_path:
            string

        :param self.data_file_path:
            relative file path of raw IMD data added to parent path
        :type self.data_file_path:
            string

        :param self.write_file_path:
            relative file path used to write processed IMD data
        :type self.write_file_path:
            string
        """
        self.parent_path = Path(self.working_directory)
        self.data_file_path = r'{}\\Raw\\IMD Data'.format(self.parent_path)
        self.write_file_path = r'{}\\Processed\\IMD Data'.format(self.parent_path)

    def read_imd_data(self) -> dict:

        """
        Read spreadsheet data from a given directory and creates a dictionary with
        the filenames as keys and a list of individual sheets as

        :returns spreadsheet_dictionary:
            Dictionary of individual sheets as values and spreadsheet names as keys
        :type spreadsheet_dictionary:
            dictionary
        """
        # Read in each spreadsheet
        spreadsheet_dictionary = {}

        for file in os.listdir(self.data_file_path):
            if file.endswith('.xlsx'):

                df = pd.read_excel(f'{self.data_file_path}\\{file}', sheet_name=None)

                if 'Notes' in df.keys():
                    df.pop('Notes', None)

                if 'Terms & Conditions' in df.keys():
                    df.pop('Terms & Conditions', None)

                print(f'{file} loaded successfully...')

                sheet_list = []

                for sheet in df.values():
                    sheet_list.append(sheet)

                spreadsheet_dictionary[file] = sheet_list

        print('All files loaded successfully.')

        return spreadsheet_dictionary

    @staticmethod
    def remove_columns(spreadsheet_dictionary: dict) -> dict:
        """
        Takes in a dictionary of spreadsheets containing the filename as keys and the list of each individual
        sheet as values. Checks if the file is at an LSOA resolution and removes LSOA name, District Name and District Code
        else just removes the relevant summary code (CCG, LEP etc..).

        :param spreadsheet_dictionary:
            Dictionary of individual sheets as values and spreadsheet names as keys
        :type spreadsheet_dictionary:
            dictionary

        :returns updated_spreadsheet_dictionary:
            Dictionary of individual sheets as values and spreadsheet names as keys
            with reformatted columns
        :type updated_spreadsheet_dictionary:
            dictionary
        """
        summary_files_check = [
            filename for filename
            in spreadsheet_dictionary.keys()
            if filename[6] != '_'
        ]

        lsoa_files_check = [
            filename for filename
            in spreadsheet_dictionary.keys()
            if filename[6] == '_'
        ]

        updated_spreadsheet_dictionary = {}

        for filename, sheet_list in spreadsheet_dictionary.items():

            updated_sheet_list = []

            if filename in summary_files_check:
                for df in sheet_list:
                    df.drop(df.columns[1], inplace=True, axis=1)
                    updated_sheet_list.append(df)

            if filename in lsoa_files_check:
                for df in sheet_list:
                    df.drop(df.columns[1:4], inplace=True, axis=1)
                    updated_sheet_list.append(df)

            updated_spreadsheet_dictionary[filename] = updated_sheet_list

        return updated_spreadsheet_dictionary

    @staticmethod
    def convert_to_eav_format(updated_spreadsheet_dictionary: dict) -> dict:
        """
        Converts all sheets in the updated spreadsheet dictionary to an Entity - Attribute - Value pattern.

        :param updated_spreadsheet_dictionary:
            A dictionary with the relvant columns removed by the remove_columns function
        :type updated_spreadsheet_dictionary:
            dictionary

        :returns eav_spreadsheet_dictionary:
            Dictionary with filenames as keys and list of sheets in an EAV format as values
        :type eav_spreadsheet_dictionary:
            dictionary
        """

        eav_spreadsheet_dictionary = {}

        for file_name, sheet_list in updated_spreadsheet_dictionary.items():

            eav_sheets = []

            for sheet in sheet_list:
                updated_sheet = pd.melt(
                    sheet,
                    id_vars=sheet.columns[0:2],
                    value_vars=sheet.columns[1:],
                    var_name='attribute',
                    value_name='value'
                )

                eav_sheets.append(updated_sheet)

            eav_spreadsheet_dictionary[file_name] = eav_sheets

        return eav_spreadsheet_dictionary

    def write_data(self, spreadsheet_dictionary: dict) -> None:

        """

        Writes dataframe data from a dictionary of sheet names(Keys) and dataframes(Values) to a specified local
        directory.

        :param spreadsheet_dictionary:
            A dictionary containing sheet names as keys and lists of dataframes as values.
        :type spreadsheet_dictionary:
            dictionary

        :returns None:
            This function does not return any values.
        :type None:
            None
        """

        for name, sheet_list in spreadsheet_dictionary.values():
            for i, sheet in enumerate(sheet_list):
                sheet.to_csv(f'{self.write_file_path}\\{i}_{name}', header=False)
