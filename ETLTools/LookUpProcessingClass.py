"""
    Author: Matt Pain
    Version: 1.0
    Date Updated: 19/12/2022
"""

# System level libraries imports
import os

# Third party libraries imports
import pandas as pd
from pathlib import Path


class ProcessLookupTable:
    """
    This class creates the master lookup table to use in the database.
    It integrates all possible geographic lookups and creates bridge tables to dela with many - many relationships.
    The following tables can be created:
    - LookupMaster
    - LADBridge
    - LADUTBridge
    - LEPBridge
    - CCGBridge

    :param self.working_directory:
        current working directory
    :type self.working_directory:
        string

    :param self.bridge_table_index_dictionary:
        dictionary of column indexes to be used when creating the bridge tables.
    :type self.bridge_table_index_dictionary:
        dictionary

    :param self.remove_columns_index:
        list of column indices to be removed from master lookup table.
    :type self.remove_columns_index:
        list
    """
    # Class Fields
    working_directory = os.getcwd()
    bridge_table_index_dictionary = {
        'lad_bridge': [2, 3],
        'lad_ut_bridge': [6, 7],
        'lep_bridge': [10, 11],
        'ccg_bridge': [14, 16]
    }
    remove_columns_index = [
        3,
        4,
        5,
        7,
        8,
        9,
        11,
        12,
        13,
        15,
        16
    ]

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
        self.data_file_path = r'{}\\Raw\\Lookups'.format(self.parent_path)
        self.write_file_path = r'{}\\Processed\\Lookups'.format(self.parent_path)

    def read_lookup_data(self) -> dict:
        """
        Read csv data from a given directory and creates a dictionary
        of lookup dataframes

        :returns lookup_dictionary:
            List of dataframes containing each lookup
        :type lookup_dictionary:
            dictionary
        """

        return {file: pd.read_csv(f'{self.data_file_path}\\{file}')
                for file
                in os.listdir(self.data_file_path)
                }

    @staticmethod
    def create_master(lookup_dictionary: dict) -> pd.DataFrame:
        """
        This function creates the master lookup table for the database. This function includes the following fields:
        - lsoa_code
        - lsoa_name
        - local_authority_district_code
        - local_authority_district_code_ut
        - local_enterprise_partnership_code
        - clinical_commisioning_group_code

        :param lookup_dictionary:
            Dictionary of individual lookup files
        :type lookup_dictionary:
            dictionary

        :returns lookup_master:
            Master lookup dataframe

        :type lookup_master:
            dataframe
        """
        # First join: LSOA_District_lookup to LSOA_DistrictUT_lookup
        df_join_one = pd.merge(
            left=lookup_dictionary['LSOA_District_lookup.csv'],
            right=lookup_dictionary['LSOA_DistrictUT_lookup.csv'],
            left_on='LSOA code (2011)',
            right_on='LSOA11CD',
            how='left')

        # Second join: df_join_one to LSOA_LEP_lookup
        df_join_two = pd.merge(
            left=df_join_one,
            right=lookup_dictionary['LSOA_LEP_lookup.csv'],
            left_on='LSOA code (2011)',
            right_on='LSOA11CD',
            how='left')

        # Third join: df_join_two to LSOA_CCG_lookup
        df_join_three = pd.merge(
            left=df_join_two,
            right=lookup_dictionary['LSOA_CCG_lookup.csv'],
            left_on='LSOA code (2011)',
            right_on='LSOA11CD',
            how='left')

        return df_join_three

    def create_bridge_tables(self, lookup_table: pd.DataFrame) -> list:
        """
        This function takes in the master lookup before columns have been removed and divides it into bridge tables:
        - LADBridge
        - LADUTBridge
        - LEPBridge
        - CCGBridge

        :param lookup_table:
            Master lookup table before columns have been removed
        :type lookup_table:
            dataframe

        :returns bridge_table_list_duplicates_removed:
            List of bridge tables as specified in function overview
        :type bridge_table_list_duplicates_removed:
            list
        """

        # Split and add lookup tables to bridge tables list
        bridge_table_list = []

        for lookup_name in self.bridge_table_index_dictionary.values():
            df = lookup_table[lookup_table.columns[lookup_name]]
            bridge_table_list.append(df)

        # Remove duplicate values in columns
        bridge_table_list_duplicates_removed = []

        for df in bridge_table_list:
            updated_df = pd.DataFrame(data={
                'code': df[df.columns[0]].unique(),
                'name': df[df.columns[1]].unique()
            })
            bridge_table_list_duplicates_removed.append(updated_df)

        return bridge_table_list_duplicates_removed

    def remove_columns(self, lookup_table: pd.DataFrame) -> pd.DataFrame:
        """
        This function removes duplicate columns from the lookup table before database loading.
        Note the change is inplace.

        :param lookup_table:
            Joined lookup table
        :type lookup_table:
            dataframe

        :param columns_index_list:
            List of column indeces to be removed
        :type columns_index_list:
            list

        :returns cleaned_lookup_table:
            Master lookup table with columns removed
        :type cleaned_lookup_table:
            dataframe
        """
        columns_list = [lookup_table.columns[column_index] for column_index in self.remove_columns_index]

        lookup_table.drop(
            columns_list,
            inplace=True,
            axis=1)

        return lookup_table

    def write_lookups(self, master_lookup: pd.DataFrame, bridge_tables: list) -> None:
        """
        This function writes the master lookup table and the associated bridge tables ready for copying into the
        database.

        :param master_lookup:
            master lookup dataframe
        :type master_lookup:
            dataframe

        :param bridge_tables:
            list of bridge tables
        :type bridge_tables:
            list
        """
        # Write Master
        master_lookup.to_csv(f"{self.write_file_path}\\MasterLookUp.csv", header=False, index=False)

        # Write bridge tables
        for df, file_name in zip(bridge_tables, self.bridge_table_index_dictionary.keys()):
            df.to_csv(f"{self.write_file_path}\\{file_name}.csv", header=False, index=False)