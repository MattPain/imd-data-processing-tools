"""
    Author: Matt Pain
    Version: 1.0
    Date Updated: 21/11/2022
"""

# Class imports
from ETLTools.IMDProcessingClass import ProcessIMDData


def main():
    # Step 1 - process IMD data
    imd_processing = ProcessIMDData()
    spreadsheet_dictionary = imd_processing.read_imd_data()
    updated_spreadsheet_dictionary = imd_processing.remove_columns(spreadsheet_dictionary)
    eav_spreadsheet_dictionary = imd_processing.convert_to_eav_format(updated_spreadsheet_dictionary)
    imd_processing.write_data(eav_spreadsheet_dictionary)

    # Step 2 - process lookup data


if __name__ == '__main__':
    main()
