"""
CHORDS Data Downloader - modified by Rebecca Zieber

This script calls the CHORDS API to extract data as specified by the user parameters and returns a CSV.
A new CSV is created for each instrument.

To use this script, fill out the user parameters before the main program.

User Parameter Breakdown:
    - null_value: [OPTIONAL] Enter whatever value should be used to signal no data (e.g. -999.99 or 'NaN'). Empty string by default (creates smaller files).
    - include_test: [OPTIONAL] Set to True to include boolean columns next to each data column which specify whether data collected was test data (False by default).
    - portal_url: The url for the CHORDS online portal.
    - portal_name: The name of the CHORDS portal, choose from this list (case sensitive): Barbados, Trinidad, 3D PAWS, 3D Calibration, FEWNSET, Kenya, Cayman Islands
    - data_path: The absolute folder path specifying where the CSV files should be printed to locally.
    - instrument_IDs: All the instruments to download data from. Use the Instrument Id from CHORDS portal.
    - user_email: The email login information in order to access the CHORDS online portal.
    - api_key: The API key which corresponds to the user's email address.
    - start: The timestamp from which to start downloading data (MUST be in the following format: 'YYYY-MM-DD HH:MM:SS' e.g. '2023-11-25 00:00:00').
    - end: The timestamp at which to end downloading data (MUST be in the following format: 'YYYY-MM-DD HH:MM:SS' e.g. '2023-11-31 23:59:59'). * see 'Usage'
    - columns_desired: [OPTIONAL] Enter the shortnames for the columns to include in csv (e.g. ['t1', 't2', 't3']). Includes all if left blank.
    - time_window_start: [OPTIONAL] Timestamp from which to collect subset of data (MUST be in the following format: 'HH:MM:SS'). Includes all timestamps if left blank.
    - time_window_end: [OPTIONAL] Timestamp from which to stop collecting subset of data (MUST be in the following format: 'HH:MM:SS') Includes all timestamps if left blank.

Usage:
    - If you want your download to exactly match a data download from CHORDS, it may be helpful to download a single day's worth of data off the CHORDS website 
      to see the hour at which a new day starts. Depending on the portal, CHORDS will start a new day at 0600Z, 0700Z, or 0800Z. 
    - Because CHORDS days aren't from midnight-to-midnight, the 'end' parameter must reflect the extension into the next day. 
      e.g. June 20th for FEWSNET goes from 2024-06-20 06:00:00 to 2024-06-21 05:45:59
    - Timestamps must include seconds, which could affect the 'end' parameter you chose to use. CHORDS timestamps are frequently timestamped at 01, 02, or 03 seconds,
      so it may not be good enough for the 'end' parameter to read 23:59:00, for example, you may have to specify 23:59:59 to include that last datapoint on the 59th minute.
    - To use the columns_desired parameter, which can be useful when downloading large datasets where you only care about a few columns, use the shortname listed on 
      CHORDS for the variable you want to include (e.g. mcp9808 -> mt1)

    - EXAMPLE -------------------------------- 
        A new day for the FEWS NET portal starts at 0600Z the study period is from Jan 1st through July 1st. This means the 'end' parameter will have to end on July 2nd 
            in order to capture the full CHORDS day.
        The first 24 stations transmit data every 15 minutes, so in order to capture the last observation in a day, the 'end' parameter is set to 05:45:59. 
        This analysis will only require daily rainfall data, so the shortnames 'rgt1', 'rgt2', 'rgp1', and 'rgp2' are specified in 'columns_desired'. This analysis doesn't require 
            all the 15-minute obs, the focus is on the time of day when the daily rainfall totals reset, so the variables 'time_window_start' and 'time_window_end' are used to 
            filter the data between 05:45:00 and 06:00:59 to capture this reset.

            
        null_value = ''
        include_test = False
        portal_url = r"https://3d-fewsnet.icdp.ucar.edu/" 
        portal_name = "FEWSNET"
        data_path = r"C:\\path\\to\\output\\folder\\" 
        instrument_IDs = [
            1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
        ]

        user_email = 'your-chords@email.com'
        api_key = 'your-chords-api-key' 
        start = '2024-01-01 06:00:00' 
        end = '2024-07-02 05:45:59'

        columns_desired = ['rgt1', 'rgt2', 'rgp1', 'rgp2']
        time_window_start = '05:45:00'
        time_window_end = '06:00:59' 
      

"""
import requests
from json import dumps
from json import loads
import numpy as np
from datetime import datetime, timedelta
import sys
import resources


# User Parameters ----------------------------------------------------------------------------------------------------------------

null_value = '' # OPTIONAL
include_test = False # OPTIONAL

portal_url = r"https://3d-fewsnet.icdp.ucar.edu/" 
portal_name = "FEWSNET"
data_path = r"/Users/rzieber/Downloads/" 
instrument_IDs = [
    1,2,3
]
user_email = 'rzieber@ucar.edu'
api_key = 'QSy8irrRowbi6ys-5PHe' 
start = '2024-01-01 06:00:00' 
end = '2024-07-02 05:45:59'

columns_desired = ['rgt1', 'rgt2', 'rgp1', 'rgp2'] # OPTIONAL
time_window_start = '05:45:00' # OPTIONAL
time_window_end = '06:00:59' # OPTIONAL
  

# MAIN PROGRAM ------------------------------------------------------------------------------------------------------------------------

def main():
    # user input validation
    format_str = "%Y-%m-%d %H:%M:%S"
    timestamp_start = datetime.strptime(start, format_str) 
    timestamp_end = datetime.strptime(end, format_str)
    if timestamp_start > timestamp_end:
            raise ValueError(f"Starting time cannot be after end time.\n\t\t\tStart: {timestamp_start}\t\tEnd: {timestamp_end}")
    if timestamp_start < datetime.now() - timedelta(days=365*2):
        print("\t ========================= WARNING =========================")
        print(f"\t timestamp_start before CHORDS cutoff (2 years): {timestamp_start}\n\t Will pull 2 year archive only.\n")
    if timestamp_end > datetime.now():
        print("\t ========================= WARNING =========================")
        print(f"\t timestamp_end in the future: {timestamp_end}\n\t Will pull up to today's date only.\n")

    if time_window_start != "" or time_window_end != "":
        format_str = "%H:%M:%S"
        timestamp_window_start = datetime.strptime(time_window_start, format_str).time()
        timestamp_window_end = datetime.strptime(time_window_end, format_str).time()
        if time_window_start > time_window_end:
            raise ValueError(f"The start time for the time window is after the end time: {time_window_start} > {time_window_end}")
        if time_window_start == "" or time_window_end == "":
            raise ValueError(f"Both the 'time_window_start' and 'time_window_end' variables must be populated to specify a collection timeframe.")

    portal_lookup = [
        'Barbados', 'Trinidad', '3D PAWS', '3D Calibration', 'FEWSNET', 'Kenya', 'Cayman Islands', 'Dominican Republic'
    ]
    if portal_name not in portal_lookup:
        raise ValueError(f"Please enter one of the following portal names as they appear here (case sensitive):\n\t \
                            Barbados, Trinidad, 3D PAWS, 3D Calibration, FEWSNET, Kenya, Cayman Islands")
    
    # processing loop
    for iD in instrument_IDs:
        if not isinstance(iD, int):
            raise TypeError(f"The instrument id's must be integers, passed {type(iD)} for id {iD}")

        print(f"---> Reading instrument ID {iD}\t\t\t\t\t\t\t{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if time_window_start == "" and time_window_end == "":
            time = [] # list of strings  (e.g. '2023-12-17T00:00:04Z')
            measurements = [] # list of dictionaries  (e.g. {'t1': 25.3, 'uv1': 2, 'rh1': 92.7, 'sp1': 1007.43, 't2': 26.9, 'vis1': 260, 'ir1': 255, 'msl1': 1013.01, 't3': 26.1})
            test = [] # list of strings of whether data point is a test value (either 'true' or 'false')

            total_num_measurements = 0
            total_num_timestamps = 0

            url = f"{portal_url}/api/v1/data/{iD}?start={start}&end={end}&email={user_email}&api_key={api_key}"
            response = requests.get(url=url)
            all_fields = loads(dumps(response.json())) # dictionary containing deep copy of JSON-formatted CHORDS data
            if resources.has_errors(all_fields):
                sys.exit(1)
            
            if resources.has_excess_datapoints(all_fields): # reduce timeframe in API call
                print("\t Large data request -- reducing.")
                reduced_data = resources.reduce_datapoints(all_fields['errors'][0], int(iD), timestamp_start, timestamp_end, \
                                                    portal_url, user_email, api_key, null_value)    # list
                                                                                        # e.g. [time, measurements, test, total_num_measurements]
                time = reduced_data[0]
                measurements = reduced_data[1]
                test = reduced_data[2]
                total_num_measurements = reduced_data[3]
            else:
                data = all_fields['features'][0]['properties']['data']  # list of dictionaries 
                                                                        # ( e.g. {'time': '2023-12-17T18:45:56Z', 'test': 'false', 'measurements': {'ws': 1.55, 'rain': 1}} )
                for i in range(len(data)):
                    t = resources.get_time(data[i]['time'])

                    #if t.minute != 0 and t.minute != 15 and t.minute != 30 and t.minute != 45: # only keep those timestamps not on 15 minute interval
                       #print(f"Current minute: {t.minute} | Whole timestamp: {t}")
                    time.append(str(data[i]['time']))
                    total_num_measurements += len(data[i]['measurements'].keys())
                    total_num_timestamps += 1
                    to_append = resources.write_compass_direction(dict(data[i]['measurements']), null_value)
                    measurements.append(to_append)
                    test.append(str(data[i]['test']))

        else: # if a time window was specified by user
            print(f"\t\t Time window specified.\n\t\t Returning data from {time_window_start} -> {time_window_end}")
            window_data = resources.time_window(int(iD), timestamp_start, timestamp_end, timestamp_window_start, timestamp_window_end, \
                                        portal_url, user_email, api_key, null_value) # a list [time, measurements, test, total_num_measurements]
            time = window_data[0]
            measurements = window_data[1]
            test = window_data[2]
            total_num_measurements = window_data[3]

        headers = resources.build_headers(measurements, columns_desired, include_test, portal_name) # list of strings 
        time = np.array(time)
        measurements = np.array(measurements)
        test = np.array(test)
        
        if resources.struct_has_data(measurements, time, test): 
            csv = f"\\{portal_name}_ID{iD}_{timestamp_start.date()}_{timestamp_end.date()}.csv"
            file_path = data_path + csv
            resources.csv_builder(headers, time, measurements, test, file_path, include_test, null_value)
            print(f"\t Finished writing to file.\t\t\t\t\t\t{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"\t Total number of measurements: {total_num_measurements}")
        else:
            # print("\t ========================= WARNING =========================")
            # print(f"\t No data found at specified timeframe for {portal_name} Instrument ID: {iD}\n")
            txt = f"\\{portal_name}_instrumentID_{iD}_[WARNING].txt"
            file_path = data_path + txt
            with open(file_path, 'w') as file:
                file.write("No data was found for the specified time frame.\nCheck the CHORDS portal to verify.")

    #resources.create_README(portal_name, data_path)


if __name__ == "__main__":
    main()
