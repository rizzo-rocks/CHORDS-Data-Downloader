import requests
from json import dumps
from json import loads
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, time as dt_time
import sys
import math
from .classes import TimestampError

# Functions -------------------------------------------------------------------------------------------------------------------------

"""
Helper method for write_compass_direction().
Takes the wind direction value in degrees and maps it to the compass reading. Returns the compass string.
"""
def wind_direction_mapper(wind_dir:int, null_value) -> str:
    if not isinstance(wind_dir, int):
        raise TypeError(f"The 'wind_dir' parameter in wind_direction_mapper() should be of type <int>, passed: {type(wind_dir)}")

    wind_dir_lookup = [
        'N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW', 'N'
    ]
    wind_val_lookup = [
        0, 22.5, 67.5, 112.5, 157.5, 202.5, 247.5, 292.4, 337.5, 360
    ]

    for i in range(len(wind_val_lookup)-1):
        if wind_dir < 0 or wind_dir > 360:
            return null_value
        if wind_dir >= wind_val_lookup[i] and wind_dir <= wind_val_lookup[(i+1)]:
            return wind_dir_lookup[i]   
        
    print(f"Error locating wind direction: {wind_dir}")    
    sys.exit(1)

"""
Helper method for get_columns(), build_headers(), and write_compass_direction().
Takes a shortname representing a measurement and returns True if that shortname is for wind direction, False otherwise.
"""
def is_wind_dir(measurement:str) -> bool:
    if not isinstance(measurement, str):
        raise TypeError(f"The 'measurement' parameter in is_wind_dir() should be of type <str>, passed: {type(measurement)}")
    
    wind_dir_shortnames = [ # as new shortnames get added to database, this must be updated
        'wd', 'wgd', 'wind_direction'
    ]

    if measurement in wind_dir_shortnames:
        return True

    return False

"""
Takes the dictionary entry of the measurements at a timestamp obtained from API. Adds the compass rose classification 
for all measurements containing wind direction or wind gust direction to a new updated dictionary and returns it.
"""
def write_compass_direction(dictionary:dict, null_value) -> dict:
    if not isinstance(dictionary, dict):
        raise TypeError(f"The 'dictionary' parameter in write_compass_direction() should be of type <dict>, passed: {type(dictionary)}")

    updated_dict = dictionary
    for measurement in list(dictionary.keys()):
        if is_wind_dir(measurement):
            compass_dir = {f'{measurement}_compass_dir': wind_direction_mapper(int(dictionary[measurement]), null_value)}
            updated_dict.update(compass_dir)  

    return updated_dict

"""
Helper function for build_headers() that checks if a header is in the known set of headers.
"""
def headers_are_valid(columns_desired:list, columns_found:list, portal_name:str) -> bool:
    if not isinstance(columns_desired, list):
        raise TypeError(f"The 'columns_desired' parameter of headers_are_valid() should be of type <list>, passed: {type(columns_desired)}")
    if not isinstance(columns_found, list):
        raise TypeError(f"The 'columns_found' parameter of headers_are_valid() should be of type <list>, passed: {type(columns_found)}")
    if not isinstance(portal_name, str):
        raise TypeError(f"The 'portal_name' parameter of headers_are_valid() should be of type <str>, passed: {type(portal_name)}")

    for col in columns_desired:
        if col.endswith('compass_dir'):
            print("\t ======================= ERROR =======================")
            print("\t 'compass_dir' columns are not valid.")
            print(f"\t Only specify shortnames found in {portal_name} associated with the instrument id.")
            return False
        if col not in columns_found:
            print("\t ======================= ERROR =======================")
            print(f"\t Could not locate desired column '{col}' in data stream.")
            str_1 = ""
            for j in columns_found:
                str_1 += f"{j}  "
            print(f"\t Column(s) identified in data stream: {str_1}")
            return False
        
    return True

"""
Helper function for get_columns that applies a kind of sort to the headers. Data from API stream comes into main() via
Python dictionaries, which randomly store key/value pairs, and so the column headers will be randomly ordered without sort.
Takes a list of column names, looks up the sort associated with the portal, and returns a new list of organized header names.
"""
def sort_columns(columns:list, portal_name:str) -> list:
    if not isinstance(columns, list):
        raise TypeError(f"The 'columns' parameter in sort_columns() should be of type <list>, passed: {type(columns)}")
    if not isinstance(portal_name, str):
        raise TypeError(f"The 'portal_name' parameter in sort_columns() should be of type <str>, passed: {type(portal_name)}")

    b_sort = [ # Barbados
        't1', 't2', 't3', 'rh1', 'msl1', 'sp1', 'ws', 'wd', 'rain', 'vis1', 'ir1', 'uv1'
    ]
    t_sort = [ # Trinidad
        'bt1', 'mt1', 'ht1', 'bp1', 'bh1', 'hh1', 'ws', 'wd', 'wg', 'wgd', 'rg', 'sv1', 'si1', 'su1', 'bcs', 'bpc', 'cfr', 'css'
    ]
    threeD_sort = [ # 3D PAWS
        't1', 't2', 't3', 'ht1', 'ht2', 'bt1', 'mt1', 'st1', 'htu21d_temp', 'mcp9808', 'bmp_temp', 'bme_temp', 
        'bp1', 'sp1', 'msl1', 'bmp_slp', 'bme_slp', 'bmp_pressure', 'bme_pressure', 
        'bmp_altitude',
        'ws', 'wind_speed', 'wd', 'wind_direction', 'wg', 'wgd',
        'hh1', 'hh2', 'rh1', 'bh1', 'sh1', 'htu21d_humidity', 'bme_humidity',
        'rain', 'rg', 'rgt', 'rgp', 'rgds',
        'h1', 'wlo', 'wld', 'wlm', 'wlr', # water level sensors
        'sg', # snow depth sensor
        'st1', 'st2', 'st3', 'sm1', 'sm2', 'sm3', # soil temp and soil moisture sensors
        'sv1', 'si1', 'su1', 'vis1', 'ir1', 'uv1', 'si1145_vis', 'si1145_ir', 'si1145_uv', 'si1145_vis1', 'si1145_ir1', 'si1145_uv1',
            'si1145_vis2', 'si1145_ir2', 'si1145_uv2', 'si1145_vis3', 'si1145_ir3', 'si1145_uv3', 'si1145_vis4', 'si1145_ir4', 'si1145_uv4',
            'solar1', 'solar2', 'lx', # irradiance sensors
        'bcs', 'bpc', 'cfr', 'bv', 'css', 'hth' # battery health

    ]
    threeD_cal_sort = [ # 3D Calibration
        'htu21d_temp', 'bmp_temp', 'mcp9808', 'sht31d_temp', 'sht31d_humidity', 'htu21d_humidity', 'bmp_slp', 'bmp_pressure', 'rain', 'wind_speed', 
        'wind_direction', 'wg', 'wgd', 'si1145_vis', 'si1145_ir', 'si1145_uv', 'bpc'
    ]
    f_sort = [ # FEWSNET
        'rg1', 'rg2', 'rgt1', 'rgt2', 'rgp1', 'rgp2',
        'hi',  'wbt', 'wbgt',
        'bt1', 'bt2', 'ht1', 'ht2', 'st1', 'mt1',
        'bh1', 'bh2', 'hh1', 'hh2', 'sh1',
        'bp1', 'bp2',
        'hth', 'bpc', 'bcs', 'css', 'cfr'
    ]
    d_sort = [ # Dominican Republic
        'ht1', 'bt1', 'mt1', 'hh1', 'bmp_slp', 'bp1', 'rg', 'ws', 'wd', 'wg', 'wgd', 'sv1', 'si1', 'su1', 'hth', 'bpc'
    ]

    if portal_name == "Barbados":
        column_map = {col: i for i, col in enumerate(b_sort)}
    elif portal_name == "Trinidad":
        column_map = {col: i for i, col in enumerate(t_sort)}
    elif portal_name == "3D PAWS":
        column_map = {col: i for i, col in enumerate(threeD_sort)}
    elif portal_name == "3D Calibration":
        column_map = {col: i for i, col in enumerate(threeD_cal_sort)}
    elif portal_name == "FEWSNET":
        column_map = {col: i for i, col in enumerate(f_sort)}
    elif portal_name == "Dominican Republic":
        column_map = {col: i for i, col in enumerate(d_sort)}
    else:
        print("Could not sort columns.")
        sys.exit(1) 

    sorted_columns = sorted(columns, key=lambda col: column_map.get(col, float('inf'))) # columns not found in sort appended at end
    return list(sorted_columns) 

"""
Takes a list of dictionaries and returns a list of the set of all variables to be used as columns in the csv.
The exception is when include_test is set to True -- it will append a column 'test' next to each variable indicating 
whether the data is test data. If a wind direction column is in the set, an adjacent compass_dir column will be appended
containing the corresponding compass rose reading for each field.
"""
def get_columns(dictionary_list:list, include_test:bool, portal_name:str) -> list:
    if not isinstance(dictionary_list, list):
        raise TypeError(f"The 'dictionary_list' parameter in get_columns() should be of type <list>, passed: {type(dictionary_list)}")
    if not isinstance(include_test, bool):
        raise TypeError(f"The 'include_test' parameter in get_columns() should be of type <bool>, passed: {type(include_test)}")
    if not isinstance(portal_name, str):
        raise TypeError(f"The 'portal_name' parameter in get_columns() should be of type <str>, passed: {type(portal_name)}")

    columns = [] # list of strings 
    for dictionary in dictionary_list:
        cols = list(dictionary.keys())
        cols_sorted = sort_columns(cols, portal_name)
        for col in cols_sorted:
            if not col in columns:
                columns.append(str(col))
                if include_test:
                    columns.append('test')

    return columns

"""
Creates a list of all the header names to be used in the csv requested by user and returns it.
"""
def build_headers(measurements:list, columns_desired:list, include_test:bool, portal_name:str) -> list:
    if not isinstance(measurements, list):
        raise TypeError(f"The 'measurements' parameter of build_headers() should be of type <list>, passed: {type(measurements)}")
    if not isinstance(columns_desired, list):
        raise TypeError(f"The 'columns_desired' parameter of build_headers() should be of type <list>, passed: {type(columns_desired)}")
    if not isinstance(include_test, bool):
        raise TypeError(f"The 'include_test' parameter of build_headers() should be of type <bool>, passed: {type(include_test)}")
    if not isinstance(portal_name, str):
        raise TypeError(f"The 'portal_name' parameter of build_headers() should be of type <str>, passed: {type(portal_name)}")

    headers = ['time'] # list of strings
    columns = get_columns(measurements, include_test, portal_name)
    
    if len(columns) == 0: # if no data, pass to main 
        return []
    if not headers_are_valid(columns_desired, columns, portal_name): # check if user typed in recognized shortnames
        sys.exit(1)
    
    for i in range(len(columns)):
        if len(columns_desired) == 0: # no user-specified columns
            headers.append(columns[i])
        elif len(columns_desired) != 0 and columns[i] in columns_desired:
            headers.append(columns[i])
            if include_test:
                headers.append(columns[i+1])
            if is_wind_dir(columns[i]):
                headers.append(f'{columns[i]}_compass_dir')
        elif len(columns_desired) != 0 and columns[i] not in columns_desired:
            continue  
        else:
            print("Error locating column header names.")
            sys.exit(1)

    return headers

"""
Accepts an array of headers, timestamps, and of dictionaries containing sensor measurements. 
Also accepts a np array of whether or not  the measurements at that timestamp are test values. 
Accepts a string of the filepath at which to create the csv file and creates the csv there.
"""
def csv_builder(headers:list, time:np.ndarray, measurements:np.ndarray, test:np.ndarray, filepath:str, include_test:bool, null_value): 
    if not isinstance(headers, list):
        raise TypeError(f"The 'headers' parameter in csv_builder() should be of type <list>, passed: {type(headers)}")
    if not isinstance(time, np.ndarray):
        raise TypeError(f"The 'time' parameter in csv_builder() should be of type <ndarray>, passed: {type(time)}")
    if not isinstance(measurements, np.ndarray):
        raise TypeError(f"The 'measurements' parameter in csv_builder() should be of type <ndarray>, passed: {type(measurements)}")
    if not isinstance(test, np.ndarray):
        raise TypeError(f"The 'test' parameter in csv_builder() should be of type <ndarray>, passed: {type(test)}")
    if not isinstance(filepath, str):
        raise TypeError(f"The 'filepath' parameter in csv_builder() should be of type <str>, passed: {type(filepath)}")
    if not isinstance(include_test, bool):
        raise TypeError(f"The 'include_test' parameter in csv_builder() should be of type <bool>, passed: {type(include_test)}")

    if len(time) == len(measurements):
        data = [] # list of timestamps, measurements, test val's, and headers (to turn into dataframe)
        for i in range(len(time)):
            measurements[i].update({'time':time[i]}) 
            if include_test and len(test) == len(time):
                measurements[i].update({'test':test[i]})
            measurement_dict = {header: measurements[i].get(header, null_value) for header in headers} # fill in null value for var's with no data
            data.append(measurement_dict)
        
        df = pd.DataFrame(data, columns=headers)
        df.to_csv(filepath, index=False)
    else:
        raise TimestampError()
    

"""
Accepts a dictionary containing the result of the API data download from CHORDS and checks whether it was successful.
Returns True if excess datapoints, False otherwise.
"""
def has_excess_datapoints(dictionary:dict) -> bool:
    if not isinstance(dictionary, dict):
        raise TypeError(f"The 'dictionary' parameter in stream_has_data() should be of type <dict>, passed: {type(dictionary)}")

    for key in dictionary: 
        if key == "errors":
            return True

    return False

"""
Accepts np arrays for measurements, time, and test which were created from the data stream from CHORDS, and checks whether 
the transfer of data from the stream to the data structs was a success.
"""
def struct_has_data(measurements:np.ndarray, time:np.ndarray, test:np.ndarray) -> bool:
    if not isinstance(measurements, np.ndarray):
        raise TypeError(f"The 'measurements' parameter in struct_has_data() should be of type <np.ndarray>, passed: {type(measurements)}")
    if not isinstance(time, np.ndarray):
        raise TypeError(f"The 'time' parameter in struct_has_data() should be of type <np.ndarray>, passed: {type(time)}")
    if not isinstance(test, np.ndarray):
        raise TypeError(f"The 'test' parameter in struct_has_data() should be of type <np.ndarray>, passed: {type(test)}")
    
    flag = True
    if len(measurements) == 0:
        #print("\t\t No measurements found.")
        flag = False
    if len(time) == 0:
        print("\t\t No timestamps found.\n")
        flag = False
    if len(test) == 0:
        #print("\t\t No test values found.")
        flag = False

    return flag


"""
Accepts the resulting data stream of API request, and checks if any of the keys are a known error. Prints are more useful error
message to the screen for troubleshooting. Returns True if error is found, False otherwise.
** when the API returns 'errors' key, the information is stored in a list  **
** when the API returns 'error' key, the information is stored in a string **
"""
def has_errors(all_fields:dict) -> bool:
    if not isinstance(all_fields, dict):
        raise TypeError(f"The 'all_fields' parameter in has_errors() should be of type <dict>, passed: {type(all_fields)}")

    for key in all_fields:
        if key == 'errors' and all_fields['errors'][0] == 'Access Denied, user authentication required.': 
            print(all_fields[key][0])
            print("Check url, email address, and api key.")
            return True
        if key == 'error' and all_fields['error'] == 'Internal Server Error':
            print(all_fields[key])
            print("Check to make sure the instrument ID's are valid. Refer to the CHORDS Portal.")
            return True
                
    return False

"""
Handles specific time window requested by user. Stores only data that falls into the time window specified and returns data from API pull as a list of lists.

TO DO: Increase efficiency. Currently steps through day-by-day, will take forever for large amounts of data.
"""
def time_window(iD:int, timestamp_start:datetime, timestamp_end:datetime, timestamp_window_start:dt_time, \
                                timestamp_window_end:dt_time, portal_url:str, user_email:str, api_key:str, null_value) -> list:
    if not isinstance(iD, int):
        raise TypeError(f"The 'iD' parameter in time_window() should be of type <int>, passed: {type(iD)}")
    if not isinstance(timestamp_start, datetime):
        raise TypeError(f"The 'timestamp_start' parameter in time_window() should be of type <datetime>, passed: {type(timestamp_start)}")
    if not isinstance(timestamp_end, datetime):
        raise TypeError(f"The 'timestamp_end' parameter in time_window() should be of type <datetime>, passed: {type(timestamp_end)}")
    if not isinstance(timestamp_window_start, dt_time):
        raise TypeError(f"The 'timestamp_window_start' parameter in time_window() should be of type <datetime.time>, passed: {type(timestamp_window_start)}")
    if not isinstance(timestamp_window_end, dt_time):
        raise TypeError(f"The 'timestamp_window_end' parameter in time_window() should be of type <datetime.time>, passed: {type(timestamp_window_end)}")
    if not isinstance(portal_url, str):
        raise TypeError(f"The 'portal_url' parameter in time_window() should be of type <str>, passed: {type(portal_url)}")
    if not isinstance(user_email, str):
        raise TypeError(f"The 'user_email' parameter in time_window() should be of type <str>, passed: {type(user_email)}")
    if not isinstance(api_key, str):
        raise TypeError(f"The 'api_key' parameter in time_window() should be of type <str>, passed: {type(api_key)}")

    time = []
    measurements = []
    test = []
    total_num_measurements = 0

    date_start = timestamp_start.date()
    if datetime.combine(date_start, timestamp_window_start) >= timestamp_start: 
        time_window_begin = datetime.combine(date_start, timestamp_window_start)
        time_window_stop = datetime.combine(date_start, timestamp_window_end)

        url = f"{portal_url}/api/v1/data/{iD}?start={time_window_begin}&end={time_window_stop}&email={user_email}&api_key={api_key}"
        response = requests.get(url=url)
        all_fields = loads(dumps(response.json()))

        if has_errors(all_fields):
                sys.exit(1)

        data = all_fields['features'][0]['properties']['data']
        for dictionary in data:
                time.append(str(dictionary['time']))
                total_num_measurements += len(dictionary['measurements'].keys())
                to_append = write_compass_direction(dict(dictionary['measurements']), null_value)
                measurements.append(to_append)
                test.append(str(dictionary['test']))

    i = 0 
    next_date = date_start + timedelta(days=1)
    while datetime.combine(next_date, timestamp_window_end) < timestamp_end:
        time_window_begin = datetime.combine(next_date, timestamp_window_start)
        time_window_stop = datetime.combine(next_date, timestamp_window_end)

        url = f"{portal_url}/api/v1/data/{iD}?start={time_window_begin}&end={time_window_stop}&email={user_email}&api_key={api_key}"
        response = requests.get(url=url)
        all_fields = loads(dumps(response.json()))

        if has_errors(all_fields):
            sys.exit(1)

        data = all_fields['features'][0]['properties']['data']
        for dictionary in data:
                time.append(str(dictionary['time']))
                total_num_measurements += len(dictionary['measurements'].keys())
                to_append = write_compass_direction(dict(dictionary['measurements']), null_value)
                measurements.append(to_append)
                test.append(str(dictionary['test']))

        next_date = next_date + timedelta(days=1)

        i += 1
        if i == 100:
            print("\t\t Large data request.")
            print("\t\t\t Getting next data segment...")
        elif i%100 == 0:
            print("\t\t\t Getting next data segment...")

    return [time, measurements, test, total_num_measurements]


"""
Helper function for reduce_datapoints() that breaks up large time frames into smaller benchmark timestamps.
Returns a list of all the timestamps to use in order to shrink data points returned by API.
"""
def get_timestamps(start_time:datetime, end_time:datetime, divisions:int) -> list:
    if not isinstance(start_time, datetime):
        raise TypeError(f"The 'start_time' parameter in get_timestamps() should be of type <datetime>, passed: {type(start_time)}")
    if not isinstance(end_time, datetime):
        raise TypeError(f"The 'end_time' parameter in get_timestamps() should be of type <datetime>, passed: {type(end_time)}")
    if not isinstance(divisions, int):
        raise TypeError(f"The 'divisions' parameter in get_timestamps() should be of type <int>, passed: {type(divisions)}")
    
    time_delta = math.floor(( (end_time - start_time).total_seconds() / 60 ) / divisions) # in minutes rounded down
    begin = start_time
    new_timestamps = []
    new_timestamps.append(start_time)

    i = 1
    while i <= divisions:
        new_timestamp = begin + timedelta(minutes=time_delta)
        new_timestamps.append(new_timestamp)
        begin = new_timestamp
        i += 1
    
    if begin != end_time:
        time_till_end = math.ceil(( (end_time- new_timestamps[len(new_timestamps)-1]).total_seconds() / 60 ) / divisions) # in minutes rounded up
        if time_till_end < time_delta:
            new_timestamps.append(end_time)
        else:
            print("Timestamp reduction error -- Check API request for incorrect input.")
            sys.exit(1)

    return new_timestamps

"""
Accepts a timestamp string from the CHORDS API and parses out the timestamp. Returns a datetime object of the parameter.
    e.g.  '2023-12-17T18:45:56Z'
"""
def get_time(timestamp:str) -> datetime:
    if not isinstance(timestamp, str):
        raise TypeError(f"The 'timestamp' parameter in get_time() should be of type <str>, passed: {type(timestamp)}")
    
    format_str = "%H:%M:%S" 
    return datetime.strptime(timestamp[11:19], format_str)

"""
Handles data request error where number of data points exceeds that allowed. Returns a list of new timestamps for which 
to run the API request to CHORDS s.t. the number of data points requested is less than the max allowed. Returns the 
lists of data necessary for main() to build csv's.
"""
def reduce_datapoints(error_message:str, iD:int, timestamp_start:datetime, timestamp_end:datetime, \
                                                        portal_url:str, user_email:str, api_key:str, null_value) -> list:
    if not isinstance(error_message, str):
        raise TypeError(f"The 'error_message' parameter in reduce_datapoints() should be of type <str>, passed: {type(error_message)}")
    if not isinstance(iD, int):
        raise TypeError(f"The 'iD' parameter in reduce_datapoints() should be of type <int>, passed: {type(iD)}")
    if not isinstance(timestamp_start, datetime):
        raise TypeError(f"The 'timestamp_start' parameter in reduce_datapoints() should be of type <datetime>, passed: {type(timestamp_start)}")
    if not isinstance(timestamp_end, datetime):
        raise TypeError(f"The 'timestamp_end' parameter in reduce_datapoints() should be of type <datetime>, passed: {type(timestamp_end)}")
    if not isinstance(portal_url, str):
        raise TypeError(f"The 'portal_url' parameter in reduce_datapoints() should be of type <str>, passed: {type(portal_url)}")
    if not isinstance(user_email, str):
        raise TypeError(f"The 'user_email' parameter in reduce_datapoints() should be of type <str>, passed: {type(user_email)}")
    if not isinstance(api_key, str):
        raise TypeError(f"The 'api_key' parameter in reduce_datapoints() should be of type <str>, passed: {type(api_key)}")


    print("\t Beginning reduction calculation.")

    time = [] # to avoid a duplicate API cycle in main() -- save time
    measurements = []
    test = []
    total_num_measurements = 0
    
    num_divisions = 2
    new_timestamps = get_timestamps(timestamp_start, timestamp_end, num_divisions)
    t = new_timestamps[0] # to store progress
    
    keep_going = True
    excess_flag = False 
    while keep_going:
        for i in range(len(new_timestamps)-1):
            if new_timestamps[i] < t and t != new_timestamps[0]: # skip timestamps already shown to pass 
                continue

            print("\t\t Getting next data segment...")
            url = f"{portal_url}/api/v1/data/{iD}?start={new_timestamps[i]}&end={new_timestamps[i+1]}&email={user_email}&api_key={api_key}"
            response = requests.get(url=url)
            all_fields = loads(dumps(response.json()))

            if has_excess_datapoints(all_fields):
                t = new_timestamps[i]
                num_divisions *= 2
                new_timestamps = get_timestamps(timestamp_start, timestamp_end, num_divisions)
                excess_flag = True
                break
            
            excess_flag = False

            data = all_fields['features'][0]['properties']['data']
            for dictionary in data:
                time.append(str(dictionary['time']))
                total_num_measurements += len(dictionary['measurements'].keys())
                to_append = write_compass_direction(dict(dictionary['measurements']), null_value)
                measurements.append(to_append)
                test.append(str(dictionary['test']))
            
        if not excess_flag:
            keep_going = False    
        
    print("\t Finished reduction calculation.")
    return [time, measurements, test, total_num_measurements]


"""
Accepts the CHORDS portal name specified by user and creates the correct README associated with that portal.
"""
def create_README(portal_name:str, data_path:str):
    if not isinstance(portal_name, str):
        raise TypeError(f"The 'portal_name' parameter in create_README() should be of type <str>, passed: {type(portal_name)}")
    if not isinstance(data_path, str):
        raise TypeError(f"The 'data_path' parameter in create_README() should be of type <str>, passed: {type(data_path)}")
    
    portal_lookup = [
        'Barbados', 'Trinidad', '3D PAWS', '3D Calibration', 'FEWSNET', 'Kenya', 'Cayman Islands', 'Dominican Republic'
    ]

    if portal_name in portal_lookup:
        portal_index = portal_lookup.index(portal_name)

        if portal_index == 0: # Barbados
            with open(data_path + 'README.txt', 'w') as file: 
                file.write("\t==================================================================================\n")
                file.write("\t============================= Units of measurement guide =========================\n")
                file.write("\t==================================================================================\n\n")
                file.write("\tSensor name\t(shortname)\t\tMeasured Property\t\t(units)\n")
                file.write("\t__________________________________________________________________________________\n\n")

                file.write("\tHTU21D_T\t(t1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tBMP280_T\t(t2)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tMCP9808_T\t(t3)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tHTU21D_RH\t(rh1)\t\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tBMP280_SLP\t(msl1)\t\t\tAir Pressure Value\t(millibar mbar)\n")
                file.write("\tBMP280_SP\t(sp1)\t\t\tAir Pressure Value\t(millibar mbar)\n")
                file.write("\twindspeed\t(ws)\t\t\tWind Speed\t\t(meters per second m/s)\n")
                file.write("\twinddirection\t(wd)\t\t\tWind Direction\t\t(degrees N degN)\n")
                file.write("\tprecipitation\t(rain)\t\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tSI1145_VIS\t(vis1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_IR\t(ir1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_UV\t(uv1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
        
        if portal_index == 1: # Trinidad
            with open(data_path + 'README.txt', 'w') as file:
                file.write("\t==============================================================================================\n")
                file.write("\t=================================== Units of measurement guide ===============================\n")
                file.write("\t==============================================================================================\n\n")
                file.write("\tSensor name\t(shortname)\t\tMeasured Property\t\t\t(units)\n")
                file.write("\t______________________________________________________________________________________________\n\n")

                file.write("\tBMX280 Temperature\t\t(bt1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tMCP9808 Temperature\t\t(mt1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tHTU21D Temperature\t\t(ht1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tBMX280 Pressure\t\t\t(bp1)\t\t\tAir Pressure Value\t(hectopascal hPa)\n")
                file.write("\tBMX280 Relative Humidity\t(bh1)\t\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tHTU21D Relative Humidity\t(hh1)\t\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tWind Speed\t\t\t(ws)\t\t\tWind Speed\t\t(meters per second m/s)\n")
                file.write("\tWind Direction\t\t\t(wd)\t\t\tWind Direction\t\t(degrees N degN)\n")
                file.write("\tWind Gust\t\t\t(wg)\t\t\tWind Speed\t\t(meters per second m/s)\n")
                file.write("\tWind Gust Direction\t\t(wgd)\t\t\tWind Direction\t\t(degrees N degN)\n")
                file.write("\tRain Gauge\t\t\t(rg)\t\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tSI1145 Visible\t\t\t(sv1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145 Infrared\t\t\t(si1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145 Ultraviolet\t\t(su1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tBattery Current State\t\t(bcs)\t\t\tState of Health\t\t(count #)\n")
                file.write("\tBattery Percent Charge\t\t(bpc)\t\t\tState of Health\t\t(percent %)\n")
                file.write("\tBattery Charge Fault Register\t(cfr)\t\t\tState of Health\t\t(count #)\n")
                file.write("\tCell Signal Strength\t\t(css)\t\t\tState of Health\t\t(percent %)\n")

        if portal_index == 2: # 3D PAWS
            with open(data_path + 'README.txt', 'w') as file: 
                file.write("\t==========================================================================================\n")
                file.write("\t================================= Units of measurement guide =============================\n")
                file.write("\t==========================================================================================\n\n")
                file.write("\tSensor name\t\t\t(shortname)\t\tMeasured Property\t(units)\n")
                file.write("\t__________________________________________________________________________________________\n\n")

                file.write("\tHTU21D_T\t\t\t(t1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tHTU21D Temperature\t\t(ht1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tHIH Temperature 2\t\t(ht2)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tHTU21D_T\t\t\t(htu21d_temp)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tBMP280_T\t\t\t(t2)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tBMP280_T\t\t\t(bmp_temp)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tBME_T\t\t\t\t(bme_temp)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tBMX280 Temperature 1\t\t(bt1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tMCP9808_T\t\t\t(t3)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tMCP9808_T\t\t\t(mcp9808)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tMCP9808 Temperature 1\t\t(mt1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tSHT Temperature\t\t\t(st1)\t\t\tTemperature\t\t(degrees C)\n\n")

                file.write("\tBMP280_ALT\t\t\t(bmp_altitude)\t\tAltitude\t\t(meter m)\n\n")

                file.write("\tBMP280_SLP\t\t\t(msl1)\t\t\tAir Pressure Value\t(millibar mbar)\n")
                file.write("\tBMP280_SLP\t\t\t(bmp_slp)\t\tAir Pressure Value\t(hectopascal hPa)\n")
                file.write("\tBMP280_SP\t\t\t(sp1)\t\t\tAir Pressure Value\t(millibar mbar)\n")
                file.write("\tBMP280_SP\t\t\t(bmp_pressure)\t\tAir Pressure Value\t(hectopascal hPa)\n")
                file.write("\tBME_StationPres\t\t\t(bme_pressure)\t\tAir Pressure Value\t(hectopascal hPa)\n")
                file.write("\tBME_SeaLevelPres\t\t(bme_slp)\t\tAir Pressure Value\t(hectopascal hPa)\n")
                file.write("\tBMX280 Pressure 1\t\t(bp1)\t\t\tAir Pressure Value\t(hectopascal hPa)\n\n")

                file.write("\tWind speed\t\t\t(ws)\t\t\tWind Speed\t\t(meters per second m/s)\n")
                file.write("\twindspeed\t\t\t(wind_speed)\t\tWind Speed\t\t(meters per second m/s)\n")
                file.write("\tWind direction\t\t\t(wd)\t\t\tWind Direction\t\t(degrees N degN)\n")
                file.write("\twinddirection\t\t\t(wind_direction)\tWind Direction\t\t(degrees N degN)\n")
                file.write("\tWind Gust\t\t\t(wg)\t\t\tWind Speed\t\t(meters per second m/s)\n")
                file.write("\tWind Gust Direction\t\t(wgd)\t\t\tWind Direction\t\t(degrees N degN)\n\n")

                file.write("\tHTU21D_RH\t\t\t(rh1)\t\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tHTU21D_RH\t\t\t(htu21d_humidity)\tHumidity Value\t\t(percent %)\n")
                file.write("\tBMX280 Relative Humidity\t(bh1)\t\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tBME_RH\t\t\t\t(bme_humidity)\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tHTU21D Relative Humidity\t(hh1)\t\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tHIH Humidity 2\t\t\t(hh2)\t\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tRelative Humidity\t\t(sh1)\t\t\tHumidity Value\t\t(percent %)\n\n")

                file.write("\tRain\t\t\t\t(rain)\t\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tRain Gauge Total Today\t\t(rgt)\t\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tStation Rain Gauge\t\t(rg)\t\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tRain Gauge Total Prior\t\t(rgp)\t\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tRain_Gauge_Delta_Seconds\t(rgds)\t\t\tTime Measurement Accuracy (seconds s)\n\n")

                file.write("\tWater level\t\t\t(hl)\t\t\tDatum\t\t\t(millimeter mm)\n")
                file.write("\tWater level outliers\t\t(wlo)\t\t\tNumber of Samples\t(count #)\n")
                file.write("\tWater level deviation\t\t(wld)\t\t\tDeviation\t\t(millimeter mm)\n")
                file.write("\tWater level median\t\t(wlm)\t\t\tDatum\t\t\t(millimeter #)\n")
                file.write("\tWater level raw\t\t\t(wlr)\t\t\tDatum\t\t\t(millimeter mm)\n\n")

                file.write("\tSnow_Depth\t\t\t(sg)\t\t\tDatum\t\t\t(centimeter cm)\n\n")

                file.write("\tSoil Temperature 1\t\t(st1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tSoil Temperature 2\t\t(st2)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tSoil Temperature 3\t\t(st3)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tSoil Moisture 1\t\t\t(sm1)\t\t\tData Point\t\t(percent saturation % Sat)\n")
                file.write("\tSoil Moisture 2\t\t\t(sm2)\t\t\tData Point\t\t(kilopascal kPa)\n")
                file.write("\tSoil Moisture 3\t\t\t(sm3)\t\t\tData Point\t\t(percent saturation % Sat)\n\n")

                file.write("\tSI1145 Visible 1\t\t(sv1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145 Infrared 1\t\t(si1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145 Ultraviolet 1\t\t(su1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_VIS\t\t\t(vis1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_IR\t\t\t(ir1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_UV\t\t\t(uv1)\t\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_VIS\t\t\t(si1145_vis)\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_IR\t\t\t(si1145_ir)\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_UV\t\t\t(si1145_uv)\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSP_215\t\t\t\t(solar1)\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSP_212\t\t\t\t(solar2)\t\tDownwelling Irradiance\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_visible1\t\t\t(si1145_vis1)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tSI11145_infrared1\t\t(si1145_ir1)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tSI1145_UV1\t\t\t(si1145_uv1)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tSI1145_visible2\t\t\t(si1145_vis2)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tSI1145_infrared2\t\t(si1145_ir2)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tSI1145_UV2\t\t\t(si1145_uv2)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tSI1145_visible3\t\t\t(si1145_vis3)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tSI11145_infrared3\t\t(si1145_ir3)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tSI1145_UV3\t\t\t(si1145_uv3)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tSI1145_visible4\t\t\t(si1145_vis4)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tSI11145_infrared4\t\t(si1145_ir4)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tSI1145_UV4\t\t\t(si1145_uv4)\t\tDownwelling Irradiance\t(count #)\n")
                file.write("\tVEML770 light sensor\t\t(lx)\t\t\tDownwelling Irradiance\t(count #)\n\n")
                
                file.write("\tBattery Current State\t\t(bcs)\t\t\tState of Health\t\t(count #)\n")
                file.write("\tBattery Percent Charge\t\t(bpc)\t\t\tState of Health\t\t(percent %)\n")
                file.write("\tBattery Charge Fault Register\t(cfr)\t\t\tState of Health\t\t(count #)\n")
                file.write("\tBattery Voltage\t\t\t(bv)\t\t\tState of Health\t\t(volts V)\n")
                file.write("\tCell Signal Strength\t\t(css)\t\t\tState of Health\t\t(percent %)\n")
                file.write("\tHealth 16bits\t\t\t(hth)\t\t\tState of Health\t\t(count #)\n\n")

        if portal_index == 3: # 3D Calibration
            with open(data_path + 'README.txt', 'w') as file:
                file.write("\t==========================================================================================\n")
                file.write("\t================================= Units of measurement guide =============================\n")
                file.write("\t==========================================================================================\n\n")
                file.write("\tSensor name\t\t\t(shortname)\t\tMeasured Property\t(units)\n")
                file.write("\t__________________________________________________________________________________________\n\n")

                file.write("\tHTU21D_T\t\t\t(htu21d_temp)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tBMP280_T\t\t\t(bmp_temp)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tMCP9808_T\t\t\t(mcp9808)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tHTU21D_T\t\t\t(sht31d_temp)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tHTU21D_RH\t\t\t(sht31d_humidity)\tHumidity Value\t\t(percent %)\n")
                file.write("\tHTU21D_RH\t\t\t(htu21d_humidity)\tHumidity Value\t\t(percent %)\n")
                file.write("\tBMP280_SLP\t\t\t(bmp_slp)\t\tAir Pressure Value\t(millibar mbar)\n")
                file.write("\tBMP280_SP\t\t\t(bmp_pressure)\t\tAir Pressure Value\t(millibar mbar)\n")
                file.write("\train\t\t\t\t(rain)\t\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\twindspeed\t\t\t(wind_speed)\t\tWind Speed\t\t(meters per second m/s)\n")
                file.write("\twinddirection\t\t\t(wind_direction)\tWind Direction\t\t(degrees N degN)\n")
                file.write("\tWind Gust\t\t\t(wg)\t\t\tWind Speed\t\t(meters per second m/s)\n")
                file.write("\tWind Gust Direction\t\t(wgd)\t\t\tWind Direction\t\t(degrees N degN)\n")
                file.write("\tSI1145_VIS\t\t\t(si1145_vis)\t\tRadiance\t\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_IR\t\t\t(si1145_ir)\t\tRadiance\t\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_UV\t\t\t(si1145_uv)\t\tRadiance\t\t(watts per square meter W/m^2)\n")
                file.write("\tBattery Percent Charge\t\t(bpc)\t\t\tState of Health\t\t(percent %)\n")

        if portal_index == 4: # FEWSNET
            with open(data_path + 'README.txt', 'w') as file:
                file.write("\t==========================================================================================\n")
                file.write("\t================================= Units of measurement guide =============================\n")
                file.write("\t==========================================================================================\n\n")
                file.write("\tSensor name\t\t\t(shortname)\t\tMeasured Property\t(units)\n")
                file.write("\t__________________________________________________________________________________________\n\n")

                file.write("\tBMX Temperature 1\t\t\t(bt1)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tBMX Temperature 2\t\t\t(bt2)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tHTU Temperature 1\t\t\t(ht1)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tHIH Temperature\t\t\t\t(ht2)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tSHT Temperature\t\t\t\t(st1)\t\tTemperature\t\t(degrees C)\n")
                file.write("\tMCP Temperature 1\t\t\t(mt1)\t\tTemperature\t\t(degrees C)\n")

                file.write("\tBMX Humidity 1\t\t\t\t(bh1)\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tBMX Humidity 2\t\t\t\t(bh2)\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tHTU Humidity 1\t\t\t\t(hh1)\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tHIH Humidity\t\t\t\t(hh2)\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tSHT Humidity\t\t\t\t(sh1)\t\tHumidity Value\t\t(percent %)\n")

                file.write("\tBMX Pressure 1\t\t\t\tt(bp1)\tAir Pressure Value\t(hectopascal hPa)\n")
                file.write("\tBMX Pressure 2\t\t\t\t\t(bp2)\tAir Pressure Value\t(hectopascal hPa)\n")

                file.write("\tRain Gauge 1\t\t\t\t(rg1)\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tRain Gauge 2\t\t\t\t(rg2)\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tRain Gauge 1 Total Today\t(rgt1)\t\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tRain Gauge 2 Total Today\t(rgt2)\t\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tRain Gauge 1 Total Prior\t(rgp1)\t\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tRain Gauge 2 Total Prior\t(rgp2)\t\t\tPrecipitation\t\t(millimeter mm)\n")

                file.write("\tHealth\t\t\t\t(hth)\t\tState of Health\t\t(count #)\n\n")
                file.write("\tBattery Percent Charge\t\t\t(bpc)\t\tState of Health\t\t(percent %)\n")
                file.write("\tBattery Current State\t\t\t(bcs)\t\tState of Health\t\t(count #)\n")
                file.write("\tCell Signal Strength\t\t\t(css)\t\tState of Health\t\t(percent %)\n")
                file.write("\tBattery Charge Fault Register\t\t(cfr)\t\tState of Health\t\t(count #)\n")

        # INSERT KENYA HERE
        # INSERT CAYMAN ISLANDS HERE        

        if portal_index == 7: # Dominican Republic
            with open(data_path + 'README.txt', 'w') as file:
                file.write("\t==========================================================================================\n")
                file.write("\t================================= Units of measurement guide =============================\n")
                file.write("\t==========================================================================================\n\n")
                file.write("\tSensor name\t\t\t(shortname)\t\tMeasured Property\t(units)\n")
                file.write("\t__________________________________________________________________________________________\n\n")

                file.write("\tBMP280_T\t\t\t(bt1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tHTU21D_T\t\t\t(ht1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tMCP9808\t\t\t\t(mt1)\t\t\tTemperature\t\t(degrees C)\n")
                file.write("\tHTU21D_RH\t\t\t(hh1)\t\t\tHumidity Value\t\t(percent %)\n")
                file.write("\tBMP280_SLP\t\t\t(bmp_pressure)\t\tAir Pressure Value\t(hectopascal hPa)\n")
                file.write("\tBMP280_SP\t\t\t(bp1)\t\t\tAir Pressure Value\t(hectopascal hPa)\n")
                file.write("\tPrecipitation\t\t\t(rg)\t\t\tPrecipitation\t\t(millimeter mm)\n")
                file.write("\tWind Speed\t\t\t(ws)\t\t\tWind Speed\t\t(meters per second m/s)\n")
                file.write("\tWind Direction\t\t\t(wd)\t\t\tWind Direction\t\t(degrees N degN)\n")
                file.write("\tWind Gust\t\t\t(wg)\t\t\tWind Speed\t\t(meters per second m/s)\n")
                file.write("\tWind Gust Direction\t\t(wgd)\t\t\tWind Direction\t\t(degrees N degN)\n\n")
                file.write("\tSI1145_VIS\t\t\t(sv1)\t\t\tRadiance\t\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_IR\t\t\t(si1)\t\t\tRadiance\t\t(watts per square meter W/m^2)\n")
                file.write("\tSI1145_UV\t\t\t(su1)\t\t\tRadiance\t\t(watts per square meter W/m^2)\n")
                file.write("\tHealth\t\t\t\t(hth)\t\t\tState of Health\t\t(dimensionless)\n\n")
                file.write("\tBattery Percent Charge\t\t(bpc)\t\t\tState of Health\t\t(percent %)\n")

    else:
        raise ValueError(f"create_README() expects a portal name to be one of the following (case sensitive):\n\t \
                            Barbados, Trinidad, 3D PAWS, 3D Calibration, Kenya, Cayman Islands, or Dominican Republic.")

    return 0
