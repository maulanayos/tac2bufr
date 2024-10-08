import csv
from io import BytesIO, StringIO
import eccodes as ecc
from datetime import datetime, timezone


def handle_missing_values(values, missing_value):
    """Convert 'MISSING' strings to a specified missing value."""
    return [missing_value if v == 'MISSING' else v for v in values]

def create_temp_bufr_ac(parsed_data, station_metadata):
    csv_file = StringIO(station_metadata)
    csv_reader = csv.DictReader(csv_file)
    bufr = ecc.codes_bufr_new_from_samples("BUFR4")

    # Set BUFR edition
    ecc.codes_set(bufr, 'edition', 4)
    
    # Set identification section (Section 1)
    ecc.codes_set(bufr, 'masterTableNumber', 0)
    ecc.codes_set(bufr, 'bufrHeaderCentre', 195)  # https://training.wis2box.wis.wmo.int/practical-sessions/bufr-command-line-tools/ 
    ecc.codes_set(bufr, 'bufrHeaderSubCentre', 0)
    ecc.codes_set(bufr, 'updateSequenceNumber', 0)
    ecc.codes_set(bufr, 'dataCategory', 2)  # Upper-air category
    ecc.codes_set(bufr, 'internationalDataSubCategory', 4)  
    ecc.codes_set(bufr, 'masterTablesVersionNumber', 40)
    ecc.codes_set(bufr, 'localTablesVersionNumber', 0)
    
    # Handle the typical date (this could be parameterized)
    
    # Set number of subsets
    ecc.codes_set(bufr, "numberOfSubsets", len(parsed_data))
    
    # Load the data from parsed_data
    replicator = []
    latitudes, longitudes, heightAboveMeanSeaLevel, pressures, gpt_heights = [], [], [], [], []
    air_temperatures, dewpoint_temperatures = [], []
    wind_directions, wind_speeds = [], []
    years, months, days, hours, minutes, station_numbers, block_numbers = ([] for _ in range(7))


    for subset in parsed_data:
        replicator.append(len(subset['windDirection']))
        date = datetime.now(timezone.utc)  # noqa: F821)
        date= date.replace(day=subset['day'], hour=int(subset['hour']),minute=int(subset['minute']))
        years.append(date.year)
        months.append(date.month)
        days.append(date.day)
        hours.append(date.hour)
        minutes.append(date.minute)
        station_numbers.append(int(subset['stationNumber']))
        block_numbers.append(int(subset['blockNumber']))
        station_id = subset['blockNumber']+subset['stationNumber']
        lat,lon,ele = ("MISSING","MISSING","MISSING")
        for row in csv_reader:
            if row["traditional_station_identifier"] == station_id:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                ele = float(row["elevation"])
                break
        latitudes.append(lat)
        longitudes.append(lon)
        heightAboveMeanSeaLevel.append(ele)
        pressures.extend(handle_missing_values(subset['pressureLevels'], missing_value=ecc.CODES_MISSING_LONG))
        air_temperatures.extend(handle_missing_values(subset['airTemperature'], missing_value=ecc.CODES_MISSING_DOUBLE))
        dewpoint_temperatures.extend(handle_missing_values(subset['dewpointTemperature'], missing_value=ecc.CODES_MISSING_DOUBLE))
        gpt_heights.extend(handle_missing_values(subset['nonCoordinateGeopotentialHeight'], missing_value=ecc.CODES_MISSING_DOUBLE))
        wind_directions.extend(handle_missing_values(subset['windDirection'], missing_value=ecc.CODES_MISSING_LONG))
        wind_speeds.extend(handle_missing_values(subset['windSpeed'], missing_value=ecc.CODES_MISSING_LONG))
        heightAboveMeanSeaLevel = handle_missing_values(heightAboveMeanSeaLevel, missing_value=ecc.CODES_MISSING_DOUBLE)
    
    # Set the template to the specific TM template
    ecc.codes_set_array(bufr, 'inputExtendedDelayedDescriptorReplicationFactor', replicator)
    ecc.codes_set(bufr, 'unexpandedDescriptors', 309052)
    ecc.codes_set(bufr, 'compressedData', 0)  

    # Set the data section (Section 4)
    ecc.codes_set_array(bufr, 'blockNumber', block_numbers)
    ecc.codes_set_array(bufr, 'stationNumber', station_numbers)
    ecc.codes_set_array(bufr, 'latitude',latitudes)
    ecc.codes_set_array(bufr, 'longitude', longitudes)
    ecc.codes_set_array(bufr, 'heightOfStationGroundAboveMeanSeaLevel',heightAboveMeanSeaLevel)
    ecc.codes_set_array(bufr, 'height', heightAboveMeanSeaLevel)
    ecc.codes_set_array(bufr, 'year',years)
    ecc.codes_set_array(bufr, 'month', months)
    ecc.codes_set_array(bufr, 'day', days)
    ecc.codes_set_array(bufr, 'hour', hours)
    ecc.codes_set_array(bufr, 'minute', minutes)
    ecc.codes_set_array(bufr, 'timeSignificance', [18]*len(years))
    ecc.codes_set_array(bufr, 'pressure', pressures)
    ecc.codes_set_array(bufr, 'airTemperature', [t + 273.15 for t in air_temperatures])
    ecc.codes_set_array(bufr, 'dewpointTemperature', [t + 273.15 for t in dewpoint_temperatures])
    ecc.codes_set_array(bufr, 'windDirection', wind_directions)
    ecc.codes_set_array(bufr, 'windSpeed', wind_speeds)

    # Encode the message
    ecc.codes_set(bufr, 'pack', 1)
    
    # Write the message to a buffer
    fh = BytesIO()
    ecc.codes_write(bufr, fh)
    fh.seek(0)
    ecc.codes_release(bufr) # Release the handle to clear memory
    output = fh.read()

    return output 

def create_temp_bufr_bd(parsed_data, station_metadata):
    csv_file = StringIO(station_metadata)
    csv_reader = csv.DictReader(csv_file)
    bufr = ecc.codes_bufr_new_from_samples("BUFR4")

    # Set BUFR edition
    
    # Set identification section (Section 1)
    ecc.codes_set(bufr, 'edition', 4)
    # Set identification section (Section 1)
    ecc.codes_set(bufr, 'masterTableNumber', 0)
    ecc.codes_set(bufr, 'bufrHeaderCentre', 195)  # https://training.wis2box.wis.wmo.int/practical-sessions/bufr-command-line-tools/ 
    ecc.codes_set(bufr, 'bufrHeaderSubCentre', 0)
    ecc.codes_set(bufr, 'updateSequenceNumber', 0)
    ecc.codes_set(bufr, 'dataCategory', 2)  # Upper-air category
    ecc.codes_set(bufr, 'internationalDataSubCategory', 4)  # 
    ecc.codes_set(bufr, 'masterTablesVersionNumber', 40)
    ecc.codes_set(bufr, 'localTablesVersionNumber', 0)
    
    # Handle the typical date (this could be parameterized)
    
    # Set number of subsets
    ecc.codes_set(bufr, "numberOfSubsets", len(parsed_data))
    
    # Load the data from parsed_data
    replicator = []
    latitudes, longitudes, heightAboveMeanSeaLevel, pressures = [], [], [], []
    air_temperatures, dewpoint_temperatures = [], []
    wind_directions, wind_speeds = [], []
    years, months, days, hours, minutes, station_numbers, block_numbers = ([] for _ in range(7))


    for subset in parsed_data:
        replicator.append(len(subset['windDirection']))
        date = datetime.now(timezone.utc)  # noqa: F821)
        date= date.replace(day=subset['day'], hour=int(subset['hour']),minute=int(subset['minute']))
        years.append(date.year)
        months.append(date.month)
        days.append(date.day)
        hours.append(date.hour)
        minutes.append(date.minute)
        station_numbers.append(int(subset['stationNumber']))
        block_numbers.append(int(subset['blockNumber']))
        station_id = subset['blockNumber']+subset['stationNumber']
        lat,lon,ele = ("MISSING","MISSING","MISSING")
        for row in csv_reader:
            if row["traditional_station_identifier"] == station_id:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                ele = float(row["elevation"])
                break
        latitudes.append(lat)
        longitudes.append(lon)
        heightAboveMeanSeaLevel.append(ele)
        pressures.extend(handle_missing_values(subset['pressureLevels'], missing_value=ecc.CODES_MISSING_LONG))
        air_temperatures.extend(handle_missing_values(subset['airTemperature'], missing_value=ecc.CODES_MISSING_DOUBLE))
        dewpoint_temperatures.extend(handle_missing_values(subset['dewpointTemperature'], missing_value=ecc.CODES_MISSING_DOUBLE))
        wind_directions.extend(handle_missing_values(subset['windDirection'], missing_value=ecc.CODES_MISSING_LONG))
        wind_speeds.extend(handle_missing_values(subset['windSpeed'], missing_value=ecc.CODES_MISSING_LONG))
        heightAboveMeanSeaLevel = handle_missing_values(heightAboveMeanSeaLevel, missing_value=ecc.CODES_MISSING_DOUBLE)
    
    # Set the template to the specific TM template
    ecc.codes_set_array(bufr, 'inputExtendedDelayedDescriptorReplicationFactor', replicator)
    ecc.codes_set(bufr, 'unexpandedDescriptors', 309052)
    ecc.codes_set(bufr, 'compressedData', 0)  

    # Set the data section (Section 4)
    ecc.codes_set_array(bufr, 'blockNumber', block_numbers)
    ecc.codes_set_array(bufr, 'stationNumber', station_numbers)
    ecc.codes_set_array(bufr, 'latitude',latitudes)
    ecc.codes_set_array(bufr, 'longitude', longitudes)
    ecc.codes_set_array(bufr, 'heightOfStationGroundAboveMeanSeaLevel',heightAboveMeanSeaLevel)
    ecc.codes_set_array(bufr, 'height', heightAboveMeanSeaLevel)
    ecc.codes_set_array(bufr, 'year',years)
    ecc.codes_set_array(bufr, 'month', months)
    ecc.codes_set_array(bufr, 'day', days)
    ecc.codes_set_array(bufr, 'hour', hours)
    ecc.codes_set_array(bufr, 'minute', minutes)
    ecc.codes_set_array(bufr, 'timeSignificance', [18]*len(years))
    ecc.codes_set_array(bufr, 'pressure', pressures)
    ecc.codes_set_array(bufr, 'airTemperature', [t + 273.15 for t in air_temperatures])
    ecc.codes_set_array(bufr, 'dewpointTemperature', [t + 273.15 for t in dewpoint_temperatures])
    ecc.codes_set_array(bufr, 'windDirection', wind_directions)
    ecc.codes_set_array(bufr, 'windSpeed', wind_speeds)

    # Encode the message
    ecc.codes_set(bufr, 'pack', 1)
    
    # Write the message to a buffer
    fh = BytesIO()
    ecc.codes_write(bufr, fh)
    fh.seek(0)
    ecc.codes_release(bufr) # Release the handle to clear memory
    output = fh.read()

    return output

