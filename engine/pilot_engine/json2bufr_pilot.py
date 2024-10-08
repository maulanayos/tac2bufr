
from io import BytesIO
import csv
from io import StringIO
import eccodes as ecc
from datetime import datetime, timezone


def handle_missing_values(values, missing_value=9999):
    """Convert 'MISSING' strings and np.nan to a specified missing value."""
    return [missing_value if v == 'MISSING' else v for v in values]

def create_pilot_bufr_ac(parsed_data, station_metadata):
    bufr = ecc.codes_bufr_new_from_samples("BUFR4")
    csv_file = StringIO(station_metadata)
    csv_reader = csv.DictReader(csv_file)
    # Upper-air data for multiple levels
    # Set BUFR edition
    ecc.codes_set(bufr, 'edition', 4)
    # Set identification section (Section 1)
    ecc.codes_set(bufr, 'masterTableNumber', 0)
    ecc.codes_set(bufr, 'bufrHeaderCentre', 195)  # https://training.wis2box.wis.wmo.int/practical-sessions/bufr-command-line-tools/ 
    ecc.codes_set(bufr, 'bufrHeaderSubCentre', 0)
    ecc.codes_set(bufr, 'updateSequenceNumber', 0)
    ecc.codes_set(bufr, 'dataCategory', 2)  # Upper-air category
    ecc.codes_set(bufr, 'internationalDataSubCategory', 5)  # PIBAL
    ecc.codes_set(bufr, 'masterTablesVersionNumber', 40)
    ecc.codes_set(bufr, 'localTablesVersionNumber', 0)
    ecc.codes_set(bufr, "numberOfSubsets", len(parsed_data))
    ecc.codes_set_array(bufr, "inputDelayedDescriptorReplicationFactor", [1,1])
    station_numbers,block_numbers, latitude,longitude,heightAboveMeanSeaLevel = [],[],[],[],[]
    years, months, days, hours, minutes = [], [], [], [], []
    measuring_equipment = []
    pressures, wind_directions, wind_speeds, significances = [], [], [], []
    wind_shears_below, wind_shears_above = [], []
    replication_factor = []

    for subset in parsed_data:
        subset['pressureLevels'].append(subset['pressureLevelMax'])
        pressures.extend(subset['pressureLevels'])
        wind_directions.extend(subset['windDirection'])
        wind_speeds.extend(subset['windSpeed'])
        significances.extend(subset["extendedVerticalSoundingSignificance"])
        wind_shears_below.append(subset['absoluteWindShearIn1KmLayerBelow'])
        wind_shears_above.append(subset['absoluteWindShearIn1KmLayerAbove'])
        date = datetime.now(timezone.utc)  # noqa: F821)
        date= date.replace(day=subset['day'], hour=int(subset['hour']),minute=0)
        years.append(date.year)
        months.append(date.month)
        days.append(date.day)
        hours.append(date.hour)
        minutes.append(date.minute)
        station_numbers.append(int(subset['stationNumber']))
        block_numbers.append(int(subset['blockNumber']))
        measuring_equipment.append(int(subset['measuringEquipment']))
        station_id = subset['blockNumber']+subset['stationNumber']
        replication_factor.append(len(subset['windSpeed']))
        # Use the csv.reader to parse the CSV data
        # Loop through each row to find the matching station_id
        lat,lon,ele = ("MISSING","MISSING","MISSING")
        for row in csv_reader:
            if row["traditional_station_identifier"] == station_id:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                ele = float(row["elevation"])
                break
        latitude.append(lat)
        longitude.append(lon)
        heightAboveMeanSeaLevel.append(ele)

        

    # Pad arrays with missing values if necessary
    missing_long = ecc.CODES_MISSING_LONG
    missing_double = ecc.CODES_MISSING_DOUBLE
    pressures = handle_missing_values(pressures, missing_value=missing_long)
    wind_direction = handle_missing_values(wind_directions, missing_value=missing_long)
    wind_speed = handle_missing_values(wind_speeds, missing_value=missing_long)
    significance = handle_missing_values(significances, missing_value=0)
    latitude = handle_missing_values(latitude, missing_value=missing_double)
    longitude = handle_missing_values(longitude, missing_value=missing_double)
    heightAboveMeanSeaLevel = handle_missing_values(heightAboveMeanSeaLevel, missing_value=missing_double)

    ecc.codes_set_array(bufr, "inputExtendedDelayedDescriptorReplicationFactor", replication_factor)
    # Set the correct template for PILOT (Section 3)
    ecc.codes_set(bufr, 'unexpandedDescriptors', 309050)  # PILOT template
    ecc.codes_set(bufr, 'compressedData', 0)  # PILOT data
    # Get the number of levels expected by the template
    # Set the data section (Section 4)

    # Station information
    ecc.codes_set_array(bufr, 'blockNumber',block_numbers)
    ecc.codes_set_array(bufr, 'stationNumber', station_numbers)



    ecc.codes_set_array(bufr, 'latitude', latitude)
    ecc.codes_set_array(bufr, 'longitude',longitude)
    ecc.codes_set_array(bufr, 'heightOfStationGroundAboveMeanSeaLevel', heightAboveMeanSeaLevel)
    ecc.codes_set_array(bufr, 'height', heightAboveMeanSeaLevel)
    # Date and time
    ecc.codes_set_array(bufr, 'year', years)
    ecc.codes_set_array(bufr, 'month', months)
    ecc.codes_set_array(bufr, 'day', days)
    ecc.codes_set_array(bufr, 'hour', hours)
    ecc.codes_set_array(bufr, 'minute', minutes)

    # Measuring equipment
    # trackingTechniqueOrStatusOfSystem= 70 for	ALL SYSTEMS IN NORMAL OPERATION
    # radiosondeType= search for Code table for element 002011 (RADIOSONDE TYPE) depended on alat
    ecc.codes_set(bufr, 'timeSignificance', 18) 	# 18 for RADIOSONDE LAUNCH TIME
    ecc.codes_set_array(bufr, 'measuringEquipmentType', measuring_equipment)


    # Set arrays
    ecc.codes_set_array(bufr, 'extendedVerticalSoundingSignificance', significance)
    ecc.codes_set_array(bufr, 'pressure', pressures)
    ecc.codes_set_array(bufr, 'windDirection', wind_direction)
    ecc.codes_set_array(bufr, 'windSpeed', wind_speed)

    # Set wind shear information
    ecc.codes_set_array(bufr, 'absoluteWindShearIn1KmLayerBelow', wind_shears_below)
    ecc.codes_set_array(bufr, 'absoluteWindShearIn1KmLayerAbove', wind_shears_above)

    # Encode the message
    ecc.codes_set(bufr, 'pack', 1)
    # Write the message
    fh = BytesIO()
    ecc.codes_write(bufr, fh)
    fh.seek(0)
    ecc.codes_release(bufr)
    output = fh.read()

    return output 

def create_pilot_bufr_bd(parsed_data, station_metadata):
    # Convert the string to a file-like object
    csv_file = StringIO(station_metadata)
    csv_reader = csv.DictReader(csv_file)
    bufr = ecc.codes_bufr_new_from_samples("BUFR4")

    # Set BUFR edition
    ecc.codes_set(bufr, 'edition', 4)

    # Set identification section
    ecc.codes_set(bufr, 'masterTableNumber', 0)
    ecc.codes_set(bufr, 'bufrHeaderCentre', 195)
    ecc.codes_set(bufr, 'bufrHeaderSubCentre', 0)
    ecc.codes_set(bufr, 'updateSequenceNumber', 0)
    ecc.codes_set(bufr, 'dataCategory', 2)
    ecc.codes_set(bufr, 'internationalDataSubCategory', 1)
    ecc.codes_set(bufr, 'masterTablesVersionNumber', 31)
    ecc.codes_set(bufr, 'localTablesVersionNumber', 0)
    ecc.codes_set(bufr, "numberOfSubsets", len(parsed_data))
    ecc.codes_set_array(bufr, "inputDelayedDescriptorReplicationFactor", [1,1])


    altitudes, wind_directions, wind_speeds, significances,years,months,days,hours,minutes,station_numbers,block_numbers,measuring_equipment,replication_factor = ([] for _ in range(13))
    latitude, longitude, heightAboveMeanSeaLevel = ([] for _ in range(3))
    for subset in parsed_data:
        altitudes.extend(subset['altitudeLevels'])
        wind_directions.extend(subset['windDirection'])
        wind_speeds.extend(subset['windSpeed'])
        significances.extend(subset["extendedVerticalSoundingSignificance"])
        date = datetime.now(timezone.utc)  # noqa: F821)
        date= date.replace(day=subset['day'], hour=int(subset['hour']),minute=0)
        years.append(date.year)
        months.append(date.month)
        days.append(date.day)
        hours.append(date.hour)
        minutes.append(date.minute)
        station_numbers.append(int(subset['stationNumber']))
        block_numbers.append(int(subset['blockNumber']))
        measuring_equipment.append(int(subset['measuringEquipment']))
        station_id = subset['blockNumber']+subset['stationNumber']
        replication_factor.append(len(subset['windSpeed']))
        # Use the csv.reader to parse the CSV data
        # Loop through each row to find the matching station_id
        lat,lon,ele = ("MISSING","MISSING","MISSING")
        for row in csv_reader:
            if row["traditional_station_identifier"] == station_id:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                ele = float(row["elevation"])
                break
        latitude.append(lat)
        longitude.append(lon)
        heightAboveMeanSeaLevel.append(ele)
    ecc.codes_set_array(bufr, "inputExtendedDelayedDescriptorReplicationFactor", replication_factor)
    
    # Template to be used
    ivalues = [301110, 301113, 301114, 101000, 31002, 303052]
    ecc.codes_set_array(bufr, 'unexpandedDescriptors', ivalues)


    missing_long = ecc.CODES_MISSING_LONG
    missing_double = ecc.CODES_MISSING_DOUBLE
    # Process data, converting from km to m, and handle 'MISSING' values
    altitude = handle_missing_values(altitudes, missing_value=missing_long)
    wind_direction = handle_missing_values(wind_directions, missing_value=missing_long)
    wind_speed = handle_missing_values(wind_speeds, missing_value=missing_long)
    significance = handle_missing_values(significances, missing_value=0)
    latitude = handle_missing_values(latitude, missing_value=missing_double)
    longitude = handle_missing_values(longitude, missing_value=missing_double)
    heightAboveMeanSeaLevel = handle_missing_values(heightAboveMeanSeaLevel, missing_value=missing_double)


    # Set the data section
    ecc.codes_set_array(bufr, 'blockNumber', block_numbers)
    ecc.codes_set_array(bufr, 'stationNumber', station_numbers)
    ecc.codes_set_array(bufr, 'latitude',latitude)
    ecc.codes_set_array(bufr, 'longitude', longitude)
    ecc.codes_set_array(bufr, 'heightOfStationGroundAboveMeanSeaLevel',heightAboveMeanSeaLevel)
    ecc.codes_set_array(bufr, 'height', heightAboveMeanSeaLevel)
    ecc.codes_set_array(bufr, 'year',years)
    ecc.codes_set_array(bufr, 'month', months)
    ecc.codes_set_array(bufr, 'day', days)
    ecc.codes_set_array(bufr, 'hour', hours)
    ecc.codes_set_array(bufr, 'minute', minutes)
    ecc.codes_set_array(bufr, 'timeSignificance', [18]*len(years))
    ecc.codes_set_array(bufr, 'measuringEquipmentType', measuring_equipment)
    
    # Set arrays
    ecc.codes_set_array(bufr, 'extendedVerticalSoundingSignificance', significance)
    ecc.codes_set_array(bufr, 'geopotentialHeight', altitude)
    ecc.codes_set_array(bufr, 'windDirection', wind_direction)
    ecc.codes_set_array(bufr, 'windSpeed', wind_speed)

    # Encode the message
    ecc.codes_set(bufr, 'pack', 1)

    # Write the message
    fh = BytesIO()
    ecc.codes_write(bufr, fh)
    fh.seek(0)
    ecc.codes_release(bufr)
    output = fh.read()

    return output 

    return output 