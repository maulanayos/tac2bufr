import re
import json
import numpy as np

from engine.temp_engine.jsontemp2bufr import create_temp_bufr_ac, create_temp_bufr_bd

# from engine.temp_engine.jsontemp2bufr import create_temp_bufr_ac, create_temp_bufr_bd

def sect2_ac_split(input_string):
    pattern = r'([0-9\/]{2})([0-9\/]{3})\s([0-9\/]{2})([0-9\/]{1})([0-9\/]{2})\s([0-9\/]{3})([0-9\/]{2})'
    matches = re.finditer(pattern, input_string)
    results = []

    for match in matches:
        # Check if the first group is "88", "77", or "66"
        if match.group(1) in {'88', '77', '66'}:
            break
        else:
            results.append(match)
    
    return results

def sect5_bd_split(input_string):
    pattern = r'([0-9\/]{2})([0-9\/]{3})\s([0-9\/]{2})([0-9\/]{1})([0-9\/]{2})'
    matches = re.finditer(pattern, input_string)
    results = []

    for match in matches:
        if match.group(1) in {'21', '31', '41', '51', '61'}:
            break
        else:
            results.append(match)

    return results

def extract_single_part_ac(line:str, part:str):
    sect_a_pressure_levels = [1000,925,850,700,500,400,300,250,200,150,100]
    sect_c_pressure_levels = [70,50,30,20,10]
    standard_pressure_levels = sect_a_pressure_levels if part == "a" else sect_c_pressure_levels
    all_pressure_levels = standard_pressure_levels.copy()
    output_dict = {}
    #Section 1
    section1_pattern = r'(TTAA|TTCC)\s(\d{2})(\d{2})(\d{1})\s(\d{2})(\d{3})'
    _, day, hour, last_sounding_idx,block_number, station_number = re.search(section1_pattern, line).groups()
    day = int(day) - 50 if int(day) > 50 else int(day)
    datas = re.split(section1_pattern, line)
    datas = datas[-1]
    sect_1_items = {"day":day, "hour":hour, "blockNumber":block_number, "stationNumber":station_number}
    output_dict.update(sect_1_items)
    #Section 2
    
    sect_2_contents = sect2_ac_split(datas)
    all_height_levels = []
    all_wind_direction = []
    all_wind_speed = []
    all_temperature = []
    all_dew_point = []
    significance = []

    if sect_2_contents[0].group(1) == '99':
        _, pressure0, temperature0, per_tenth0, dew_point0, wind_direction0, wind_speed0 = sect_2_contents[0].groups()
        all_height_levels.append(0) # to be replaced with station height
        all_pressure_levels.insert(0,1000+int(pressure0) if int(pressure0) < 100 else int(pressure0))
        temp_unit0 = 1 if int(per_tenth0) % 2 == 0 else -1
        per_tenth_temp0 = int(per_tenth0) + 1 if int(per_tenth0) % 2 == 0 else int(per_tenth0)
        try :
            all_temperature.append((float(temperature0) + (per_tenth_temp0/10)) *temp_unit0)
        except ValueError:
            all_temperature.append("MISSING")
        try : 
            all_dew_point.append(int(dew_point0))
        except ValueError:
            all_dew_point.append("MISSING")
        try :
            wspeed0 = int(wind_speed0) if int(wind_speed0) < 500  else int(wind_speed0) - 500    
            wdir0 = int(wind_direction0) if int(wind_speed0) < 500 else (int(wind_direction0)*10) +5
            all_wind_direction.append(wdir0)
            all_wind_speed.append(int(wspeed0))
        except ValueError:
            all_wind_direction.append("MISSING")
            all_wind_speed.append("MISSING")
        significance.append(131072)
        sect_2_contents = sect_2_contents[1:]

    for i,content in enumerate(sect_2_contents):
        pressure, height, temperature, per_tenth, dew_point, wind_direction, wind_speed = content.groups()
        
        if pressure not in str(standard_pressure_levels[i]):
            raise ValueError("TAC pressure level is unsubscriptable, make sure TAC is written correctly")
        try :
            all_height_levels.append(int(height))
        except ValueError:
            all_height_levels.append("MISSING")

        try :
            temp_unit = 1 if int(per_tenth) % 2 == 0 else -1
            per_tenth_temp = int(per_tenth) + 1 if int(per_tenth) % 2 == 0 else int(per_tenth)
            all_temperature.append((float(temperature) + (per_tenth_temp/10)) *temp_unit)
        except ValueError:
            all_temperature.append("MISSING")

        try : 
            all_dew_point.append(int(dew_point))
        except ValueError:
            all_dew_point.append("MISSING")

        try :
            wspeed = int(wind_speed) if int(wind_speed) < 500  else int(wind_speed) - 500    
            wdir = int(wind_direction) if int(wind_speed) < 500 else (int(wind_direction)*10) +5
            all_wind_direction.append(wdir)
            all_wind_speed.append(int(wspeed))
        except ValueError:
            all_wind_direction.append("MISSING")
            all_wind_speed.append("MISSING")

        significance.append(65536 if i != last_sounding_idx else 16)

    #Section 3

    sect_3_pattern = r'(88\d{3})\s(\d{2})(\d{1})(\d{2})\s(\d{3})(\d{2})'
    sect_3_contents = re.search(sect_3_pattern, datas)
    if sect_3_contents and sect_3_contents.group(1) != '88999':
        tropo_pressure, tropo_temperature, tropo_per_tenth, tropo_dew_point, tropo_wind_direction, tropo_wind_speed = sect_3_contents.groups()
        try :
            tropo_temp_unit = 1 if int(tropo_per_tenth) % 2 == 0 else -1
            tropo_per_tenth = int(tropo_per_tenth) + 1 if int(tropo_per_tenth) % 2 == 0 else int(tropo_per_tenth)
            all_pressure_levels.append(int(tropo_pressure))
            all_dew_point.append(int(tropo_dew_point))
            all_temperature.append((float(tropo_temperature) + (tropo_per_tenth/10)) *tropo_temp_unit)
        except ValueError:
            all_pressure_levels.append("MISSING")
            all_temperature.append("MISSING")
            all_dew_point.append("MISSING")
        try :
            wspeed_tropo = int(tropo_wind_speed) if int(tropo_wind_speed) < 500  else int(tropo_wind_speed) - 500    
            wdir_tropo = int(tropo_wind_direction) if int(tropo_wind_speed) < 500 else (int(tropo_wind_direction)*10) +5
            all_wind_direction.append(wdir_tropo)
            all_wind_speed.append(int(wspeed_tropo))
        except ValueError:
            all_wind_direction.append("MISSING")
            all_wind_speed.append("MISSING")
        significance.append(32768)

    
    #Section 4
    default_sect_4 = ("MISSING", "MISSING", "MISSING","MISSING","MISSING")
    no_mw_pattern = r'77999'
    regex = re.compile(no_mw_pattern)
    if regex.search(datas):
        idwmax = all_wind_speed.index(max([v if v != 'MISSING' else -9999 for v in all_wind_speed]))
        significance[idwmax] = 	16384
        all_pressure_levels.append("MISSING")
        significance.append(64)
    else:
        max_wind_pattern = r'(77|66)(\d{3})\s(\d{2})(\d{3})\s4(\d{2})(\d{2})'
        max_wind_data_re = re.search(max_wind_pattern, line)
        _,pressure_level_max, max_wind_direction, max_wind_speed,vbvb,vava = max_wind_data_re.groups() if max_wind_data_re else default_sect_4
        all_pressure_levels.append(int(pressure_level_max))
        wspeed_max = int(max_wind_speed) if int(max_wind_speed) < 500  else int(max_wind_speed) - 500    
        wdir_max = int(max_wind_direction) if int(max_wind_speed) < 500 else (int(max_wind_direction)*10) +5
        all_wind_direction.append(wdir_max)
        all_wind_speed.append(int(wspeed_max))
        significance.append(16384)
        sect_4_items = {'absoluteWindShearIn1KmLayerBelow':vbvb,'absoluteWindShearIn1KmLayerAbove':vava}
        output_dict.update(sect_4_items)

    
    #Section 7
    sect_7_pattern = r'31313\s(\d{1})(\d{2})(\d{2})\s8(\d{2})(\d{2})'
    sect_7_contents = re.search(sect_7_pattern, datas)
    if sect_7_contents:
        solar_correction,radiosonde_type,tracking_technique,hour,minute = sect_7_contents.groups()
        #TODO : add item also change type
        sect_7_items = {'solarCorrection':int(solar_correction),'radiosondeType':int(radiosonde_type),'trackingTechnique':int(tracking_technique), 'hour':int(hour),'minute':int(minute)}
        output_dict.update(sect_7_items)
    rest_of_data = {'pressureLevels':all_pressure_levels,"nonCoordinateGeopotentialHeight":all_height_levels,'airTemperature':all_temperature,'dewpointTemperature':all_dew_point,\
                    'windDirection':all_wind_direction,'windSpeed':all_wind_speed,'verticalSignificanceSurfaceObservations':significance}
    output_dict.update(rest_of_data)
    return output_dict
    

def extract_single_part_bd(line:str):
    output_dict = {}
    #Section 1
    section1_pattern = r'(TTBB|TTDD)\s(\d{2})(\d{2})(\d{1})\s(\d{2})(\d{3})'
    _, day, hour,measuring_equipment, block_number, station_number = re.search(section1_pattern, line).groups()
    day = int(day) - 50 if int(day) > 50 else int(day)
    datas = re.split(section1_pattern, line)
    datas = datas[-1]
    sect_1_items = {"day":day, "hour":hour, "blockNumber":block_number,"measuring_equipment":measuring_equipment, "stationNumber":station_number}
    output_dict.update(sect_1_items)
    #Section 5
    
    sect_2_contents = sect5_bd_split(datas)
    all_temperature = []
    all_dew_point = []
    significance = []
    all_pressure_levels = []
    all_wind_directions = []
    all_wind_speeds = []

    for i,content in enumerate(sect_2_contents):
        _,pressure, temperature, per_tenth, dew_point = content.groups()
        all_pressure_levels.append(int(pressure))
        try :
            temp_unit = 1 if int(per_tenth) % 2 == 0 else -1
            per_tenth_temp = int(per_tenth) + 1 if int(per_tenth) % 2 == 0 else int(per_tenth)
            all_temperature.append((float(temperature) + (per_tenth_temp/10)) *temp_unit)
        except ValueError:
            all_temperature.append("MISSING")

        try : 
            all_dew_point.append(int(dew_point))
        except ValueError:
            all_dew_point.append("MISSING")
        all_wind_directions.append("MISSING")
        all_wind_speeds.append("MISSING")
        significance.append(8192)

    #Section 6

    sect_6_pattern = r'21212\s(.*?(?=\s31313|=))'
    wind_pattern = r'(\d{2})(\d{3})\s(\d{2})(\d{3})'
    sect_6_contents = re.search(sect_6_pattern, datas)
    if sect_6_contents:
        wind_contents = re.findall(wind_pattern, sect_6_contents.group(1))
        pressure_levels = [int(wind[1]) for wind in wind_contents]
        wind_directions = [int(wind[2])*10 if int(wind[3]) <500 else (int(wind[2])*10)+5 for wind in wind_contents]
        wind_speeds = [int(wind[3]) if int(wind[3]) < 500 else int(wind[3])-500 for wind in wind_contents]

        for pressure, wind_direction, wind_speed in zip(pressure_levels, wind_directions, wind_speeds, ):
            if pressure in all_pressure_levels:
                # Find the index of the pressure in all_pressure_levels
                idx = all_pressure_levels.index(pressure)
                # Update the wind direction at the found index
                all_wind_directions[idx] = wind_direction
                all_wind_speeds[idx] = wind_speed
                significance[idx] = 2048
            else:
                # If pressure not in all_pressure_levels, append the new data
                all_pressure_levels.append(pressure)
                all_wind_directions.append(wind_direction)
                all_wind_speeds.append(wind_speed)
                all_temperature.append("MISSING")
                all_dew_point.append("MISSING")
                significance.append(2048) #TODO: add feature if pressure levels is present on section 5 then dont append new
        # all_pressure_levels.extend(pressure_levels)
        # all_wind_directions.extend(wind_directions)
        # all_wind_speeds.extend(wind_speeds)
        # all_temperature.extend(["MISSING"]*len(pressure_levels))
        # all_dew_point.extend(["MISSING"]*len(pressure_levels))
        
    #Section 7
    sect_7_pattern = r'31313\s(\d{1})(\d{2})(\d{2})\s8(\d{2})(\d{2})'
    sect_7_contents = re.search(sect_7_pattern, datas)
    if sect_7_contents:
        solar_correction,radiosonde_type,tracking_technique,hour,minute = sect_7_contents.groups()
        #TODO : add item also change type
        sect_7_items = {'solarCorrection':int(solar_correction),'radiosondeType':int(radiosonde_type),'trackingTechnique':int(tracking_technique), 'hour':int(hour),'minute':int(minute)}
        output_dict.update(sect_7_items)

    #Section 8
    sect_8_pattern = r'41414\s(\d{1})(\d{1})(\d{1})(\d{1})(\d{1})'
    sect_8_contents = re.search(sect_8_pattern, datas)
    if sect_8_contents:
        cloud_amounts,cloud_type1,height_base_cloud,cloud_type2,cloud_type3 = sect_8_contents.groups()
        sect_8_items = {'cloudAmounts':int(cloud_amounts),'cloudType':[int(cloud_type1),int(cloud_type2),int(cloud_type3)],'heightBaseCloud':int(height_base_cloud)}
        output_dict.update(sect_8_items)
    
    #MISSING MAX WIND DATA
    all_pressure_levels.append("MISSING")
    significance.append(16384)
    rest_of_data = {'pressureLevels':all_pressure_levels,'airTemperature':all_temperature,'dewpointTemperature':all_dew_point,\
                    'windDirection':all_wind_directions,'windSpeed':all_wind_speeds,'verticalSignificanceSurfaceObservations':significance}
    output_dict.update(rest_of_data)
    return output_dict

def extract_part_ac(message:str):
    line_pattern = r'TTAA[^=]*=|TTCC[^=]*='
    line_matches = re.findall(line_pattern, message)
    #Start parsing and storing it into array
    results = []
    # results = [extract_part_ac(line) if "TTAA" in line else extract_single_part_c for line in line_matches]
    for line in line_matches :
        try :
            part = "a" if "TTAA" in line else "c"
            result = extract_single_part_ac(line,part)
            results.append(result)
        except Exception as e:
            print(e)
    return results
       
        
def extract_part_bd(message:str):
    line_pattern = r'TTBB[^=]*=|TTDD[^=]*='
    line_matches = re.findall(line_pattern, message)
    results = []
    for line in line_matches :
        result = extract_single_part_bd(line)
        results.append(result)
    return results

    # #Start parsing and storing it into array
    # for m in matches :
    #     var = re.findall(variables_pattern, m)
    #         for v in var:
            

def parse_temp(message_without_header:str, tt:str,station_metadata):
    """
    Parse a TEMP message and return a dictionary with the data.

    """
    message_without_header = message_without_header.strip()
    flatten = message_without_header.replace('\n', ' ').replace('\t', ' ').replace('\r', '')
    flatten = ' '.join(message_without_header.split())
    # results = extract_part_ac(flatten) if tt == "US" or tt == "UL" \
    #     else extract_part_bd(flatten) if tt == "UK" or tt == "UE" \
        # else None
    if tt == "US" or tt == "UL":
        results = extract_part_ac(flatten)
        bufr_bin = create_temp_bufr_ac(results, station_metadata=station_metadata)
    elif tt == "UK" or tt == "UE":
        results = extract_part_bd(flatten)
        bufr_bin = create_temp_bufr_bd(results, station_metadata=station_metadata)
    else:
        return None
    return bufr_bin   