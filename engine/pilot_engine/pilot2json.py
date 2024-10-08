import re

from engine.pilot_engine.jsonpilot2bufr import create_pilot_bufr_ac, create_pilot_bufr_bd

'''
IMPORTANT NOTES:

- For section A, C based on common practice in Indonesia, the level is in pressure levels
for  now, it only accomodates such level.
- For section B, D, the level is in height levels, which is in feets
'''

def extract_single_part_ac(line:str):
    #Section 1
    section1_pattern = r'(PPAA|PPCC)\s(\d{2})(\d{2})(\d{1})\s(\d{2})(\d{3})'
    parts,day, hour, measuring_equipment,block_number, station_number = re.search(section1_pattern, line).groups()
    day = int(day) - 50 if int(day) > 50 else int(day)
    #Section 2
    data_tac = re.split(section1_pattern, line)
    var_pattern = r'(55\d{3}|44\d{3})\s(.*?)\s(?=55\d{3}|44\d{3}|7\d{4}|6\d{4}|77999=)'
    dimension_pattern = r'(\d{2})(\d{1})(\d{2})'
    wind_pattern = r'(\d{2})(\d{3})'
    ppaa_pressure = [850, 700, 500, 400, 300, 250, 200, 150,100] if parts == "PPAA" else [70, 50, 30, 20, 10]
    variables = re.findall(var_pattern, data_tac[-1])
    all_pressure_levels = []
    all_wind_direction = []
    all_wind_speed = []
    for v in variables:
        dimension, winds = v
        standard_isobaric, amount_observed, pressure_level_start = re.search(dimension_pattern, dimension).groups()
        idx = ppaa_pressure.index(int(pressure_level_start)*10)
        pressure_level = ppaa_pressure[idx:idx+int(amount_observed)]
        all_pressure_levels.extend(pressure_level)
        winds = re.findall(wind_pattern, winds)
        wind_directions = [int(wind[0])*10 if int(wind[1]) <500 else (int(wind[0])*10)+5 for wind in winds]
        wind_speeds = [int(wind[1]) if int(wind[1]) < 500 else int(wind[1])-500 for wind in winds]
        all_wind_direction.extend(wind_directions)
        all_wind_speed.extend(wind_speeds)

    #Section 3
    default_sect_3 = ("MISSING", "MISSING", "MISSING","MISSING","MISSING")
    no_mw_pattern = r'77999='
    regex = re.compile(no_mw_pattern)
    if regex.search(data_tac[-1]):
        idwmax = all_wind_speed.index(max(all_wind_speed))

        extendedVerticalSoundingSignificance = [65536] * (len(all_pressure_levels) + 1)
        extendedVerticalSoundingSignificance[idwmax] = 	2048
        return {
            "blockNumber": block_number,
            "stationNumber": station_number,
            "day": day,
            "hour": hour,
            "measuringEquipment": measuring_equipment,
            "pressureLevels": all_pressure_levels,
            "extendedVerticalSoundingSignificance":extendedVerticalSoundingSignificance,
            "windDirection": all_wind_direction,
            "windSpeed": all_wind_speed,
            "pressureLevelMax": "MISSING", 
            "maxWindDirection": "MISSING", 
            "maxWindSpeed": "MISSING",
            "absoluteWindShearIn1KmLayerBelow" : "MISSING",
            "absoluteWindShearIn1KmLayerAbove" : "MISSING"
        }
    
    if standard_isobaric == "44":
        max_wind_pattern = r'(77|66)(\d{3})\s(\d{2})(\d{3})\s4(\d{2})(\d{2})'
        max_wind_data_re = re.search(max_wind_pattern, line)
        _,pressure_level_max, max_wind_direction, max_wind_speed,vbvb,vava = max_wind_data_re.groups() if max_wind_data_re else default_sect_3
        pressure_level_max = int(pressure_level_max)
    elif standard_isobaric == "55":
        max_wind_pattern = r'(7\d{4}|6\d{4})\s(\d{2})(\d{3})\s4(\d{2})(\d{2})'
        max_wind_data_re = re.search(max_wind_pattern, line)
        pressure_level_max, max_wind_direction, max_wind_speed,vbvb,vava = max_wind_data_re.groups() if max_wind_data_re else default_sect_3
        pressure_level_max = int(pressure_level_max)

    #Section 5, undefined
    #Section 6, undefined
    extendedVerticalSoundingSignificance = [65536] * (len(all_pressure_levels) + 1)
    extendedVerticalSoundingSignificance[-1] = 	2048
    return {
            "blockNumber": block_number,
            "stationNumber": station_number,
            "day": day,
            "hour": hour,
            "measuringEquipment": measuring_equipment,
            "pressureLevels": all_pressure_levels,
            "extendedVerticalSoundingSignificance":extendedVerticalSoundingSignificance,
            "windDirection": all_wind_direction,
            "windSpeed": all_wind_speed,
            "pressureLevelMax": pressure_level_max, 
            "maxWindDirection": int(max_wind_direction), 
            "maxWindSpeed": int(max_wind_speed),
            "absoluteWindShearIn1KmLayerBelow" : int(vbvb),
            "absoluteWindShearIn1KmLayerAbove" : int(vava)
            }

def extract_single_part_bd(line:str):
    #Section 1
    section1_pattern = r'(PPBB|PPDD)\s(\d{2})(\d{2})(\d{1})\s(\d{2})(\d{3})'
    parts,day, hour, measuring_equipment,block_number, station_number = re.search(section1_pattern, line).groups()
    day = int(day) - 50 if int(day) > 50 else int(day)
    data_tac = re.split(section1_pattern, line)
    #Section 4
    var_pattern = r'\s9(\d{1})([0-9\/]{3})\s(.*?(?=\s9|=))'
    wind_pattern = r'(\d{2})(\d{3})'
    variables = re.findall(var_pattern, data_tac[-1])
    all_altitude_levels = []
    all_wind_direction = []
    all_wind_speed = []
    extendedVerticalSoundingSignificance = []
    for v in variables:
        altitude_start, altitude_dimension, wind_data = v
        altitude_start = int(altitude_start)
        altitudes = [int(s) if s != "/" else "MISSING" for s in altitude_dimension]
        levels = [altitude_start*10000 + a*1000 if isinstance(a,int) else "MISSING" for a in altitudes]
        all_altitude_levels.extend(levels)
        winds = re.findall(wind_pattern, wind_data)
        wind_directions = [int(wind[0])*10 if int(wind[1]) <500 else (int(wind[0])*10)+5 for wind in winds]
        wind_speeds = [int(wind[1]) if int(wind[1]) < 500 else int(wind[1])-500 for wind in winds]
        if len(wind_speeds) != len(levels):
            miss_idx = [i for i, e in enumerate(levels) if e == "MISSING"]
            for idx in miss_idx:
                wind_speeds.insert(idx, "MISSING")
                wind_directions.insert(idx, "MISSING")
        significance = [65536 if isinstance(w,int) else 32 for w in wind_speeds]
        extendedVerticalSoundingSignificance.extend(significance)
        all_wind_direction.extend(wind_directions)
        all_wind_speed.extend(wind_speeds)

    idwmax = all_wind_speed.index(max([x if x != "MISSING" else 0 for x in all_wind_speed]))
    extendedVerticalSoundingSignificance[idwmax] = 	2048
    extendedVerticalSoundingSignificance[-1] = 	16
    return {
        "blockNumber": block_number,
        "stationNumber": station_number,
        "day": day,
        "hour": hour,
        "measuringEquipment": measuring_equipment,
        "altitudeLevels": all_altitude_levels,
        "extendedVerticalSoundingSignificance":extendedVerticalSoundingSignificance,
        "windDirection": all_wind_direction,
        "windSpeed": all_wind_speed,
    }

def extract_part_ac(message:str):
    line_pattern = r'PPAA[^=]*=|PPCC[^=]*='
    line_matches = re.findall(line_pattern, message)
    #Start parsing and storing it into array
    results = []
    for line in line_matches :
        result = extract_single_part_ac(line)
        results.append(result)
    return results
       
        
def extract_part_bd(message:str):
    line_pattern = r'PPBB[^=]*=|PPDD[^=]*='
    line_matches = re.findall(line_pattern, message)
    results = []
    for line in line_matches :
        result = extract_single_part_bd(line)
        results.append(result)
    return results

def parse_pilot(message_without_header:str, tt:str,station_metadata):
    """
    Parse a PILOT message and return a dictionary with the data.

    """
    message_without_header = message_without_header.strip()
    flatten = message_without_header.replace('\n', ' ').replace('\t', ' ').replace('\r', '')
    flatten = ' '.join(message_without_header.split())
    results = extract_part_ac(flatten) if tt == "UP" or tt == "UH" \
        else extract_part_bd(flatten) if tt == "UG" or tt == "UQ" \
        else None
    if tt == "UP" or tt == "UH":
        results = extract_part_ac(flatten)
        bufr_bin = create_pilot_bufr_ac(results, station_metadata=station_metadata)
    elif tt == "UG" or tt == "UQ":
        results = extract_part_bd(flatten)
        bufr_bin = create_pilot_bufr_bd(results, station_metadata=station_metadata)
    else:
        return None
    return bufr_bin
        