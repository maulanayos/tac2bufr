from synop2bufr import transform as synop_transform
from engine.pilot_engine.pilot2json import parse_pilot
from engine.temp_engine.temp2json import parse_temp



async def synop2bufr(payload, metadata) -> tuple:
    try :
        time = payload['timestamp']
        year = int(time[:4])
        month = int(time[5:7])
        synop_string = payload['message']
        decoded_message = synop_transform(synop_string,metadata = metadata ,year = year, month = month)
        content = []
        for item in decoded_message:
            if item["_meta"]["properties"].get("datetime") is not None and item.get("bufr4") is not None:
                content.append(item['bufr4'])
        content = b''.join(content)
        return (None, content)
    except Exception as e:
        return (e,None)
    

async def pilot2bufr(payload: dict, station_metadata:str) -> tuple:
    pilot_string = payload['message']
    try :
        tt = payload['tt']
        decoded = parse_pilot(pilot_string,tt, station_metadata)
        return (None, decoded)
    except Exception as e:
        return (e,None)

async def temp2bufr(payload: dict, station_metadata:str) -> tuple:
    temp_string = payload['message']
    try :
        tt = payload['tt']
        decoded = parse_temp(temp_string,tt, station_metadata)
        return (None, decoded)
    except Exception as e:
        return (e,None)
    