from typing import Optional
from pydantic import BaseModel
from datetime import datetime


class PayloadInModel(BaseModel):
	file_id: Optional[str]
	file_name: Optional[str]
	source: Optional[str]
	file_size: Optional[int]
	timestamp: Optional[datetime]  # waktu diterima
	circuit_in_id: Optional[int]
	user_id: Optional[int] = None
	type: Optional[str]  # synop, metar, speci, taf, temp, todo yos masukin semua type message. nc, binary
	# raw_message : Optional[str] # full message including header
	header: Optional[str]  # SAID31 WIII 010000
	message: Optional[str]  # without header, unformatted -> BIKIN
	is_bulletin: Optional[bool] = False
	station_id: Optional[str]
	tt: Optional[str]
	aa: Optional[str]
	ii: Optional[str]
	cccc: Optional[str]
	bbb: Optional[str]
	filling_time: Optional[datetime]

class PayloadOutModel(PayloadInModel):
	source: Optional[str] = "tac2bufr"

	