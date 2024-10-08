import asyncpg
import csv
import io
import asyncio
from typing import Optional

class MetadataManager:
    """
    Manager for fetching and caching station data as CSV and metadata as dict.

    This class provides methods to update the cached station data from
    the database and retrieve it as a CSV string.
    """

    def __init__(self, uri: str):
        """
        Initializes the manager with a given database URI.

        Args:
            uri (str): The database connection URI.
        """
        self.uri = uri
        self.connection: Optional[asyncpg.Connection] = None
        self.cached_csv_data: Optional[str] = None
        self.mapper = []

    async def ensure_updated(self) -> "MetadataManager":
        """
        Ensures that the station data is updated.

        Returns:
            "MetadataManager": The manager instance.
        """
        await self.initialize_db_connection()
        if self.cached_csv_data is None:
            await self.update_station_data()
            await self.update_mapper()

        return self

    async def initialize_db_connection(self) -> None:
        """
        Initializes the database connection using a URI.
        """
        self.connection = await asyncpg.connect(self.uri)

    async def update_station_data(self) -> None:
        """
        Fetches data from the database and updates the cache.

        Executes the query to retrieve station data and updates the cached CSV data.
        Raises an exception if the connection is not initialized.
        """
        if self.connection is None:
            raise Exception("Database connection is not initialized.")

        query = """
        SELECT 
            name AS station_name,
            wigos AS wigos_station_identifier,
            wmo AS traditional_station_identifier,
            facility_type,
            latitude,
            longitude,
            altitude AS elevation,
            barometer_height AS barometer_height,
            teritory AS territory_name,
            wmo_region
        FROM 
            station;
        """

        records = await self.connection.fetch(query)

        output = io.StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow([
            'station_name', 'wigos_station_identifier', 'traditional_station_identifier',
            'facility_type', 'latitude', 'longitude', 'elevation',
            'barometer_height', 'territory_name', 'wmo_region'
        ])
        
        for record in records:
            csv_writer.writerow(record)

        self.cached_csv_data = output.getvalue()
        output.close()
    
    async def update_mapper(self) -> None:
        """
        Fetches data from the database and updates the cache.

        Executes the query to retrieve station data and updates the cached CSV data.
        Raises an exception if the connection is not initialized.
        """
        if self.connection is None:
            raise Exception("Database connection is not initialized.")

        query = """
        SELECT 
            ttaaii_in,
            ttaaii_out
        FROM 
            tac2bufr;
        """

        records = await self.connection.fetch(query)

        mapper_tmp = []
        for record in records:
            mapper_tmp.append(
                {"ttaaii_in": record[0], "ttaaii_out": record[1]}
            )

        self.mapper = mapper_tmp

    def get_mapper(self) -> Optional[dict]:
        """
        Retrieves the cached CSV station data.

        Returns:
            Optional[str]: CSV data as a string if available; otherwise, None.
        """
        return self.mapper
        
    def get_data(self) -> Optional[str]:
        """
        Retrieves the cached CSV station data.

        Returns:
            Optional[str]: CSV data as a string if available; otherwise, None.
        """
        return self.cached_csv_data

    async def close_db_connection(self) -> None:
        """
        Closes the database connection if it's initialized.
        """
        if self.connection is not None:
            await self.connection.close()
            self.connection = None

    async def close(self) -> None:
        """
        Closes the manager and the database connection.
        """
        await self.close_db_connection()

    async def run_update(self) -> None:
        """
        Continuously updates the station data every 5 minutes.
        """
        try:
            while True:
                await self.update_station_data()
                await self.update_mapper()
                await asyncio.sleep(300)  # Sleep for 5 minutes (300 seconds)
        except asyncio.CancelledError:
            pass