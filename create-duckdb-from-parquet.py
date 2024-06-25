#%%
import duckdb
import os


## check if directory exists and if not create it
directory = 'duckdb-database'
if not os.path.exists(directory):
    os.makedirs(directory)

## create connection to database file
con = duckdb.connect(database='duckdb-database/itineraries.duckdb')

preview_data_query = "SELECT COUNT(*) FROM 'data/expedia-itineraries.snappy.parquet'"
con.execute(preview_data_query).df()


load_query = """
    CREATE OR REPLACE TABLE itineraries 
    AS SELECT
        flightDate,
        startingAirport,
        destinationAirport,
        -- travelDuration, /* inferred from Arrival & Departure */
        -- isBasicEconomy, /* not required for analysis */
        -- isRefundable, /* not required for analysis */
        -- isNonStop, /* applied as a filter on a single value */
        -- baseFare, /* not required for analysis */
        totalFare,
        seatsRemaining,
        segmentsAirlineName,
        segmentsArrivalTimeRaw,
        segmentsDepartureTimeRaw,
        segmentsCabinCode
    FROM 'data/expedia-itineraries.snappy.parquet'
    WHERE isNonStop
"""

con.execute(load_query)

con.execute("SELECT count(*) FROM itineraries").df()

# %%
