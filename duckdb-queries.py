#%%

import duckdb
import altair as alt

con = duckdb.connect(database='duckdb-database/itineraries.duckdb', read_only=True)

#%%
con.execute("""
        SELECT 
            DISTINCT startingAirport 
        FROM itineraries 
        ORDER BY startingAirport
    """).df()

#%%
startingAirport = "DEN"

#%%
con.execute("""
        SELECT 
            DISTINCT destinationAirport 
        FROM itineraries 
        WHERE startingAirport = ? 
        ORDER BY destinationAirport ASC
    """, [startingAirport]).df()

#%%
destinationAirport = "PHL"

#%%
(ts_min, ts_max) = con.execute("""
    SELECT 
        min(segmentsArrivalTimeRaw::timestamp), 
        max(segmentsArrivalTimeRaw::timestamp) 
    FROM itineraries 
    WHERE startingAirport = ? 
        AND destinationAirport = ?
""", [startingAirport, destinationAirport]).fetchone()

#%%

#%%
main_table_count = con.execute("""
    SELECT COUNT(*) 
    FROM itineraries 
    WHERE startingAirport = ? 
        AND destinationAirport = ? 
        AND segmentsArrivalTimeRaw BETWEEN ? AND ? 
    """, [startingAirport, destinationAirport, slider_min, slider_max]).fetchone()[0]



main_table_head = con.execute("""
    SELECT * 
    FROM itineraries 
    WHERE startingAirport = ? 
        AND destinationAirport = ? 
        AND segmentsArrivalTimeRaw BETWEEN ? AND ? 
    LIMIT 10
    """, [startingAirport, destinationAirport, slider_min, slider_max]).df()


st.write('Total number of rows: ', main_table_count) 
st.write('First 10 rows: ', main_table_head)

st.subheader('Aggregates')

col_b1, col_b2 = st.columns(2)

with col_b1: 
    avg_fare = con.execute("""
        SELECT 
            segmentsCabinCode, 
            ROUND(AVG(totalFare),2) as avg_fare 
        FROM itineraries 
        WHERE startingAirport = ? 
            AND destinationAirport = ? 
            AND segmentsArrivalTimeRaw BETWEEN ? AND ? 
        GROUP BY ALL 
        ORDER BY 2
        """, [startingAirport, destinationAirport, slider_min, slider_max]).df()
    st.write('Average total fare by cabin type: ', avg_fare)

with col_b2:
    avg_time = con.execute("""
        SELECT 
            segmentsAirlineName, 
            AVG(DATE_DIFF('minute', segmentsDepartureTimeRaw::timestamp, segmentsArrivalTimeRaw::timestamp)) as avg_duration 
        FROM itineraries 
        WHERE startingAirport = ? 
            AND destinationAirport = ? 
            AND segmentsArrivalTimeRaw BETWEEN ? AND ? 
        GROUP BY ALL 
        ORDER BY 2
        """, [startingAirport, destinationAirport, slider_min, slider_max]).df()
    st.write('Average travel duration by airline company: ', avg_time)


st.subheader('Charts')

price_by_fareAndCarrier = con.execute("""
    SELECT 
        flightDate, 
        segmentsAirlineName as carrier, 
        avg(totalFare) as totalFare 
    FROM itineraries 
    WHERE startingAirport = ? 
        AND destinationAirport = ? 
        AND segmentsArrivalTimeRaw BETWEEN ? AND ? 
    GROUP BY ALL
    """, [startingAirport, destinationAirport, slider_min, slider_max]).df()

chart = alt.Chart(price_by_fareAndCarrier).mark_circle().encode(
    x = 'flightDate',
    y = 'totalFare',
    color = 'carrier'
).interactive()

st.altair_chart(chart, theme="streamlit", use_container_width=True)

seatsRemaining_df = con.execute("""
    SELECT 
        seatsRemaining, 
        ROUND(AVG(totalFare),2) AS totalFare
    FROM itineraries 
    WHERE startingAirport = ? 
        AND destinationAirport = ? 
        AND segmentsArrivalTimeRaw BETWEEN ? AND ? 
    GROUP BY ALL
    """, [startingAirport, destinationAirport, slider_min, slider_max]).df()

st.line_chart(data = seatsRemaining_df, x = 'seatsRemaining', y = 'totalFare')

end_timer = timer()

st.write("Total running time: ", end_timer-start_timer, " seconds")