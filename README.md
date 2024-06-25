# Beyond `Pandas`

In this session, you will learn about newer tools that allow you to work more efficiently with data and also process large datasets much faster.

- [Polars](https://pola.rs/): a new dataframe library written in the [Rust programming language](https://www.rust-lang.org/)
- [DuckDB](https://duckdb.org/): a portable and lightweight analytical SQL database with great Python integration, and blazingly fast
- [Apache Parquet](https://en.wikipedia.org/wiki/Apache_Parquet): a column-oriented data storage format (on disk) that provides efficient data compression, metadata, and partitioning capabilities
- [Apache Arrow](https://arrow.apache.org/): the language-independent columnar memory format 
- [Streamlit](https://streamlit.io/): great for building dashboards with Python and turning data scripts into shareable web apps

## Python libraries introduced

- `polars`: the library for Polars dataframes
- `duckdb`: the DuckDB client library
- `streamlit`: the Streamlit library
- `pyarrow`: the Arrow and Parquet interface
- `parquet-tools`: a command-line tool to interact with Parquet files (since they're binary)


## Instructions

1. Clone this repository to your local machine
2. Open VSCode in the repo directory
3. Create a Python virtual environment and install the libraries in [requirements.txt](requirements.txt)

## Introduction

- Open [from-pandas-to-polars.py](from-pandas-to-polars.py) for an introduction comparing `pandas` and `polars`.
- Open [duckdb-and-arrow.py](duckdb-and-arrow.py) to learn about DuckDB and Arrow

## Retrieving data from DuckDB

- `fetchnumpy()` fetches the data as a dictionary of Numpy arrays
- `df()` brings the data as a Pandas Dataframe
- `arrow()` fetches the data as an Arrow table
- `pl()` pulls the data as a Polars Dataframe.

Ultimately, choosing the best method that suits your needs is up to you. **As a best practice, do not use fetch_one or fetch_all as they create a Python object for every value returned by your query.** If you have a lot of data, you can have a memory error or poor performance.


## Benchmark

1. Generate a one Billion row dataset by running the [generate-1brow-dataset.py](generate-1brow-dataset.py) script.
2. Run the individual files in the `benchmark-scripts` directory to see the performance of polars, duckdb, and pandas. **run `using-pure-python.py` last.**


## Build a Streamlit dashboard with DuckDB and Streamlit

1. Download the data for the dashboard by running the [download-expedia-itinerary-data.py](download-expedia-itinerary-data.py)
2. Open [app.py](app.py) to explore the code for the streamlit app
3. Run: `streamlit run app.py` in the terminal, and that will open the app in a browser

## Sources and references

Content adapted from:

- https://www.blog.pythonlibrary.org/2024/05/06/how-to-read-and-write-parquet-files-with-python/
- https://duckdb.org/2021/12/03/duck-arrow.html
- https://github.com/gabriel-garciae/one_billion_row_challenge_python
- https://medium.com/@octavianzarzu/build-and-deploy-apps-with-duckdb-and-streamlit-in-under-one-hour-852cd31cccce
- https://github.com/octavianzarzu/flight-prices-streamlit-app-1
- https://github.com/gabriel-garciae/one_billion_row_challenge_python
