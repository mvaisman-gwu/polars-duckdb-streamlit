#%%

import holoviews as hv
import hvplot.polars
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
import seaborn as sns
import time

#%%
# Create a dictionary of names and num_occurrences

data = {
    "name": ["Japhy", "Arthur", "Rusty","Alan"],
    "num_occurrences": [3, 2, None, 9]
}


#%%


# Create a pandas dataframe

df_pd = pd.DataFrame(data)
df_pd



#%%

# Create a Polars dataframe

df_pl = pl.DataFrame(data)
df_pl


# ## Key syntax is consistent across both Pandas and Polars

# Use of `head()`, `shape`, and `dtypes` is identical in both Pandas and Polars.

#%%


# show top row of pandas dataframe

df_pd.head(1)





# show top row of polars dataframe

df_pl.head(1)





# show shape of pandas dataframe

df_pd.shape





# show shape of polars dataframe

df_pl.shape





# show datatypes of pandas dataframe

df_pd.dtypes





# show datatypes of polars dataframe

df_pl.dtypes





# use describe() on pandas dataframe

df_pd['num_occurrences'].describe()





# use describe() on polars dataframe

df_pl['num_occurrences'].describe()







diamonds = sns.load_dataset('diamonds')







# assemble the pandas dataframe

df_pd = pd.DataFrame(diamonds)
df_pd

# assemble the polars dataframe

df_pl = pl.DataFrame(diamonds)
df_pl

# subset pandas columns

df_pd[['cut','price','clarity']]


#subset polars columns

df_pl[['cut','price','clarity']]


# use select() on polars dataframe

df_pl.select('cut','price','clarity')

# Now, assemble the DataFrame in Polars

# filter pandas dataframe

df_pd[df_pd['price'] > 1000]





# filter polars dataframe

df_pl.filter(df_pl['price'] > 1000)





# sort pandas dataframe

df_pd.sort_values('price')





# sort polars dataframe

df_pl.sort('price')





# Find the price for the 5th record in our dataset.

print(df_pl[4,6])
print(df_pl[4,"price"])


#%%


#%%


# create pandas dataframe and add a column

df_pd = pd.DataFrame(diamonds)
df_pd['USD_Euro_conversion'] = 0.92
df_pd


#%%


# calculate a new column

df_pd['price_euro'] = df_pd['price'] * df_pd['USD_Euro_conversion']
df_pd


#%%


# create polars dataframe and add a column

df_pl = pl.DataFrame(diamonds)
df_pl = df_pl.with_columns(pl.lit(0.92).alias('USD_Euro_conversion'))


#%%


# calculate a new column

df_pl = df_pl.with_columns((df_pl['price'] * df_pl['USD_Euro_conversion']).alias('price_euro'))
df_pl


#%%


# drop a column from pandas dataframe

df_pd = df_pd.drop(columns=['USD_Euro_conversion'])
df_pd


#%%


# drop a column from polars dataframe

df_pl = df_pl.drop('USD_Euro_conversion')
df_pl


#%%


# rename polars column name

df_pl = df_pl.rename({'price_euro': 'price_in_euros'})
df_pl


#%%


# change column data type

df_pl.with_columns(pl.col("carat").cast(pl.Float32))




#%%




# Load dataset
planets = sns.load_dataset('planets')
planets.shape


#%%


# Check for null values

planets.isnull().sum()


#%%


# pandas
df_pd = pd.DataFrame(planets)

# polars
df_pl = pl.DataFrame(planets)


#%%


# fillna in pandas

df_pd['mass'] = df_pd['mass'].fillna(df_pd['mass'].mean())


#%%


# fill null in polars

df_pl = df_pl.with_columns(pl.col('mass').fill_null(strategy="mean"))

# Note strategy options: {None, ‘forward’, ‘backward’, ‘min’, ‘max’, ‘mean’, ‘zero’, ‘one’}

# show null values

df_pl.select(pl.all().is_null().sum())


#%%


# pandas drop na

df_pd_2 = df_pd.dropna()
df_pd_2


#%%


# polars drop null

df_pl_2 = df_pl.drop_nulls()
df_pl_2


#%%


# polars drop null select column

df_pl_3 = df_pl.drop_nulls('orbital_period')
df_pl_3


#%%







#%%


# Load dataset
flights = sns.load_dataset('flights')
flights


#%%


# create flights in pandas and group by year

df_pd = pd.DataFrame(flights)

grouped_pd = df_pd.groupby('year')['passengers'].mean()
grouped_pd


#%%


# create flights in pandas and group by year

df_pl = pl.DataFrame(flights)

grouped_pl = df_pl.groupby('year').agg(pl.col('passengers').mean())
grouped_pl


#%%


# compute rolling average in polars

rolling_avg_pl = df_pl.select([
    pl.col('passengers').rolling_mean(window_size=12).alias('rolling_year_avg_passengers')
])

rolling_avg_pl








# Load the Diamonds dataset
diamonds = sns.load_dataset('diamonds')
df_pl = pl.DataFrame(diamonds)





# benchmark in polars eager mode

start_time = time.time()

eager_result = df_pl.filter(pl.col('carat') > 1) \
                    .group_by('cut') \
                    .agg(avg_price=pl.col('price').mean())

end_time = time.time()





# show execution time

print(f"Eager Execution Time: {end_time - start_time} seconds")





# create lazyframe

lazy_df = df_pl.lazy()





# benchmark in polars lazy mode

start_time = time.time()

lazy_result = (lazy_df.filter(pl.col('carat') > 1)
                      .group_by('cut')
                      .agg(avg_price=pl.col('price').mean())
                      .collect())

end_time = time.time()

print(f"Lazy Execution Time: {end_time - start_time} seconds")


#%%







#%%




# simulate a large dataset

large_df_pd = pd.DataFrame({'A': range(1000000), 'B': range(1000000)})

print(large_df_pd.info(memory_usage='deep'))


#%%




# create a similar dataset in polars

large_df_pl = pl.DataFrame({'A': range(1000000), 'B': range(1000000)})
print(large_df_pl.estimated_size("mb"))


#%%


# create as a lazyframe

lazy_df = pl.DataFrame({'A': range(1000000), 'B': range(1000000)}).lazy()


#%%


# apply a filter operation

lazy_df = lazy_df.filter(pl.col('A') > 500000)


#%%


# collect result and print esimated size

result = lazy_df.collect()
print(result.estimated_size("mb"))




#%%



# create a big dataframe in pandas

big_df = pd.DataFrame({
    'id': np.arange(10000000),
    'value': np.random.rand(10000000)
})

# output to csv

big_df.to_csv('data/big_data.csv', index=False)

# show top rows

big_df.head()


#%%



# Benchmark pandas performance

start_time = time.time()

pd_df = pd.read_csv('data/big_data.csv')

end_time = time.time()

print(f"Pandas read_csv: {end_time - start_time} seconds")


#%%




# Benchmark polars performance

start_time = time.time()

pl_df = pl.read_csv('data/big_data.csv')

end_time = time.time()

print(f"Polars read_csv: {end_time - start_time} seconds")


#%%


# benchmark method chaining in pandas

start_time = time.time()

filtered_pandas = pd_df[pd_df['value'] > 0.5].groupby('id').sum()

end_time = time.time()

print(f"Pandas method chaining: {end_time - start_time} seconds")


#%%


# benchmark method chaining in polars

start_time = time.time()

filtered_polars = pl_df.filter(pl.col('value') > 0.5).group_by('id').agg(pl.sum('value'))

end_time = time.time()

print(f"Polars method chaining: {end_time - start_time} seconds")


#%%


# bechmark column transformation in pandas

start_time = time.time()

# column multiplication
pd_df['transformed'] = pd_df['value'].apply(lambda x: x * 2 if x > 0.5 else x)

end_time = time.time()

print(f"Pandas column transformation: {end_time - start_time} seconds")


#%%


# benchmark column transformation in polars eager mode

start_time = time.time()

# column multiplication
df_polars = pl_df.with_columns(pl.col('value').map_elements(lambda x: x * 2 if x > 0.5 else x))


end_time = time.time()

print(f"Polars column transformation: {end_time - start_time} seconds")


#%%


# benchmark column transformation in polars eager mode

start_time = time.time()

# scan_csv to load as a lazyframe
pl_df_lazy = pl.scan_csv("data/big_data.csv").lazy()

# use map_batches for column multiplication
df_transformed = pl_df_lazy.with_columns(
    pl.col("value").map_batches(lambda x: x * 2 if x > 0.5 else x).alias("transformed")
)

end_time = time.time()

print(f"Polars apply in lazy mode: {end_time - start_time} seconds")




#%%




# Load the Diamonds dataset from Seaborn
diamonds = sns.load_dataset('diamonds')


#%%


# load diamonds as a polars dataframe

pl_diamonds = pl.DataFrame(diamonds)

# export diamonds as a csv

pl_diamonds.write_csv('data/diamonds.csv')


#%%


# read diamonds data with scan_csv

lazy_df = pl.scan_csv('data/diamonds.csv')


#%%


# queue up transformations and aggregations on the lazyframe

transformed_lazy_df = (lazy_df
                       .filter(pl.col("price") > 500)
                       .group_by("cut")
                       .agg([
                           pl.col("price").mean().alias("avg_price"),
                           pl.col("carat").max().alias("max_carat")
                       ])
                      )

transformed_lazy_df.collect()


#%%


# transform clarity into a dummy variable

dummy_df = lazy_df.collect().to_dummies('clarity')
dummy_df


#%%







#%%




# Load the Flights dataset from Seaborn into a Polars dataframe
flights = sns.load_dataset('flights')
df_flights = pl.DataFrame(flights)
df_flights


#%%


# Manual mapping of month abbreviations to numbers
month_map = {
    "Jan": 1, "Feb": 2, "Mar": 3,
    "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9,
    "Oct": 10, "Nov": 11, "Dec": 12
}

# Apply the mapping to the month column to convert abbreviations to numbers
df_flights_with_month_num = df_flights.with_columns(
    pl.col("month").map_elements(lambda x: month_map[x]).alias("month_num")
)

# Create the date column by combining year, month_num, and a fixed day (assuming the first day of the month)
df_flights_with_date = df_flights_with_month_num.with_columns(
    pl.datetime("year", "month_num", pl.lit(1)).alias("date")
)

df_flights_with_date


#%%


# Calculate a rolling average of passengers
rolling_avg_passengers = df_flights_with_date.with_columns(
    pl.col("passengers").rolling_mean(window_size=3).alias("rolling_avg_passengers")
)

rolling_avg_passengers


#%%











# Load the Tips data from seaborn as a polars dataframe

tips_df = pl.from_pandas(sns.load_dataset('tips'))





# load tips as a lazyframe

lazy_tips = tips_df.lazy()

# transform tips data

filtered_avg_tip = (lazy_tips.filter(pl.col("total_bill") > 10)
                              .select([pl.col("tip").mean().alias("average_tip")]))

filtered_avg_tip





# collect the result

filtered_avg_tip.collect()





# install hvplot

#!pip install hvplot





get_ipython().run_line_magic('env', 'HV_DOC_HTML=true')



hv.extension('bokeh')

# visualize bill amounts from Tips using hvplot as a histogram

plot = lazy_tips.hvplot.hist("tip", bins=30, title="Distribution of Tip Amounts")
plot





# show the correlation between tip and total_bill

correlation = lazy_tips.select(pl.corr("tip", "total_bill")).collect()
print(f"Correlation between total_bill and tip: {correlation}")




#%%


# Load the Flights data as a polars dataframe

flights = sns.load_dataset('flights')
pl_flights = pl.DataFrame(flights)
pl_flights


#%%


# Calculate a rolling 3-month average of passengers

pl_flights = pl_flights.with_columns(
    pl.col("passengers").rolling_mean(window_size=3).alias("rolling_3m_avg_passengers")
)

pl_flights


#%%


# convert to a pandas dataframe

pd_flights = pl_flights.to_pandas()
pd_flights


#%%



# melt the dataframe to align with seaborn format

pd_flights_melted = pd_flights.melt(id_vars=["year", "month"],
                                    value_vars=["passengers", "rolling_3m_avg_passengers"],
                                    var_name="Type", value_name="Number of Passengers")

# create line plot with seaborn

plt.figure(figsize=(10, 6))
sns.lineplot(data=pd_flights_melted, x="year", y="Number of Passengers", hue="Type", style="Type")

plt.title('Monthly Passengers vs. Rolling 3-Month Average')
plt.show()


#%%


# convert back to polars dataframe from pandas

pl_flights_from_pandas = pl.from_pandas(pd_flights)
pl_flights_from_pandas




#%%



# Create an eager dataframe about fish

df_fish = pl.DataFrame({
    "fish": ["Salmon", "Trout", "Catfish", "Pike", "Sturgeon"],
    "length_cm": [70, 50, 120, 85, 200],
    "weight_kg": [5, 2, 15, 8, 20]
})

# convert to a lazyframe

lazy_fish = df_fish.lazy()


#%%


# perform series of operations on lazyframe

lazy_fish_transformed = (
    lazy_fish
    .filter(pl.col("length_cm") > 80)
    .select([
        pl.col("fish"),
        pl.col("weight_kg").mean().alias("average_weight_kg")
    ])
)


#%%


# explain query plan

lazy_fish_transformed.explain()


#%%


# streaming = true

lazy_fish_transformed.explain(streaming=True)


#%%


# visualize the query plan as a graph

lazy_fish_transformed.show_graph()
