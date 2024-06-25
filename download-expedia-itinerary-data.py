#%%
from urllib.request import urlretrieve

url = "https://georgetown.box.com/shared/static/7vdzo9fzwds2o9oc1tmlvoz2l3aeqh1l"
local_filename = "data/expedia-itineraries.snappy.parquet"
urlretrieve(url, local_filename)
