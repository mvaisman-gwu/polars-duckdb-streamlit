#%%
from urllib.request import urlretrieve

url = "https://georgetown.box.com/shared/static/7vdzo9fzwds2o9oc1tmlvoz2l3aeqh1l"
local_filename = "data/expedia-itineraries.snappy.parquet"
urlretrieve(url, local_filename)


url = "https://app.box.com/index.php?rm=box_download_shared_file&shared_name=1t8rcuj8285ev1sk2rvbbmp91i8bib0q&file_id=f_1648595084523"
local_filename = "data/lineitemsf1.snappy.parquet"
urlretrieve(url, local_filename)
