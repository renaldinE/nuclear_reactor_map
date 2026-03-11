import numpy as np
import pandas as pd
from os import listdir, chdir
from os.path import join

# Change base directory
chdir(r"C:/Users/renal/Documents/Generation Atomics/Nuclear reactor map")

fns = [f for f in listdir("pris_data")]

df = pd.read_excel(
    join("pris_data", fns[0]),
    skiprows=19
)

# Remove empty rows and repetitive headers
index_to_drop = df.index.values[(df['ISO Code'].isnull().values) | (df['ISO Code'] == "ISO Code")]
df.drop(
    index=index_to_drop,
    inplace=True
)

# Remove empty columns
column_to_drop = [col_name for col_name in df.columns if df[col_name].isnull().values.all()]
df.drop(
    columns=column_to_drop,
    inplace=True
)

