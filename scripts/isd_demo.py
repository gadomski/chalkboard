import dask
from dask.distributed import Client
import dask.dataframe.multi
import os
import isd
import isd.pandas
import sys

if len(sys.argv) != 2:
    raise Exception("Invalid number of arguments")

client = Client(sys.argv[1])
outdir = "isd/parquet"


def read_to_data_frame(path):
    with isd.open(path) as iterator:
        records = list(iterator)
    return isd.pandas.data_frame(records)


data_frames = []
for file_name in os.listdir("isd/2020"):
    path = os.path.join("isd/2020", file_name)
    data_frames.append(dask.delayed(read_to_data_frame)(path))

data_frame = dask.dataframe.from_delayed(data_frames)
data_frame.to_parquet(outdir, partition_on=["year", "month"])
