
#sudo pip install cogeo-mosaic --pre

import numpy as np
import pandas as pd
import xarray as xr
from affine import Affine
from pathlib import Path
import xarray
import matplotlib.pyplot as plt
from azure.storage.blob import ContainerClient
from os import path
from cogeo_mosaic.mosaic import MosaicJSON
from cogeo_mosaic.backends import MosaicBackend
import glob
import os

account_url = "https://deppcpublicstorage.blob.core.windows.net/output?sp=racwl&st=2022-04-03T23:17:37Z&se=2023-05-01T07:17:37Z&spr=https&sv=2020-08-04&sr=c&sig=wJkqOOZCPromubKaTzCAAY%2FvV5LJ7fIYHrbpwOJQDdk%3D"
container_client = ContainerClient.from_container_url(account_url)

name = "sipi"
mosaic_urls = []

#get online cog resources
blob_list = container_client.list_blobs(name_starts_with=name + '/')
for blob in blob_list:
    if blob.name.endswith(".tif"):
        url = "https://deppcpublicstorage.blob.core.windows.net/output/" + blob.name
        mosaic_urls.append(url)
        print(url)

#generate
json_file = "pacific-" + name + "-mosaic.json"
if os.path.exists(json_file): os.remove(json_file)
mosaicdata = MosaicJSON.from_urls(mosaic_urls)
with MosaicBackend(json_file, mosaic_def=mosaicdata) as mosaic:
    mosaic.write()

#upload mosiac json
with open(json_file, "rb") as blob_file:
        blob_name = "/".join([name, path.basename(json_file)])
        container_client.upload_blob(name=blob_name, data=blob_file, overwrite=True)

print("Finished.")

