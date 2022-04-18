"""
Compute SIPI for the AOI Sentinel-2 images, saving to Blob Storage.
"""
import io, os
import pystac_client
import stackstac
import numpy as np
import xarray as xr
import planetary_computer as pc
import geopandas as gpd
import json
import rasterio.features
from pystac.extensions.projection import ProjectionExtension as proj
import azure.storage.blob
from azure.storage.blob import ContainerClient
import rioxarray  # noqa
import xrspatial.multispectral as ms
from xrspatial.focal import mean
from dask_gateway import GatewayCluster
from dask.distributed import Client, progress


def main():
    print("Starting Job...")

    #dask cluster 
    cluster = GatewayCluster()  # Creates the Dask Scheduler. Might take a minute.
    client = cluster.get_client()
    cluster.adapt(minimum=4, maximum=100)
    print(cluster.dashboard_link)
    #print(f"/proxy/{client.scheduler_info()['services']['dashboard']}/status")

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1"
    )

    #area of interest
    country_name = "Tonga"
    df = gpd.read_file('pic_bounding_box.geojson')
    country = df.loc[df['NAME'] == country_name]
    area_of_interest = json.loads(country.geometry.to_json())['features'][0]['geometry']
    
    search = catalog.search(
        intersects=area_of_interest,
        datetime="2022-01-01/2022-12-30",
        collections=["sentinel-2-l2a"],
        limit=100,  # fetch items in batches of 500
        query={"eo:cloud_cover": {"lt": 20}}, #cloudcover_percentage
    )

    #sort by cloud cover and select first 
    raw_items = search.get_items()
    #raw_items = sorted(raw_items, key=lambda item: item.properties['eo:cloud_cover'])[0:1]

    #sign
    items = [pc.sign(item).to_dict() for item in raw_items]

    item = next(search.get_items())
    epsg = proj.ext(item).epsg

    ds = (
        stackstac.stack(
            items,
            epsg=epsg,
            resolution=50, #resolution/m
            assets=["B02", "B04", "B08"],  # blue, red, nir
            chunksize=8192, #4096, #256,  # set chunk size to 256 to get one chunk per time step
    )
        .where(lambda x: x > 0, other=np.nan)  # sentinel-2 uses 0 as nodata
        .assign_coords(
            band=lambda x: x.common_name.rename("band"),  # use common names            
        )
    )#.squeeze()

    ds = ds.resample(time="1Y").median("time", keep_attrs=True)

    print("Computing SIPI : " + country_name)

    #indicies calculation
    sipi_aggs = [ms.sipi(x.sel(band="nir"), x.sel(band="red"), x.sel(band="blue")) for x in ds]
    
    #smooth
    mean_aggs = [mean(sipi_agg) for sipi_agg in sipi_aggs]
    sipi = xr.concat(mean_aggs, dim="time")

    #generate and upload
    account_url = "https://deppcpublicstorage.blob.core.windows.net/output?sp=racwl&st=2022-04-03T23:17:37Z&se=2023-05-01T07:17:37Z&spr=https&sv=2020-08-04&sr=c&sig=wJkqOOZCPromubKaTzCAAY%2FvV5LJ7fIYHrbpwOJQDdk%3D"
    container_client = ContainerClient.from_container_url(account_url)

    print("Creating COG GeoTiff...")
    country_name = country_name.lower()
    sipi.rio.to_raster("sipi_{country_name}_50m.tif", driver="COG")
    with open("sipi_{country_name}_50m.tif", "rb") as f:
        container_client.upload_blob(
            f"/sipi/{country_name}_50m.tif", f, overwrite=True
        )

    
    print("Completed.")


if __name__ == "__main__":
    main()

