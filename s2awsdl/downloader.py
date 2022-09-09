import datetime
from pathlib import Path
from typing import List, Union

import boto3
import xmltodict
from resens import io
from tqdm import tqdm

try:
    from osgeo import gdal
except ModuleNotFoundError:
    import gdal

S2_BANDS = [
    "B01", "B02", "B03", "B04", "B05",
    "B06", "B07", "B08", "B8A", "B09",
    "B10", "B11", "B12", "TCI"
    ]
BAND_RES = {
    10: [
    "B02", "B03", "B04", "B08", "TCI"
    ],
    20: [
    "B05", "B06", "B07", "B8A", "B11", "B12"
    ],
    60: [
    "B01", "B09", "B10"
    ]
}
S3_PREFIX = {
    "l1c": "sentinel-s2-l1c/tiles",
    "l2a": "sentinel-s2-l2a/tiles"
    }
S3_BUCKETS = {
    "l1c": "sentinel-s2-l1c",
    "l2a": "sentinel-s2-l2a"
    }
XML_HEADERS = {
    "l1c": "n1:Level-1C_Tile_ID",
    "l2a": "n1:Level-2A_Tile_ID"
}

class S2AWSDownloader:
    def __init__(
        self,
        access_keyid: str,
        secret_access_keyid: str,
        verbose=True
        ) -> None:
        
        self.access_keyid = access_keyid
        self.secret_access_keyid = secret_access_keyid
        self.verbose = verbose

        self.cloud_cov = 70
        self.nodata_cov = 10

        # some placeholders
        self.s3_client = None
        self.processing_level = None

        self._set_profiles()
    
    def _set_profiles(self) -> None:
        self.s3_client = boto3.client("s3",
                         aws_secret_access_key=self.secret_access_keyid,
                         aws_access_key_id=self.access_keyid)

        # gdal.SetConfigOption("AWS_REGION", "eu-central-1")
        gdal.SetConfigOption("AWS_SECRET_ACCESS_KEY", self.secret_access_keyid)
        gdal.SetConfigOption("AWS_ACCESS_KEY_ID", self.access_keyid)
        gdal.SetConfigOption("AWS_REQUEST_PAYER", "requester")

    def _get_keys(
        self, 
        bucket: str,
        prefix: str,
        requester_pays: bool = True
        ) -> str:
        """Function to get s3 objects from a bucket/prefix. 
        Optionally use a requester-pays header.

        Args:
            bucket (str): S3 bucket name
            prefix (str): S3 prefix
            requester_pays (bool, optional): Flag to enable use of requester payer header.
            Defaults to True.

        Yields:
            (str): File key
        """
        extra_kwargs = {}
        if requester_pays:
            extra_kwargs = {'RequestPayer': 'requester'}

        next_token = 'init'
        while next_token:
            kwargs = extra_kwargs.copy()
            if next_token != 'init':
                kwargs.update({'ContinuationToken': next_token})

            resp = self.s3_client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, **kwargs)

            try:
                next_token = resp['NextContinuationToken']
            except KeyError:
                next_token = None
            try:
                for contents in resp['Contents']:
                   key = contents['Key']

                   if any([key.endswith(i) for i in ["R10m/B02.jp2", "0/B02.jp2"]]):
                       yield key
                   else:
                       continue
            except KeyError:
                print(prefix)

    def search_s2l2a(
        self,
        tile: str,
        date_from: datetime.datetime,
        date_to: datetime.datetime,
        cloud_cov: int = 100,
        nodata_cov: int = 100,
        processing_level: str = "l2a"
        ) -> List:

        # De-construct tile id
        if tile.startswith("T"):
            tile = tile[1:]
        tileid1 = tile[:2]
        tileid2 = tile[2:3]
        tileid3 = tile[3:]

        # Check that a valid processing level has been used
        self.processing_level = processing_level.lower()
        if self.processing_level not in S3_BUCKETS:
            raise ValueError("Wrong processing level was passed. \
            Choose between L1C or L2A.")

        # dates
        dates = []  # to store sensing_dates

        # get list of dates between start and end
        n_days = (date_to - date_from).days
        search_dates = [date_from + datetime.timedelta(days=i) for i in range(n_days + 1)]

        for search_date in tqdm(search_dates):
            year = search_date.year
            mon = search_date.month
            day = search_date.day

            obj_key = f"tiles/{tileid1}/{tileid2}/{tileid3}/{year}/{mon}/{day}/0/metadata.xml"
            try:
                resp = self.s3_client.get_object(Bucket=S3_BUCKETS[self.processing_level],
                                        Key=obj_key, **{'RequestPayer': 'requester'})
            except:
                continue
                    
            parsed_xml = xmltodict.parse(resp["Body"].read())
            cloudy = float(
                parsed_xml[
                    XML_HEADERS[self.processing_level]
                ]['n1:Quality_Indicators_Info']['Image_Content_QI']['CLOUDY_PIXEL_PERCENTAGE']
                )
            if self.processing_level == "l2a":
                nodata = float(
                    parsed_xml[
                        XML_HEADERS[self.processing_level]
                    ]['n1:Quality_Indicators_Info']['Image_Content_QI']['NODATA_PIXEL_PERCENTAGE']
                    )
            else:
                nodata = 0

            if cloudy <= cloud_cov and nodata <= nodata_cov:
                dates.append(search_date)

        return dates
    
    def download_images(
        self,
        tile: str,
        dates: Union[datetime.datetime, List],
        output_dir: Union[str, Path],
        bands: List = S2_BANDS,
        resolution: int = None,
        download_cloud: bool = True,
        overwrite: bool = False
    ) -> None:

        # create output path
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True, parents=True)

        if isinstance(dates, (list, tuple)):
            path_dict = {}
            for date in dates:
                ret = self.download_images(tile, date, output_dir, bands, resolution)
                path_dict[date.strftime("%Y%m%d")] = ret
        else:
            path_dict = {}
            # De-construct tile id
            if tile.startswith("T"):
                tile = tile[1:]
            tileid1 = tile[:2]
            tileid2 = tile[2:3]
            tileid3 = tile[3:]

            output_date_dir = output_dir.joinpath(dates.strftime("%Y%m%d"))
            output_date_dir.mkdir(exist_ok=True, parents=True)

            # Construct S3 URI
            year = dates.year
            month = dates.month
            day = dates.day

            # Download the L2 cloud mask
            if download_cloud and self.processing_level == "l2a":
                im_s3_uri = f"/vsis3/{S3_PREFIX[self.processing_level]}/{tileid1}/{tileid2}/{tileid3}/{year}/{month}/{day}/0/qi/CLD_20m.jp2"
                
                output_fpath = output_date_dir.joinpath(f"T{tile}_{dates.strftime('%Y%m%d')}_{self.processing_level.upper()}_CLD.tif")
                if (output_fpath.exists() and overwrite) or not output_fpath.exists():
                    arr, transf, proj, _ = io.load_image(im_s3_uri)
                    io.write_image(arr, output_fpath.as_posix(), transf, proj)

                path_dict["CLD"] = output_fpath

            # Download each band
            for band in bands:
                if resolution:
                    band_res = max([resolution, [key for key in BAND_RES if band in BAND_RES[key]][0]])
                else:
                    band_res = [key for key in BAND_RES if band in BAND_RES[key]][0]

                if self.processing_level == "l2a":
                    im_s3_uri = f"/vsis3/{S3_PREFIX[self.processing_level]}/{tileid1}/{tileid2}/{tileid3}/{year}/{month}/{day}/0/R{band_res}m/{band}.jp2"
                else:
                    im_s3_uri = f"/vsis3/{S3_PREFIX[self.processing_level]}/{tileid1}/{tileid2}/{tileid3}/{year}/{month}/{day}/0/{band}.jp2"

                # Download and write image
                output_fpath = output_date_dir.joinpath(f"T{tile}_{dates.strftime('%Y%m%d')}_{self.processing_level.upper()}_{band}.tif")
                if (output_fpath.exists() and overwrite) or not output_fpath.exists():
                    arr, transf, proj, _ = io.load_image(im_s3_uri)
                    io.write_image(arr, output_fpath.as_posix(), transf, proj)
                
                path_dict[band] = output_fpath
            
        return path_dict
