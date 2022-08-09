import datetime
from pathlib import Path
from typing import List, Union

import boto3
import xmltodict
from resens.rasterlib import IO
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
    "10m": [
    "B02", "B03", "B04", "B08", "TCI"
    ],
    "20m": [
    "B05", "B06", "B07", "B8A", "B11", "B12"
    ],
    "60m": [
    "B01", "B09", "B10"
    ]
}
L2_PREFIX = "sentinel-s2-l2a/tiles"

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
        self.l2_prefix = "sentinel-s2-l2a/tiles"

        # some placeholders
        self.s3_client = None

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

                   if key.endswith("R10m/B02.jp2"):
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
        ) -> List:

        # De-construtct tile id
        if tile.startswith("T"):
            tile = tile[1:]
        tileid1 = tile[:2]
        tileid2 = tile[2:3]
        tileid3 = tile[3:]

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
                resp = self.s3_client.get_object(Bucket="sentinel-s2-l2a",
                                        Key=obj_key, **{'RequestPayer': 'requester'})
            except:
                continue
                    
            parsed_xml = xmltodict.parse(resp["Body"].read())
            cloudy = float(
                parsed_xml['n1:Level-2A_Tile_ID']['n1:Quality_Indicators_Info']['Image_Content_QI']['CLOUDY_PIXEL_PERCENTAGE']
                )
            nodata = float(
                parsed_xml['n1:Level-2A_Tile_ID']['n1:Quality_Indicators_Info']['Image_Content_QI']['NODATA_PIXEL_PERCENTAGE']
                )

            if cloudy <= cloud_cov and nodata <= nodata_cov:
                dates.append(search_date)

        return dates
    
    def download_images(
        self,
        tile: str,
        dates: Union[datetime.datetime, List],
        output_dir: Union[str, Path],
        bands: List = S2_BANDS,
    ) -> None:

        # create output path
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True, parents=True)

        if isinstance(dates, (list, tuple)):
            for date in dates:
                self.download_images(tile, date, output_dir, bands)
        else:
            # De-construtct tile id
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

            for band in bands:
                band_res = [key for key in BAND_RES if band in BAND_RES[key]][0]
                im_s3_uri = f"/vsis3/{L2_PREFIX}/{tileid1}/{tileid2}/{tileid3}/{year}/{month}/{day}/0/R{band_res}/{band}.jp2"

                # Download and write image
                arr, transf, proj, _ = IO().load_image(im_s3_uri)

                IO().write_image(arr, output_date_dir.joinpath(band).as_posix(), transf, proj)
