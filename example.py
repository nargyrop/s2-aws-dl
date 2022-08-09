import argparse
import json
from datetime import datetime
from pathlib import Path

from s2awsdl.downloader import S2AWSDownloader

if __name__ == "__main__":
    # run from the cmd like python example.py --help
    parser = argparse.ArgumentParser("Sentinel-2 L2A Band Downloader")
    parser.add_argument(
        "--tile-id", 
        help="Sentinel-2 tile id (e.g. T35VNH).",
        required=True,
        type=str
        )
    parser.add_argument(
        "--start-date", 
        help="Start date for search in YYYY-mm-dd format.",
        required=True,
        type=str
        )
    parser.add_argument(
        "--end-date", 
        help="End date for search in YYYY-mm-dd format.",
        required=True,
        type=str
        )
    parser.add_argument(
        "--output-path",
        help="Path to directory where images will be downloaded.",
        required=True,
        type=str
        )
    parser.add_argument(
        "--access-key",
        help="AWS access key ID.",
        required=True,
        type=str
        )
    parser.add_argument(
        "--secret-access-key",
        help="AWS secret access key ID.",
        required=True,
        type=str
        )
    
    args = parser.parse_args()

    tileid = args.tile_id
    start_date = args.start_date
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = args.end_date
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    output_path = args.output_path

    s2dl = S2AWSDownloader(
        access_keyid=args.access_key,
        secret_access_keyid=args.secret_access_key
    )

    dates = s2dl.search_s2l2a(
        tile=tileid,
        date_from=start_date,
        date_to=end_date
    )

    s2dl.download_images(
        tile=tileid,
        dates=dates,
        output_dir=output_path,
        bands=["B08", "B8A", "B12", "TCI"]
    )
