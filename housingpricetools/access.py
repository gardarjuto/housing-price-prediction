from .config import *

import requests
import os

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def upload_local_file(conn, file_path, table_name):
    """Uploads a file from the local file system into the provided table."""
    with conn.curson() as crs:
        sql = f"""
            LOAD DATA LOCAL INFILE '{file_path}' INTO TABLE `{table_name}`
            FIELDS TERMINATED BY ','
            LINES STARTING BY '' TERMINATED BY '\n';
            """
        crs.execute(sql)


def download_file(url, dest_path, chunk_size=8192):
    """Downloads a file from URL to the destination path. Creates directories if output path doesn't exist."""
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)

    file_name = os.path.basename(url)
    file_path = os.path.join(dest_path, file_name)

    r = requests.get(url, stream=True)
    if r.ok:
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size):
                f.write(chunk)
    return file_name

