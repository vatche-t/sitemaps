import string
import gzip
import os
import re

import requests
import pandas as pd
from loguru import logger

import config
from models.site_maps import SiteMapsDigikala

sitemap_url = "https://www.digikala.com/sitemap.xml"

def download_and_extract_gz(url, destination_folder):
    try:
        gz_url_response = requests.get(url)
        gz_url_response.raise_for_status()  # Raise an exception for 4xx and 5xx errors

        filename = url.split("/")[-1]
        filepath = os.path.join(destination_folder, filename)

        # Check if the file already exists in the destination folder
        if os.path.exists(filepath):
            logger.info(f"Skipping download. File already exists: {filename}")
            return filepath

        with open(filepath, 'wb') as f:
            f.write(gz_url_response.content)

        with gzip.open(filepath, 'rb') as f_in:
            extracted_file_path = filepath.replace('.gz', '')
            with open(extracted_file_path, 'wb') as f_out:
                # Filter out non-printable characters and write the cleaned content
                cleaned_content = ''.join(filter(lambda x: x in string.printable, f_in.read().decode()))
                f_out.write(cleaned_content.encode())

        # Check if the extracted file is in valid XML format
        try:
            pd.read_xml(extracted_file_path)
        except Exception as e:
            logger.warning(f"Skipping processing. Invalid XML format: {extracted_file_path}. Error: {e}")
            os.remove(extracted_file_path)
            return None

        os.remove(filepath)
        return extracted_file_path

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download: {url}. Error: {e}")
        return None

def read_sitemap_and_save_to_db(sitemap_file):
    try:
        sitemaps_df = pd.read_xml(sitemap_file)

        # Convert NaN values in 'image' column to None to match the database schema

        # Iterate through the DataFrame and save each row to the database
        for _, row in sitemaps_df.iterrows():
            # Check if the 'loc' value already exists in the database
            existing_loc_record = SiteMapsDigikala.select().where(SiteMapsDigikala.loc == row['loc']).first()
            if not existing_loc_record:
                SiteMapsDigikala.create(
                    loc=row['loc'],
                    changefreq=row['changefreq'],
                    priority=row['priority'],
                )
            else:
                logger.info(f"Skipping duplicate record: {row['loc']}")

        logger.info(f"Data from {sitemap_file} saved to the database.")
    except Exception as e:
        logger.error(f"Failed to process {sitemap_file}. Error: {e}")

def main():
    try:
        get_site_map = requests.get(sitemap_url)
        get_site_map.raise_for_status()  
        sitemap_xml = get_site_map.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download sitemap.xml. Error: {e}")
        sitemap_xml = ""

    # Find all the .gz URLs in the sitemap.xml
    gz_urls = re.findall(r'<loc>(.*?)</loc>', sitemap_xml)

    # Destination folder to store the extracted files
    destination_folder = "sitemap_files"
    os.makedirs(destination_folder, exist_ok=True)

    # Download and extract each .gz file
    extracted_file_paths = []
    for i, gz_url in enumerate(gz_urls):
        extracted_file_path = download_and_extract_gz(gz_url, destination_folder)
        if extracted_file_path:
            extracted_file_paths.append(extracted_file_path)
            logger.info(f"Extracted: {extracted_file_path}")

    # Read the contents of each sitemap file into a Pandas DataFrame
    for i, extracted_file_path in enumerate(extracted_file_paths):
        try:
            read_sitemap_and_save_to_db(extracted_file_path)
        except Exception as e:
            logger.error(f"Failed to process DataFrame {i}. Error: {e}")

if __name__ == "__main__":
    main()
