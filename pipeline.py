import string
import gzip
import os
import re

import requests
import pandas as pd
import concurrent.futures
from loguru import logger

import config
from models.site_maps import SiteMapsDigikala

sitemap_url = "https://www.digikala.com/sitemap.xml"


def process_sitemap_file(extracted_file_path):
    try:
        sitemaps_df = pd.read_xml(extracted_file_path)
        logger.info(f"saved to dataframe: {extracted_file_path}")
        return sitemaps_df
    except Exception as e:
        logger.error(f"Failed to process file: {extracted_file_path}. Error: {e}")
        return None



def download_and_extract_gz(url, destination_folder):
    try:
        gz_url_response = requests.get(url)
        gz_url_response.raise_for_status()  # Raise an exception for 4xx and 5xx errors

        filename = url.split("/")[-1]
        filepath = os.path.join(destination_folder, filename)
        extracted_file_path = filepath.replace('.gz', '')

        # Check if the extracted file already exists in the destination folder
        if os.path.exists(extracted_file_path):
            logger.info(f"Skipping download and extraction. File already exists: {extracted_file_path}")
            return extracted_file_path

        # Check if the .gz file already exists in the destination folder
        if os.path.exists(filepath):
            logger.info(f"Skipping download. File already exists: {filename}")
            return extracted_file_path

        with open(filepath, 'wb') as f:
            f.write(gz_url_response.content)

        with gzip.open(filepath, 'rb') as f_in:
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


def read_sitemap_and_save_to_db(dataframe_batch):
    try:
        dataframe_batch = dataframe_batch.drop(columns=['image'], errors='ignore')
        records = dataframe_batch.to_dict(orient='records')
        num_records = len(records)
        SiteMapsDigikala.insert_many(records).execute()
        logger.info(f"Saved {num_records} records to the database.")
    except Exception as e:
        logger.error(f"Failed to save data to the database. Error: {e}")


def main():
    try:
        get_site_map = requests.get(sitemap_url)
        get_site_map.raise_for_status()
        sitemap_xml = get_site_map.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download sitemap.xml. Error: {e}")
        sitemap_xml = ""

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

    # Create an empty DataFrame to store the combined sitemaps
    combined_sitemaps_df = pd.DataFrame()


    # Read the contents of each sitemap file into a Pandas DataFrame
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Submit tasks to read and process sitemap files
        futures = [executor.submit(process_sitemap_file, extracted_file_path) for extracted_file_path in extracted_file_paths]

        # Combine the results into a single DataFrame
        combined_sitemaps_df = pd.concat([future.result() for future in concurrent.futures.as_completed(futures)], ignore_index=True)

    # Remove duplicates based on 'loc' column
    combined_sitemaps_df.drop_duplicates(subset='loc', inplace=True)


    # Save data in batches of 10,000 records
    batch_size = 10000
    for batch_start in range(0, len(combined_sitemaps_df), batch_size):
        dataframe_batch = combined_sitemaps_df.iloc[batch_start:batch_start + batch_size]
        read_sitemap_and_save_to_db(dataframe_batch)


if __name__ == "__main__":
    main()
