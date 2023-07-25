import string
import gzip
import os
import re

import requests
import pandas as pd
import concurrent.futures
from loguru import logger
from fake_useragent import UserAgent

import config
from models.site_maps import SiteMapsDigikala


def get_sitemap_urls_from_robots_txt(url, headers):
    try:
        robots_response = requests.get(f"{url.rstrip('/')}/robots.txt", allow_redirects=False, headers=headers)

        # Check if the response is a redirect
        if robots_response.is_redirect:
            redirect_url = robots_response.headers["Location"]
            # Now you can make another request to the redirect URL if needed
            robots_response = requests.get(redirect_url, headers=headers)

        robots_txt = robots_response.text

        # Use regular expression to find all the Sitemap URLs
        sitemap_urls = re.findall(r"Sitemap:\s*(.*)", robots_txt)

        return sitemap_urls

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download robots.txt. Error: {e}")
        return []


def process_sitemap_file(extracted_file_path, sitemap_url):
    try:
        sitemaps_df = pd.read_xml(extracted_file_path)
        sitemaps_df["sitemap_url"] = sitemap_url
        logger.info(f"saved to dataframe: {extracted_file_path}")
        return sitemaps_df
    except Exception as e:
        logger.error(f"Failed to process file: {extracted_file_path}. Error: {e}")
        return None


def download_and_extract_gz(url, destination_folder):
    ua = UserAgent()
    headers = {"User-Agent": ua.random}  # Select a random user-agent for each request
    try:
        filename = url.split("/")[-1]
        filepath = os.path.join(destination_folder, filename)

        # Check if the file already exists in the destination folder
        # if os.path.exists(filepath):
        #     logger.info(f"Skipping download. File already exists: {filename}")
        #     return filepath

        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        # Check if the content is gzip-compressed
        if response.headers.get("Content-Encoding") == "gzip":
            with gzip.GzipFile(fileobj=response.raw) as f_in:
                with open(filepath, "wb") as f_out:
                    # Write the binary content directly to the file
                    f_out.write(f_in.read())
        else:
            # Save the content directly to a file
            with open(filepath, "wb") as f_out:
                # Write the binary content directly to the file
                f_out.write(response.content)

        # Check if the extracted file is in valid XML format
        try:
            pd.read_xml(filepath)
        except Exception as e:
            logger.warning(f"Skipping processing. Invalid XML format: {filepath}. Error: {e}")
            os.remove(filepath)
            return None

        return filepath

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download: {url}. Error: {e}")
        return None


def read_sitemap_and_save_to_db(dataframe_batch):
    try:
        # Select only the 'loc' and 'priority' columns
        dataframe_batch = dataframe_batch.loc[:, ["sitemap_url", "loc", "priority"]]

        records = dataframe_batch.to_dict(orient="records")
        num_records = len(records)
        SiteMapsDigikala.insert_many(records).execute()
        logger.info(f"Saved {num_records} records to the database.")
    except Exception as e:
        logger.error(f"Failed to save data to the database. Error: {e}")


def main():
    ua = UserAgent()
    headers = {"User-Agent": ua.random}  # Select a random user-agent for each request
    url = "https://www.sheypoor.com"
    sitemap_urls = get_sitemap_urls_from_robots_txt(url, headers)
    for sitemap_url in sitemap_urls:
        try:
            get_site_map = requests.get(sitemap_url, headers=headers)
            get_site_map.raise_for_status()
            sitemap_xml = get_site_map.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download sitemap.xml. Error: {e}")
            sitemap_xml = ""

        gz_urls = re.findall(r"<loc>(.*?)</loc>", sitemap_xml)

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
            futures = [
                executor.submit(process_sitemap_file, extracted_file_path, sitemap_url)
                for extracted_file_path in extracted_file_paths
            ]

            # Combine the results into a single DataFrame
            combined_sitemaps_df = pd.concat(
                [future.result() for future in concurrent.futures.as_completed(futures)], ignore_index=True
            )

        # Remove duplicates based on 'loc' column
        combined_sitemaps_df.drop_duplicates(subset="loc", inplace=True)

        # Save data in batches of 10,000 records
        batch_size = 10000
        for batch_start in range(0, len(combined_sitemaps_df), batch_size):
            dataframe_batch = combined_sitemaps_df.iloc[batch_start : batch_start + batch_size]
            read_sitemap_and_save_to_db(dataframe_batch)


if __name__ == "__main__":
    main()
