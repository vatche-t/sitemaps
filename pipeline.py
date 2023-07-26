import hashlib
import gzip
import json
import os
import re

import requests
import pandas as pd
import numpy as np
import concurrent.futures
from loguru import logger
from fake_useragent import UserAgent

import config
from models.site_maps import SiteMapsDigikala


DEFAULT_SITEMAP_URLS = [
    "/sitemap-index.xml",
    "/sitemap.php",
    "/sitemap.txt",
    "/sitemap.xml.gz",
    "/sitemap/",
    "/sitemap/sitemap.xml",
    "/sitemapindex.xml",
    "/sitemap/index.xml",
    "/sitemap1.xml",
]


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
        logger.info("sitemap URLs found in robots.txt.")

        # If no sitemap URLs found, use the default sitemap URLs
        if not sitemap_urls:
            logger.warning("No sitemap URLs found in robots.txt. Using default sitemap URLs.")
            sitemap_urls = [f"{url.rstrip('/')}{default_url}" for default_url in DEFAULT_SITEMAP_URLS]

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


def generate_unique_filename(url):
    # Create a unique file name by hashing the URL
    url_hash = hashlib.md5(url.encode()).hexdigest()
    return f"{url_hash}.xml"


def download_and_extract_gz(url, destination_folder):
    ua = UserAgent()
    headers = {"User-Agent": ua.random}  # Select a random user-agent for each request
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        if response.headers.get("content-type") == "application/json" and "error" in response.text.lower():
            error_data = json.loads(response.text)
            if (
                "error" in error_data
                and "message" in error_data["error"]
                and "forbidden" in error_data["error"]["message"].lower()
            ):
                logger.warning(f"URL is forbidden: {url}")
                return None

        if url.endswith(".gz"):
            try:
                decompressed_content = gzip.decompress(response.content)
                # Convert bytes to a string using UTF-8 encoding
                sitemap_xml = decompressed_content.decode("utf-8")
            except Exception as e:
                logger.warning(f"Skipping processing. Error while decompressing: {url}. Error: {e}")
                return None
        else:
            sitemap_xml = response.text

        # Check if the decompressed/decoded content is in valid XML format
        try:
            pd.read_xml(sitemap_xml)
        except Exception as e:
            logger.warning(f"Skipping processing. Invalid XML format: {url}. Error: {e}")
            return None

        return sitemap_xml

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download: {url}. Error: {e}")
        return None


def read_sitemap_and_save_to_db(dataframe_batch):
    try:
        # Select only the 'loc' and 'priority' columns if they exist in the dataframe
        required_columns = ["sitemap_url", "loc", "priority"]
        dataframe_batch = dataframe_batch.reindex(columns=required_columns)

        # Check if all required columns are present
        missing_columns = [col for col in required_columns if col not in dataframe_batch.columns]
        if missing_columns:
            logger.warning(f"Missing columns in dataframe_batch: {missing_columns}")
            # Fill missing columns with NaN values
            for col in missing_columns:
                dataframe_batch[col] = np.nan

        records = dataframe_batch.to_dict(orient="records")
        num_records = len(records)
        SiteMapsDigikala.insert_many(records).execute()
        logger.info(f"Saved {num_records} records to the database.")
    except Exception as e:
        logger.error(f"Failed to save data to the database. Error: {e}")


def is_xml_or_gz_url(url):
    return url.endswith(".xml") or url.endswith(".gz")


def is_gz_url(url):
    return url.endswith(".gz")


def process_nested_sitemaps(xml_content, extracted_file_paths):
    # Find all <loc> tags in the XML content
    ua = UserAgent()
    headers = {"User-Agent": ua.random}
    destination_folder = "sitemap_files"
    loc_tags = re.findall(r"<loc>(.*?)</loc>", xml_content)

    # Lists to store URLs with .xml/.gz and nested sitemaps respectively
    xml_or_gz_urls = []
    nested_sitemaps = []

    # Separate .xml/.gz URLs and nested sitemaps
    for loc_tag in loc_tags:
        if is_xml_or_gz_url(loc_tag):
            xml_or_gz_urls.append(loc_tag)
        else:
            nested_sitemaps.append(loc_tag)

    has_gz_urls = any(is_gz_url(url) for url in loc_tags)

    if has_gz_urls:
        for loc_tag in loc_tags:
            if is_xml_or_gz_url(loc_tag):
                extracted_file_path = download_and_extract_gz(loc_tag, destination_folder)
                if extracted_file_path:
                    extracted_file_paths.append(extracted_file_path)
                    logger.info(f"Extracted: {loc_tag}")

    # Loop through .xml/.gz URLs and download them
    for url in xml_or_gz_urls:
        get_site_map_raw = requests.get(url, headers=headers)
        get_site_map_raw.raise_for_status()

        # Check if the response contains a forbidden message
        if "forbidden" in get_site_map_raw.text.lower():
            logger.warning(f"URL is forbidden: {url}")
            continue

        # If the response is valid, proceed with processing the sitemap
        sitemap_xml = get_site_map_raw.text

        loc_tags_raw = re.findall(r"<loc>(.*?)</loc>", sitemap_xml)

        has_xml_urls = any(is_xml_or_gz_url(url) for url in loc_tags_raw)

        if has_xml_urls:
            # If there are .xml URLs in loc_tags_raw, process them individually
            for loc_tag_raw in loc_tags_raw:
                if is_xml_or_gz_url(loc_tag_raw):
                    extracted_file_path = download_and_extract_gz(loc_tag_raw, destination_folder)
                    if extracted_file_path:
                        extracted_file_paths.append(extracted_file_path)
                        logger.info(f"Extracted: {loc_tag_raw}")
        else:
            # If there are no .xml URLs in loc_tags_raw, process the original URL
            extracted_file_path = download_and_extract_gz(get_site_map_raw.url, destination_folder)
            if extracted_file_path:
                extracted_file_paths.append(extracted_file_path)
                logger.info(f"Extracted: {get_site_map_raw.url}")

    # Process nested sitemaps recursively
    for nested_sitemap_url in nested_sitemaps:
        nested_sitemap = requests.get(nested_sitemap_url, headers=headers)
        if nested_sitemap.status_code == 200:
            process_nested_sitemaps(nested_sitemap.text, extracted_file_paths)


def main():
    ua = UserAgent()
    headers = {"User-Agent": ua.random}  # Select a random user-agent for each request
    url = "https://www.basalam.com"
    sitemap_urls = get_sitemap_urls_from_robots_txt(url, headers)

    # Loop through each sitemap URL and attempt to process it
    for sitemap_url in sitemap_urls:
        try:
            # Send a request to the current sitemap URL
            get_site_map = requests.get(sitemap_url, headers=headers)
            get_site_map.raise_for_status()

            # Check if the response contains a forbidden message
            if "forbidden" in get_site_map.text.lower():
                logger.warning(f"URL is forbidden: {sitemap_url}")
                continue

            # If the response is valid, proceed with processing the sitemap
            sitemap_xml = get_site_map.text

            # Destination folder to store the extracted files
            destination_folder = "sitemap_files"
            os.makedirs(destination_folder, exist_ok=True)

            # Process nested sitemaps
            extracted_file_paths = []
            process_nested_sitemaps(sitemap_xml, extracted_file_paths)

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

            # Exit the loop if a valid sitemap response is found
            break

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download sitemap.xml. Error: {e}")
            continue


if __name__ == "__main__":
    main()
