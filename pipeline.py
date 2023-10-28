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
from models.site_maps import SiteMap


DEFAULT_SITEMAP_URLS = [
    "/sitemap.xml",
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


def get_sitemap_urls_from_robots_txt(url):
    try:
        ua = UserAgent()
        headers = {"User-Agent": ua.random}
        robots_response = requests.get(
            f"{url.rstrip('/')}/robots.txt", allow_redirects=False, headers=headers, timeout=15
        )

        # Check if the response is a redirect
        if robots_response.is_redirect:
            redirect_url = robots_response.headers["Location"]
            # Now you can make another request to the redirect URL if needed
            robots_response = requests.get(redirect_url, headers=headers)

        robots_txt = robots_response.text

        # Use regular expression to find all the Sitemap URLs
        sitemap_urls = re.findall(r"(?i)sitemap:\s*(.*)", robots_txt)
        logger.info("sitemap URLs found in robots.txt.")

        # If no sitemap URLs found, use the default sitemap URLs
        if not sitemap_urls:
            logger.warning("No sitemap URLs found in robots.txt. Using default sitemap URLs.")
            sitemap_urls = []

        # Additional list to hold final sitemap URLs
        final_sitemap_urls = [f"{url.rstrip('/')}{default_url}" for default_url in DEFAULT_SITEMAP_URLS]

        # Always extend sitemap_urls with final_sitemap_urls
        sitemap_urls.extend(final_sitemap_urls)

        return sitemap_urls

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download robots.txt. Error: {e}")
        # If connection failed, return sitemap_urls including the final_sitemap_urls
        final_sitemap_urls = [f"{url.rstrip('/')}{default_url}" for default_url in DEFAULT_SITEMAP_URLS]
        return final_sitemap_urls


def process_sitemap_file(extracted_xmls, sitemap_url):
    try:
        sitemaps_dataframe = pd.read_xml(extracted_xmls)
        sitemaps_dataframe["sitemap_url"] = sitemap_url
        logger.info(f"saved to dataframe: {sitemap_url}")
        return sitemaps_dataframe
    except Exception as e:
        logger.error(f"Failed to process file: {extracted_xmls}. Error: {e}")
        return None


def download_and_extract_gz(url, destination_folder):
    ua = UserAgent()
    headers = {"User-Agent": ua.random}  # Select a random user-agent for each request
    try:
        if url.endswith(".xml") or url.endswith(".gz"):
            try:
                xml_response = requests.get(url, headers=headers, allow_redirects=False)
                xml_response.raise_for_status()

                if (
                    xml_response.headers.get("content-type") == "application/json"
                    and "error" in xml_response.text.lower()
                ):
                    error_data = json.loads(xml_response.text)
                    if (
                        "error" in error_data
                        and "message" in error_data["error"]
                        and "forbidden" in error_data["error"]["message"].lower()
                    ):
                        logger.warning(f"URL is forbidden: {url}")
                        return None

                if xml_response.url.endswith(".xml.gz"):
                    try:
                        sitemap_xml = xml_response.text
                    except Exception as e:
                        logger.warning(f"Skipping processing. Error while decompressing: {url}. Error: {e}")
                        return None

                elif url.endswith(".gz"):
                    try:
                        decompressed_content = gzip.decompress(xml_response.content)
                        # Convert bytes to a string using UTF-8 encoding
                        sitemap_xml = decompressed_content.decode("utf-8")
                    except Exception as e:
                        logger.warning(f"Skipping processing. Error while decompressing: {url}. Error: {e}")
                        return None
                else:
                    sitemap_xml = xml_response.text

                # Check if the decompressed/decoded content is in valid XML format
                try:
                    pd.read_xml(sitemap_xml)
                except Exception as e:
                    logger.warning(f"Skipping processing. Invalid XML format: {url}. Error: {e}")
                    return None

                return sitemap_xml

            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to download: {url}. Error: {e}")
        else:
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


def process_nested_sitemaps(xml_content, extracted_xmls):
    # Find all <loc> tags in the XML content
    ua = UserAgent()
    headers = {"User-Agent": ua.random}
    destination_folder = "sitemap_files"
    if xml_content.endswith(".xml"):
        loc_tags_urls = re.findall(r"<loc>(.*?)</loc>", xml_content)

        if not loc_tags_urls:
            # If loc_tags is empty, skip processing and use xml_content as loc_tags
            loc_tags_urls = [xml_content]
    else:
        loc_tags_urls = [xml_content]

    # Lists to store URLs with .xml/.gz and nested sitemaps respectively
    xml_or_gz_urls = []
    nested_sitemaps = []

    # Separate .xml/.gz URLs and nested sitemaps
    for loc_tag_url in loc_tags_urls:
        if is_xml_or_gz_url(loc_tag_url):
            xml_or_gz_urls.append(loc_tag_url)
        else:
            nested_sitemaps.append(loc_tag_url)

    has_gz_urls = any(is_gz_url(url) for url in loc_tags_urls)

    if has_gz_urls:
        for loc_tag_url in loc_tags_urls:
            if is_xml_or_gz_url(loc_tag_url):
                extracted_xml = download_and_extract_gz(loc_tag_url, destination_folder)
                if extracted_xml:
                    extracted_xmls.append(extracted_xml)
                    logger.info(f"Extracted: {loc_tag_url}")

    # Loop through .xml/.gz URLs and download them
    for url in xml_or_gz_urls:
        get_site_map = requests.get(url, headers=headers)
        get_site_map.raise_for_status()

        # Check if the response contains a forbidden message
        if "forbidden" in get_site_map.text.lower():
            logger.warning(f"URL is forbidden: {url}")
            continue

        # If the response is valid, proceed with processing the sitemap
        sitemap_xml = get_site_map.text

        loc_urls = re.findall(r"<loc>(.*?)</loc>", sitemap_xml)

        has_xml_urls = any(is_xml_or_gz_url(url) for url in loc_urls)

        if has_xml_urls:
            # If there are .xml URLs in loc_tags_raw, process them individually
            for loc_url in loc_urls:
                if is_xml_or_gz_url(loc_url):
                    extracted_xml = download_and_extract_gz(loc_url, destination_folder)
                    if extracted_xml:
                        extracted_xmls.append(extracted_xml)
                        logger.info(f"Extracted: {loc_url}")
        else:
            # If there are no .xml URLs in loc_tags_raw, process the original URL
            extracted_xml = download_and_extract_gz(get_site_map.url, destination_folder)
            if extracted_xml:
                extracted_xmls.append(extracted_xml)
                logger.info(f"Extracted: {get_site_map.url}")

    # Process nested sitemaps recursively
    for nested_sitemap_url in nested_sitemaps:
        nested_sitemap = requests.get(nested_sitemap_url, headers=headers)
        if nested_sitemap.status_code == 200:
            process_nested_sitemaps(nested_sitemap.text, extracted_xmls)


def main():
    ua = UserAgent()
    headers = {"User-Agent": ua.random}  # Select a random user-agent for each request
    url = "https://www.example.com"
    sitemap_urls = get_sitemap_urls_from_robots_txt(url)

    # Initialize an empty list to store all extracted file paths
    extracted_xmls = []

    # Loop through each sitemap URL and attempt to process it
    for sitemap_url in sitemap_urls:
        try:
            get_site_map = requests.get(sitemap_url, headers=headers, allow_redirects=False)
            get_site_map.raise_for_status()

            if "forbidden" in get_site_map.text.lower():
                logger.warning(f"URL is forbidden: {sitemap_url}")
                continue

            sitemap_xml = get_site_map.text

            destination_folder = "sitemap_files"
            os.makedirs(destination_folder, exist_ok=True)
            loc_tags_urls = re.findall(r"<loc>(.*?)</loc>", sitemap_xml)

            xml_or_gz_urls = []
            for loc_tag_url in loc_tags_urls:
                if is_xml_or_gz_url(loc_tag_url):
                    xml_or_gz_urls.append(loc_tag_url)

            has_xml_urls = any(is_xml_or_gz_url(url) for url in loc_tags_urls)

            if has_xml_urls:
                # Process each URL in xml_or_gz_urls and accumulate extracted files
                for loc_tag_url in xml_or_gz_urls:
                    process_nested_sitemaps(loc_tag_url, extracted_xmls)
            else:
                extracted_file_path = sitemap_xml
                if extracted_file_path:
                    extracted_xmls.append(extracted_file_path)
                    logger.info(f"Extracted: {get_site_map.url}")

            # Create an empty DataFrame to store the combined sitemaps
            combined_sitemaps_df = pd.DataFrame()

            # Read the contents of each extracted sitemap file into a Pandas DataFrame
            with concurrent.futures.ProcessPoolExecutor() as executor:
                # Submit tasks to read and process sitemap files
                futures = [
                    executor.submit(process_sitemap_file, extracted_xml, sitemap_url)
                    for extracted_xml in extracted_xmls
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

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download sitemap.xml. url: {sitemap_url}  Error: {e}")
            continue


if __name__ == "__main__":
    main()
