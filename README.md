# Sitemap Scraper

This Python application is designed to scrape sitemap data from websites and store it in a PostgreSQL database. The tool is developed using Python 3.10 and utilizes Peewee as the ORM for database operations. It scrapes sitemap URLs from a given website, downloads XML and GZipped files, processes nested sitemaps, and saves the data into a PostgreSQL database.

## Prerequisites

- **Python 3.10:** Make sure you have Python 3.10 installed. You can download it from the [official Python website](https://www.python.org/downloads/).

- **Virtual Environment (venv):** Create a virtual environment to manage project dependencies.
  ```bash
  python3 -m venv venv
  source venv/bin/activate  # On Windows, use venv\Scripts\activate
  ```

- **PostgreSQL Database:** Create a `.env` file in the project root to store your PostgreSQL database connection details.
  ```plaintext
  DATABASE_URL=postgresql://username:password@localhost:5432/database_name
  ```

- **Change Website URL:** Open `pipeline.py` and change the `EXAMPLE_URL` variable to the desired website.

- **Install Dependencies:** Install project dependencies using `requirements.txt`.
  ```bash
  pip install -r requirements.txt
  ```

## Project Structure

- **`config.py`:** Configuration file containing constants and settings for the application.

- **`models/`:** Directory containing Peewee models for database schema.

- **`sitemap_files/`:** Directory where downloaded sitemap files are stored.

- **`pipeline.py`:** Python script that handles scraping and processing sitemaps. Change the `EXAMPLE_URL` variable to the desired website.

- **`main.py`:** Main Python script that orchestrates the scraping process.

## How to Use

1. **Activate Virtual Environment:**
   ```bash
   source venv/bin/activate  # On Windows, use venv\Scripts\activate
   ```

2. **Database Setup:**
   - Create a PostgreSQL database and update the `.env` file with your database connection details.

3. **Change Website URL:**
   Open `pipeline.py` and change the `EXAMPLE_URL` variable to the desired website.

4. **Run the Scraper:**
   ```bash
   python main.py
   ```
   The scraper will start processing sitemap URLs from the specified website. If there are no sitemap URLs on the website, it will be logged using Loguru logger.

## Additional Information

- **Python Version:** 3.10

- **Database:** PostgreSQL

- **Dependencies:** Check `requirements.txt` for the list of Python packages used in this project.

- **Notes:** Ensure proper network connectivity, and permissions to access the website's sitemaps. If you encounter a 403 Forbidden error, check the website's `robots.txt` file for sitemap access restrictions.



Last updated on: 2024-02-13

Last updated on: 2024-02-14

Last updated on: 2024-02-15

Last updated on: 2024-02-16

Last updated on: 2024-02-21