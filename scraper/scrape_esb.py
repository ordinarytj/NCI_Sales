#!/usr/bin/env python3
"""
Configurable scraper for ESB website to download sales reports.
This script logs in, requests a report for a specific period, polls a queue until the report is ready,
and then downloads the generated Excel file.

Usage: python scrape_esb.py --config config.yaml
"""
import argparse
import time
import yaml
import logging
import os
import re
from typing import Dict, Any, Optional, Generator
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("esb_scraper")

# --- Core Scraping Logic ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def fetch(session: requests.Session, url: str, method: str = "GET", **kwargs) -> requests.Response:
    """Generic fetch function with retry logic."""
    logger.debug(f"Requesting {method} {url}")
    if method.upper() == "GET":
        r = session.get(url, **kwargs)
    else:
        r = session.post(url, **kwargs)
    r.raise_for_status()
    return r

def get_csrf_token(soup: BeautifulSoup) -> Optional[str]:
    """Extracts CSRF token from a BeautifulSoup object."""
    token_tag = soup.find("meta", {"name": "csrf-token"})
    return token_tag["content"] if token_tag else None

def login(session: requests.Session, config: Dict[str, Any]) -> bool:
    """Logs into the website and returns True on success."""
    login_url = config["urls"]["login"]
    creds = config["credentials"]
    
    try:
        logger.info(f"Fetching login page at {login_url}")
        resp_get = fetch(session, login_url, timeout=15)
        soup = BeautifulSoup(resp_get.text, "lxml")
        
        csrf_token = get_csrf_token(soup)
        if not csrf_token:
            logger.error("Could not find CSRF token on login page.")
            return False
        logger.info(f"Found CSRF token: {csrf_token[:10]}...")

        payload = {
            "_csrf-esb-fnb-backend": csrf_token,
            "LoginForm[username]": creds["username"],
            "LoginForm[password]": creds["password"],
        }
        
        logger.info(f"Attempting login for user '{creds['username']}'")
        resp_post = fetch(session, login_url, method="POST", data=payload, allow_redirects=True, timeout=20)

        if "site/login" in str(resp_post.url) or "login-form" in resp_post.text:
            logger.error("Login failed: redirected back to login page or form is still present.")
            return False

        logger.info("Login successful.")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred during login: {e}")
        return False

def generate_monthly_ranges(start_date_str: str, end_date_str: str) -> Generator[Dict[str, str], None, None]:
    """Yields dictionaries with start and end dates for each month in the range."""
    current_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    while current_date <= end_date:
        start_of_month = current_date.strftime("%d-%m-%Y")
        end_of_month_dt = current_date + relativedelta(months=1, days=-1)
        if end_of_month_dt > end_date:
            end_of_month_dt = end_date
        end_of_month = end_of_month_dt.strftime("%d-%m-%Y")
        
        yield {
            "start": start_of_month,
            "end": end_of_month,
            "label": current_date.strftime("%Y-%m")
        }
        current_date += relativedelta(months=1)

def request_report_generation(session: requests.Session, config: Dict[str, Any], date_range: Dict[str, str]) -> bool:
    """Sends a POST request to trigger the server-side report generation."""
    report_url = config["urls"]["report"]
    try:
        logger.info(f"Fetching report page for CSRF token for {date_range['label']}")
        resp_get = fetch(session, report_url, timeout=15)
        soup = BeautifulSoup(resp_get.text, "lxml")
        csrf_token = get_csrf_token(soup)
        if not csrf_token:
            logger.error("Could not find CSRF token on report page.")
            return False

        form_data = {
            "_csrf-esb-fnb-backend": csrf_token,
            "SalesReport[reportDate]": f"{date_range['start']} - {date_range['end']}",
            "SalesReport[dateFrom]": date_range['start'],
            "SalesReport[dateTo]": date_range['end'],
            "SalesReport[companyID][]": "",
            "SalesReport[branchID][]": "",
            "export-full-xls": "1",
        }

        logger.info(f"Requesting report generation for {date_range['label']}...")
        fetch(session, report_url, method="POST", data=form_data, timeout=30)
        logger.info("Report generation request sent successfully.")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to request report generation for {date_range['label']}: {e}")
        return False

def poll_report_queue(session: requests.Session, config: Dict[str, Any], date_range_label: str) -> Optional[str]:
    """Polls the report queue and returns the download URL when the report is ready."""
    queue_url = urljoin(config["urls"]["login"], "/site/get-data-report-queue")
    polling_config = config["polling_settings"]
    timeout = time.time() + polling_config["timeout_seconds"]
    
    logger.info(f"Polling report queue for a report matching '{date_range_label}'...")
    
    processed_ids = set()

    while time.time() < timeout:
        try:
            resp = fetch(session, queue_url, timeout=10)
            queue_data = resp.json()
            logger.debug(f"Received queue data: {queue_data}")
            
            # The queue data is a list of reports. We need to find the newest completed one.
            # The most reliable way is to find a completed report that we haven't processed yet.
            
            for item in queue_data.get("data", []):
                item_html_str = " ".join(item)
                soup = BeautifulSoup(item_html_str, "lxml")

                # Find the download link to get the ID
                download_tag = soup.find("a", href=re.compile(r"/site/download-queue\?id=(\d+)"))
                if not download_tag:
                    continue

                queue_id_match = re.search(r"id=(\d+)", download_tag["href"])
                if not queue_id_match:
                    continue
                
                queue_id = queue_id_match.group(1)

                # Check if we've already seen this completed report
                if queue_id in processed_ids:
                    continue

                # Check for completion status and if the name contains the month
                if "Completed" in item_html_str and date_range_label in item_html_str:
                    download_url = urljoin(config["urls"]["login"], download_tag["href"])
                    logger.info(f"Found completed report for {date_range_label} (ID: {queue_id}). URL: {download_url}")
                    return download_url
                
                # If it's completed but name doesn't match, just mark it as processed
                if "Completed" in item_html_str:
                    processed_ids.add(queue_id)

        except (requests.exceptions.RequestException, ValueError) as e:
            logger.warning(f"Could not poll report queue: {e}")
        
        logger.debug(f"Report not ready. Waiting {polling_config['interval_seconds']} seconds...")
        time.sleep(polling_config["interval_seconds"])

    logger.error(f"Timeout reached while waiting for report '{date_range_label}'.")
    return None

def download_file(session: requests.Session, url: str, output_dir: str):
    """Downloads a file from a URL and saves it to the output directory."""
    os.makedirs(output_dir, exist_ok=True)
    try:
        with fetch(session, url, stream=True, timeout=300) as r:
            content_disp = r.headers.get("content-disposition")
            fname_match = re.search('filename="(.+?)"', content_disp) if content_disp else None
            fname = fname_match.group(1) if fname_match else url.split("=")[-1] + ".xlsx"
            
            output_path = os.path.join(output_dir, fname)
            total_size = int(r.headers.get('content-length', 0))

            with open(output_path, "wb") as f, tqdm(
                desc=fname, total=total_size, unit="iB", unit_scale=True, unit_divisor=1024
            ) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    bar.update(size)
            logger.info(f"Successfully downloaded and saved report to {output_path}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download file from {url}: {e}")

def run_scraper(config_path: str):
    """Main function to orchestrate the scraping process."""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found at {config_path}")
        return
    
    with requests.Session() as session:
        session.headers.update({"User-Agent": config.get("user_agent", "ESB-Scraper/1.0")})

        if not login(session, config):
            logger.error("Stopping scraper due to login failure.")
            return

        scraping_params = config["scraping_parameters"]
        date_ranges = generate_monthly_ranges(
            scraping_params["start_date"], 
            scraping_params["end_date"]
        )
        
        rate_limit = float(config.get("rate_limit_seconds", 5.0))
        
        for date_range in date_ranges:
            if request_report_generation(session, config, date_range):
                download_url = poll_report_queue(session, config, date_range["label"])
                if download_url:
                    download_file(session, download_url, config["output"]["directory"])
                else:
                    logger.error(f"Could not retrieve download URL for report {date_range['label']}. Skipping.")
            
            logger.info(f"Waiting for {rate_limit} seconds before next request...")
            time.sleep(rate_limit)

    logger.info("Scraping process finished.")

def main():
    parser = argparse.ArgumentParser(description="ESB Sales Report Scraper")
    parser.add_argument("--config", required=True, help="Path to the YAML configuration file.")
    args = parser.parse_args()
    run_scraper(args.config)

if __name__ == "__main__":
    main()
