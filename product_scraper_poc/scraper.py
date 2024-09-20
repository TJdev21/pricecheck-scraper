import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from loguru import logger
import os

async def fetch_page(session, url):
    """Asynchronously fetch a page."""
    async with session.get(url) as response:
        if response.status != 200:
            logger.error(f"Failed to fetch page: {url}, status code: {response.status}")
            return None
        return await response.text()
    

async def scrape_listing_page(base_url: str, selectors: dict, session):
    """Asynchronously scrape product data from the listing page and handle pagination."""
    all_product_details = []
    current_page_url = base_url
    current_page = 1

    while current_page_url:
        logger.info(f"Scraping page {current_page}: {current_page_url}")

        # Fetch the page asynchronously
        html = await fetch_page(session, current_page_url)
        if html is None:
            break

        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')

        # Extract product details
        products = soup.select(selectors['product_container'])
        for product in products:
            try:
                title_element = product.select_one(selectors['product_title'])
                title = title_element.text.strip() if title_element else 'N/A'
                image_element = product.select_one(selectors['product_image'])
                image_url = image_element['src'] if image_element else 'N/A'
                product_url = title_element['href'] if title_element else 'N/A'

                all_product_details.append({
                    'Title': title,
                    'Image URL': image_url,
                    'Product URL': product_url
                })
            except Exception as e:
                logger.warning(f"Error extracting data for a product: {e}")

        logger.info(f"Scraped {len(products)} products from page {current_page}")

        # Find the "Next" page link
        next_page_link = soup.select_one(selectors['next_page'])
        if next_page_link:
            current_page_url = next_page_link['href']
            current_page += 1
        else:
            logger.info("No more pages found. Scraping completed.")
            current_page_url = None  # Stop the loop

    return all_product_details

# This is your working scrape_product_details function
async def scrape_product_details(product_url: str, session) -> dict:
    """Asynchronously scrape product details from each product's page."""
    logger.info(f"Scraping product details from {product_url}")

    # Fetch the product page asynchronously
    html = await fetch_page(session, product_url)
    soup = BeautifulSoup(html, 'html.parser')

    # Extract data from the product details table
    product_data = {}
    table = soup.find('table', class_='table table-bordered mt-3')

    if table:
        # Find all rows in the table
        rows = table.find_all('tr')
        for row in rows:
            columns = row.find_all('td')
            if len(columns) == 2:
                # Extract header and value
                header_text = columns[0].get_text(strip=True).replace(':', '')
                value_text = columns[1].get_text(strip=True)
                product_data[header_text] = value_text
            else:
                logger.warning(f"Unexpected number of <td> elements in row: {row}")
    else:
        logger.warning(f"No product details table found for {product_url}")

    return product_data

async def scrape_data(base_url: str, selectors: dict) -> pd.DataFrame:
    """Scrape data asynchronously."""
    async with aiohttp.ClientSession() as session:
        product_details_list = await scrape_listing_page(base_url, selectors, session)
        all_products_data = []

        # Fetch all product details asynchronously
        tasks = []
        for product in product_details_list:
            tasks.append(scrape_product_details(product['Product URL'], session))

        product_details = await asyncio.gather(*tasks)

        # Combine product data with general information
        for product, details in zip(product_details_list, product_details):
            if details:
                details.update({
                    'Title': product['Title'],
                    'Image URL': product['Image URL'],
                    'Product URL': product['Product URL']
                })
                all_products_data.append(details)
            else:
                logger.warning(f"Skipping {product['Title']} as no details were extracted.")

        df = pd.DataFrame(all_products_data)
        logger.info(f"Scraping completed. Extracted data for {len(df)} products.")
        return df

def save_to_excel(data: pd.DataFrame, file_path: str):
    """Save DataFrame to an Excel file."""
    output_dir = os.path.join(os.getcwd(), os.path.dirname(file_path))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created directory: {output_dir}")

    try:
        data.to_excel(file_path, index=False)
        logger.info(f"Data saved to {file_path}")
    except Exception as e:
        logger.error(f"Error saving data to Excel: {e}")
