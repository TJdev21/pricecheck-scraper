import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from loguru import logger
import os


"""Asynchronously get Webpages
    Returns: The HTML content of webpage """
async def fetch_page(session, url):
    async with session.get(url) as response:
        if response.status != 200:
            logger.error(f"Failed to fetch page: {url}, status code: {response.status}")
            return None
        return await response.text()
    
"""Asynchronously extract main product categories from the dropdown
    Returns a list of dictionaries where each dictionary contains the category name and its URL"""
async def fetch_categories(base_url: str, selectors: dict, session) -> list:
    logger.info("Fetching main categories from the homepage...")
    html = await fetch_page(session, base_url)
    if html is None:
        logger.error("Failed to fetch categories.")
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    
    categories = soup.select(selectors['category_container'])
    
    category_urls = []
    for category in categories:
        try:
            #if len(category_urls)<=1:

                category_name = category.select_one('span').text.strip()
                category_url = category.select_one('a')['href']
                if not category_url.startswith('http'):
                    category_url = os.path.join(base_url, category_url)

                category_urls.append({'name': category_name, 'url': category_url})

        except Exception as e:
            logger.warning(f"Error extracting category URL: {e}")

    logger.info(f"Found {len(category_urls)} main categories.")
    return category_urls

"""Asynchronously scrape all categories and their products data
    Return a dictionary where 
    keys - category names
    values - Pandas DataFrames with product data"""
async def scrape_all_categories(base_url: str, selectors: dict) -> dict:
    async with aiohttp.ClientSession() as session:
        categories = await fetch_categories(base_url, selectors, session)
        all_category_data = {}

        for category in categories:
            logger.info(f"Scraping category: {category['name']}")
            products_df = await scrape_data(category['url'], selectors)
            all_category_data[category['name']] = products_df

        return all_category_data

"""Asynchronously scrape all the product data from each category page through pagination iteration"""
async def scrape_listing_page(base_url: str, selectors: dict, session):
    all_product_details = []
    current_page_url = base_url
    current_page = 1

    while current_page_url:
        logger.info(f"Scraping page {current_page}: {current_page_url}")

 
        html = await fetch_page(session, current_page_url)
        if html is None:
            break


        soup = BeautifulSoup(html, 'html.parser')


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


        next_page_link = soup.select_one(selectors['next_page'])
        if next_page_link:
            current_page_url = next_page_link['href']
            current_page += 1
        else:
            logger.info("No more pages found. Scraping completed.")
            current_page_url = None  

    return all_product_details

"""Asynchronously scrape product details from each products page
    Returns a dictionary containing the detailed information from individual product pages"""
async def scrape_product_details(product_url: str, session) -> dict:
    logger.info(f"Scraping product details from {product_url}")
    
    html = await fetch_page(session, product_url)
    soup = BeautifulSoup(html, 'html.parser')
   
    product_data = {}
    table = soup.find('table', class_='table table-bordered mt-3')

    if table:
        
        rows = table.find_all('tr')
        for row in rows:
            columns = row.find_all('td')
            if len(columns) == 2:
                
                header_text = columns[0].get_text(strip=True).replace(':', '')
                value_text = columns[1].get_text(strip=True)
                product_data[header_text] = value_text
    else:
        logger.warning(f"No product details table found for {product_url}")

    return product_data


"""Scrape product data from category listing page and product detail pages asynchronously.
    Returns pd.DataFrame"""
async def scrape_data(base_url: str, selectors: dict) -> pd.DataFrame:
    async with aiohttp.ClientSession() as session:
        product_details_list = await scrape_listing_page(base_url, selectors, session)
        all_products_data = []


        tasks = []
        for product in product_details_list:
            tasks.append(scrape_product_details(product['Product URL'], session))

        product_details = await asyncio.gather(*tasks)


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


"""Save data to an Excel file with multiple sheets """
def save_to_excel_multiple_sheets(category_data: dict, file_path: str):
    
    output_dir = os.path.join(os.getcwd(), os.path.dirname(file_path))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created directory: {output_dir}")

    try:
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            for category, data in category_data.items():
                if not data.empty:

                    sheet_name = category[:31]
                    logger.info(f"Writing data for category: {sheet_name}")
                    data.to_excel(writer, sheet_name=sheet_name, index=False)
        logger.info(f"Data saved to {file_path} with {len(category_data)} sheets.")
    except Exception as e:
        logger.error(f"Error saving data to Excel: {e}")
        raise e  

