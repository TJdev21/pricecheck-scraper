import hydra
import asyncio
from omegaconf import DictConfig
from product_scraper_poc.scraper import scrape_all_categories, save_to_excel_multiple_sheets
from loguru import logger

@hydra.main(config_path="config", config_name="config", version_base=None)
def main(cfg: DictConfig):
    logger.info("Starting the product scraping process...")

    loop = asyncio.get_event_loop()
    # Scraping all product categories and their product data asynchronously
    category_data = loop.run_until_complete(scrape_all_categories(cfg.scraper.base_url, cfg.scraper.selectors))

    if category_data:
        save_to_excel_multiple_sheets(category_data, cfg.scraper.output_file) 
    else:
        logger.error("No data scraped. Exiting the process.")

if __name__ == "__main__":
    main()

