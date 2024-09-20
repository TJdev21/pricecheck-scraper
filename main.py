import hydra
import asyncio
from omegaconf import DictConfig
from product_scraper_poc.scraper import scrape_data, save_to_excel
from loguru import logger


@hydra.main(config_path="config", config_name="config", version_base=None)
def main(cfg: DictConfig):
    logger.info("Starting the product scraping process...")

    loop = asyncio.get_event_loop()
    data = loop.run_until_complete( scrape_data(cfg.scraper.base_url, cfg.scraper.selectors))

    if not data.empty:
        save_to_excel(data, cfg.scraper.output_file)
    else:
        logger.error("No data scraped. Exiting the process.")


if __name__ == "__main__":
    main()
