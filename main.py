import json
from email_downloader import EmailDownloaderService
from logger_config import setup_logger
import logging

def main():
    setup_logger()
    logger = logging.getLogger("Main")
    logger.info("Starting the application")
    
    with open("config.json", "r") as f:
        config = json.load(f)
    
    service = EmailDownloaderService(config)
    service.start()

if __name__ == "__main__":
    main()