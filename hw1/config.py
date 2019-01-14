import configparser
import logging

logger = logging.getLogger(__name__)

# Configuration file with parameters for scraping
CONFIG_FILE = 'config/config.ini'

config = configparser.ConfigParser()
config.read(CONFIG_FILE)

if len(config.sections()) == 0:
    logger.error('Config file not found')
    raise FileNotFoundError
