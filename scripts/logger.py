import logging

# --------------------------------------------------
# LOGGING CONFIGURATION
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("data_quality.log"),
        logging.StreamHandler()
    ]
)

def logger():
    logger = logging.getLogger("DataQualityGuard")

