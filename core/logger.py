import logging
import logging.handlers
from pathlib import Path

log_dir = Path("log")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "log.log"
console_file = log_dir / "console.log"

rotating_handler = logging.handlers.RotatingFileHandler(
    log_file,
    mode='a',
    maxBytes=2*1024*1024,
    backupCount=5,
    encoding='utf-8'
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

# File handler for console.log (captures console output)
console_file_handler = logging.FileHandler(console_file, mode='a', encoding='utf-8')
console_file_handler.setLevel(logging.INFO)
console_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

# Suppress httpx logs only in the console
class HttpxFilter(logging.Filter):
    def filter(self, record):
        return not record.name.startswith("httpx")

console_handler.addFilter(HttpxFilter())
console_file_handler.addFilter(HttpxFilter())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        rotating_handler,
        console_handler,
        console_file_handler
    ]
)

def get_logger(name: str = __name__):
    return logging.getLogger(name)

# copy paste
# from core.logger import get_logger
# logger = get_logger(__name__)