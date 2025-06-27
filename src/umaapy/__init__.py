import os, logging, sys

from umaapy.util.dds_configurator import DDSConfigurator
from umaapy.util.event_processor import EventProcessor


def setup_logging(level="INFO", log_file=None):
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(root.level)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    root.addHandler(ch)

    # Optional file handler
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setLevel(root.level)
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        root.addHandler(fh)


setup_logging(level="DEBUG")

DOMAIN_ID = int(os.getenv("DOMAIN_ID", "0"))
QOS_FILE = os.getenv("QOS_FILE", "/workspace/umaapy/src/umaapy/resource/umaapy_qos_lib.xml")

configurator = DDSConfigurator(DOMAIN_ID, QOS_FILE)
event_processor = EventProcessor()
