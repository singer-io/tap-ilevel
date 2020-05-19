import unittest
import tap_ilevel
import singer
import singer.metadata
from singer.schema import Schema
from datetime import datetime

try:
    import tests.test_utils as test_utils
except ImportError:
    import utils as utils

LOGGER = singer.get_logger()

SINGER_MESSAGES = []

def accumulate_singer_messages(message):
    SINGER_MESSAGES.append(message)

singer.write_message = accumulate_singer_messages

class TestSync(unittest.TestCase):

    def setUp(self):
        self.state = {}

    def test_date_split(self):
        start_date = datetime.strptime('2020-01-01', '%Y-%m-%d')
        end_date = datetime.strptime('2020-05-01', '%Y-%m-%d')

