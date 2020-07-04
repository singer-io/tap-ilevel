import unittest
import os

from tap_tester import connections, menagerie, runner

class BaseTapTest(unittest.TestCase):

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-ilevel"
