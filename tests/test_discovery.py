from tap_tester import menagerie
from tap_tester.scenario import SCENARIOS
import re
from base import BaseTapTest

class DiscoveryTest(BaseTapTest):

    def name(self):
        return "tap_tester_ilevel_discovery_test"

    def do_test(self, conn_id):
        """
         Verify number of actual streams discovered match expected
        """

        found_catalogs = menagerie.get_catalogs(conn_id)

        self.assertGreater(len(found_catalogs), 0,
                           msg="unable to locate schemas for connection {}".format(conn_id))

        # Verify the stream names discovered were what we expect
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertEqual(set(self.expected_streams()),
                         set(found_catalog_names),
                         msg="Expected streams don't match actual streams")

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        self.assertTrue(all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))


SCENARIOS.add(DiscoveryTest)