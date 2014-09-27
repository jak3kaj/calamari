import logging
import uuid

from tests.server_testcase import RequestTestCase

log = logging.getLogger(__name__)


class TestCrushNodeManagement(RequestTestCase):
    def setUp(self):
        super(TestCrushNodeManagement, self).setUp()
        self.ceph_ctl.configure(2)
        self.calamari_ctl.configure()

    def test_lifecycle(self):
        """
        Test that we can:
         - Create a crush node
         - Add some children to it
         - Delete it
        """

        cluster_id = self._wait_for_cluster()

        # unique name assuming no collisions
        crush = {'name': str(uuid.uuid1()),
                 'bucket-type': 'rack',
                 'items': [],
                 }
        r = self.api.post("cluster/%s/crush_node" % cluster_id, crush)
        self._wait_for_completion(r)
