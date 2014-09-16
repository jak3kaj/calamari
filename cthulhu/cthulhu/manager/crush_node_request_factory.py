from cthulhu.manager.request_factory import RequestFactory
from cthulhu.manager.user_request import OsdMapModifyingRequest
from calamari_common.types import OsdMap


class CrushNodeRequestFactory(RequestFactory):
    # TODO add-bucket is probably not the right operation
    def update(self, osd_id, attributes):
        commands = [('osd crush add-bucket', {'name': 'gmeno_test_root', 'type': 'root'})]
        message = "Adding CRUSH buckey in {cluster_name}".format(cluster_name=self._cluster_monitor.name)
        return OsdMapModifyingRequest(message, self._cluster_monitor.fsid, self._cluster_monitor.name, commands)

    def create(self, attributes):
        commands = [('osd crush add-bucket', {'name': attributes['name'], 'type': attributes['bucket-type']})]
        message = "Adding CRUSH bucket in {cluster_name}".format(cluster_name=self._cluster_monitor.name)
        return OsdMapModifyingRequest(message, self._cluster_monitor.fsid, self._cluster_monitor.name, commands)

    def delete(self, node_id):
        name = self._cluster_monitor.get_sync_object(OsdMap).crush_node_by_id[node_id]['name']
        commands = [('osd crush remove', {'name': name})]
        message = "Removing CRUSH bucket  in {cluster_name}".format(cluster_name=self._cluster_monitor.name)
        return OsdMapModifyingRequest(message, self._cluster_monitor.fsid, self._cluster_monitor.name, commands)
