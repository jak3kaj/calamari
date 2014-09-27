from cthulhu.manager.request_factory import RequestFactory
from cthulhu.manager.user_request import OsdMapModifyingRequest
from calamari_common.types import OsdMap


def add_bucket(name, bucket_type):
    return [('osd crush add-bucket', {'name': name, 'type': bucket_type},)]


def remove_bucket(name):
    return [('osd crush remove', {'name': name},)]


class CrushNodeRequestFactory(RequestFactory):
    def _add_items(self, name, bucket_type, items):
        commands = []
        # TODO we need to restore the weight of OSDs after they move
        # TODO what about subtrees containing OSDs
        for item in items:
            id = item['id']
            if id < 0:  # bucket case
                child = self._cluster_monitor.get_sync_object(OsdMap).crush_node_by_id[id]['name']
                commands.append(('osd crush move',
                                 {'name': child,
                                  'args': "{type}={parent}".format(type=bucket_type, parent=name),
                                  }))
            else:  # OSD
                child = self._cluster_monitor.get_sync_object(OsdMap).osd_tree_node_by_id[id]['name']
                # TODO it seems that the CLI and admin socket behave differently here :(
                # the socket can't parse the crush map which leads me to me believe that it is doing the wrong set
                commands.append(('osd crush add',
                                 {'id': child,
                                  'weight': 0,
                                  'args': "{type}={parent}".format(type=bucket_type, parent=name),
                                  }))
        return commands

    def update(self, osd_id, attributes):
        # TODO change to use rename-bucket when #9526 lands in ceph
        name, bucket_type, items = [attributes[key] for key in ('name', 'bucket-type', 'items')]
        commands = self._add_items(name, bucket_type, items)
        '''
            #remove_bucket(name) +\
            #add_bucket(name, bucket_type) +\
            #self._add_items(name, bucket_type, items)
        '''
        message = "update CRUSH bucket in {cluster_name}".format(cluster_name=self._cluster_monitor.name)
        return OsdMapModifyingRequest(message, self._cluster_monitor.fsid, self._cluster_monitor.name, commands)

    def create(self, attributes):
        name, bucket_type, items = [attributes[key] for key in ('name', 'bucket-type', 'items')]
        commands = add_bucket(name, bucket_type) +\
            self._add_items(name, bucket_type, items)

        message = "Adding CRUSH bucket in {cluster_name}".format(cluster_name=self._cluster_monitor.name)
        return OsdMapModifyingRequest(message, self._cluster_monitor.fsid, self._cluster_monitor.name, commands)

    def delete(self, node_id):
        name = self._cluster_monitor.get_sync_object(OsdMap).crush_node_by_id[node_id]['name']
        commands = remove_bucket(name)
        message = "Removing CRUSH bucket  in {cluster_name}".format(cluster_name=self._cluster_monitor.name)
        return OsdMapModifyingRequest(message, self._cluster_monitor.fsid, self._cluster_monitor.name, commands)
