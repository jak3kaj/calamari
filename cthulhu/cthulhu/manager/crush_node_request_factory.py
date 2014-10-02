from cthulhu.manager.request_factory import RequestFactory
from cthulhu.manager.user_request import OsdMapModifyingRequest
from calamari_common.types import OsdMap


class CrushNodeRequestFactory(RequestFactory):
    '''
    '''
    def update(self, node_id, attributes):
        # TODO need smarts about what to change, report No-ops can we do that from here?
        current_node = self._cluster_monitor.get_sync_object(OsdMap).crush_node_by_id[node_id]
        name, bucket_type, items = [attributes[key] for key in ('name', 'bucket-type', 'items')]

        # TODO change to use rename-bucket when #9526 lands in ceph
        commands = []
        if name != current_node['name'] or bucket_type != current_node['bucket-type']:
            commands = [add_bucket(name, bucket_type), remove_bucket(current_node['name'])]

        to_remove = []
        for item in current_node['items']:
            if item not in attributes['items']:
                to_remove.append(item)
        commands += self._remove_items(name, bucket_type, to_remove)

        commands += self._add_items(name, bucket_type, attributes['items'])
        message = "update CRUSH bucket in {cluster_name}".format(cluster_name=self._cluster_monitor.name)
        return OsdMapModifyingRequest(message, self._cluster_monitor.fsid, self._cluster_monitor.name, commands)

    def create(self, attributes):
        name, bucket_type, items = [attributes[key] for key in ('name', 'bucket-type', 'items')]
        commands = [add_bucket(name, bucket_type)] +\
            self._add_items(name, bucket_type, items)

        message = "Adding CRUSH bucket in {cluster_name}".format(cluster_name=self._cluster_monitor.name)
        return OsdMapModifyingRequest(message, self._cluster_monitor.fsid, self._cluster_monitor.name, commands)

    def delete(self, node_id):
        name = self._cluster_monitor.get_sync_object(OsdMap).crush_node_by_id[node_id]['name']
        commands = [remove_bucket(name)]
        message = "Removing CRUSH bucket  in {cluster_name}".format(cluster_name=self._cluster_monitor.name)
        return OsdMapModifyingRequest(message, self._cluster_monitor.fsid, self._cluster_monitor.name, commands)

    def _remove_items(self, name, bucket_type, items):
        commands = []
        for item in items:
            id = item['id']
            if id < 0:
                current_node = self._cluster_monitor.get_sync_object(OsdMap).crush_node_by_id[id]
                commands.append(remove_bucket(current_node['name']))
            else:
                child = 'osd.{id}'.format(id=id)
                # TODO does reweight have an ancestor parameter?
                commands.append(reweight_osd(child, 0.0))
                commands.append(remove_bucket(child))
        return commands

    def _add_items(self, name, bucket_type, items):
        commands = []
        # TODO what about subtrees containing OSDs
        for item in items:
            id = item['id']
            if id < 0:  # bucket case
                child = self._cluster_monitor.get_sync_object(OsdMap).crush_node_by_id[id]['name']
                commands.append(move_bucket(child, name, bucket_type))
            else:  # OSD
                child = 'osd.{id}'.format(id=id)
                commands.append(reweight_osd(child, 0.0))
                commands.append(remove_bucket(child))
                commands.append(move_osd(id, name, bucket_type))
                commands.append(reweight_osd(child, item['weight']))
        return commands


def add_bucket(name, bucket_type):
    return ('osd crush add-bucket', {'name': name, 'type': bucket_type},)


def remove_bucket(name):
    return ('osd crush remove', {'name': name},)


def reweight_osd(name, weight):
    return ('osd crush reweight', {'name': name,
                                   'weight': weight,
                                   },
            )


def move_osd(osd_id, parent_name, parent_type):
    return ('osd crush add', {'args': ['{type}={name}'.format(type=parent_type,
                                                              name=parent_name)],
                              'id': osd_id,
                              'weight': 0.0,
                              }
            )


def move_bucket(name, parent_name, parent_type):
    return ('osd crush move',
            {'name': name,
             'args': "{type}={name}".format(type=parent_type, name=parent_name),
             })
