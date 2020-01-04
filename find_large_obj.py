#!/usr/bin/python
import json
import rados
import rbd

ceph_conf_path = '/etc/ceph/ceph.conf'
rados_connect_timeout = 5

class RADOSClient(object):
    def __init__(self,driver,pool=None):
        self.driver = driver
        self.client, self.ioctx = driver._connect_to_rados(pool)
    def __enter__(self):
        return self
    def __exit__(self, type_, value, traceback):
        self.driver._disconnect_from_rados(self.client, self.ioctx)

class RBDDriver(object):
    def __init__(self,ceph_conf_path,rados_connect_timeout,pool=None):
        self.ceph_conf_path = ceph_conf_path
        self.rados_connect_timeout = rados_connect_timeout
        self.pool = pool
    def _connect_to_rados(self, pool=None):
        client = rados.Rados(conffile=self.ceph_conf_path)
        try:
            if self.rados_connect_timeout >= 0:
                client.connect(timeout=
                               self.rados_connect_timeout)
            else:
                client.connect()
            if self.pool == None:
                                ioctx = None
            else:
                                ioctx = client.open_ioctx(self.pool)
            return client, ioctx
        except rados.Error:
            msg = "Error connecting to ceph cluster."
            client.shutdown()
            raise msg

    def _disconnect_from_rados(self, client, ioctx=None):
                if ioctx == None:
                        client.shutdown()
                else:
                        ioctx.close()
                        client.shutdown()

class cmd_manager():
    def get_large_omap_obj_poolname(self):
        with RADOSClient(RBDDriver(ceph_conf_path,rados_connect_timeout)) as dr:
                result = ''
                cmd = '{"prefix": "health", "detail": "detail", "format": "json"}'
                result = dr.client.mon_command(cmd,result)
                if result[0] == 0:
                    res_ = json.loads(result[1])
                    if res_["checks"]['LARGE_OMAP_OBJECTS']:
                        return res_["checks"]['LARGE_OMAP_OBJECTS']['detail'][0]['message'].split("'")[1]
                else:
                    return False
    def get_pg_list_by_pool(self,poolname):
        with RADOSClient(RBDDriver(ceph_conf_path,rados_connect_timeout)) as dr:
                result = ''
                cmd = '{"prefix": "pg ls-by-pool", "poolstr": "' + poolname + '", "format": "json"}'
                result = dr.client.mon_command(cmd,result)
                if result[0] == 0:
                    return json.loads(result[1])
                else:
                    return False

cmd_ = cmd_manager()
poolname = cmd_.get_large_omap_obj_poolname()
print "Large omap objects poolname = {0}".format(poolname)
res = cmd_.get_pg_list_by_pool(poolname)
res = res["pg_stats"]
for i in res:
#    print i
    if i["stat_sum"]["num_large_omap_objects"] != 0:
        print "pgid={0} OSDs={1} num_large_omap_objects={2}".format(i["pgid"],i["acting"],i["stat_sum"]["num_large_omap_objects"])
