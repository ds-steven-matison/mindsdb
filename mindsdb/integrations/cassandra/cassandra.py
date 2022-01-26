from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class CassandraConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.keyspace = kwargs.get('database')

    def check_connection(self):
        try:
            cloud_config= {
                    'secure_connect_bundle': '/tmp/secure-connect-mindsdb.zip'
            }
            auth_provider = PlainTextAuthProvider('XsXDMLBtpoTkqwebibuBjSKQ', 'd_9igJodZk.RD01zxJIvRO4MJTWw12EHUA-HTN02--,F68bbq7aQ3TbKR+7aJnWYhUuUktDttwHask02y0CsYvNHQEqZ46XL7C8,IM5,Dp6lWNP5JZzUF2rBJ3hgtgeZ')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider,protocol_version=4)
            session = cluster.connect()

            if isinstance(self.keyspace, str) and len(self.keyspace) > 0:
                session.set_keyspace(self.keyspace)

            session.execute('select release_version from system.local').one()

            connected = True
        except Exception:
            connected = False
        return connected

