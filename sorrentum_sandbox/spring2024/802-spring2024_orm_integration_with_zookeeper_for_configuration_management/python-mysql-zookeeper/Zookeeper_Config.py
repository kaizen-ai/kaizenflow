from kazoo.client import KazooClient

# connection ZooKeeper
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

#zk.create('/configs/database_url', b"this is test", makepath=True)
# create /configs root config node
#zk.ensure_path("/configs")
if zk.exists('/configs/database_url'):
    print("1*******************************************")
# database_config
zk.set("/configs/database_url", b"mysql+pymysql://root:123456@localhost:3306/my_database")

test = zk.get('/configs/database_url')
print(test)
# close connection
zk.stop()