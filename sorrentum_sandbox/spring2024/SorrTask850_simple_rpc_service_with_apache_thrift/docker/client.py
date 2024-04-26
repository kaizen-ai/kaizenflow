import sys
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from gen_py.SimpleService import SimpleService

try:
    # Make socket
    transport = TSocket.TSocket('127.0.0.1', 9090)

    # Buffering
    transport = TTransport.TBufferedTransport(transport)

    # Wrap protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client
    client = SimpleService.Client(protocol)

    # Connect
    transport.open()

    print(client.sayHello("test"))
    print(client.addNumbers(1, 1))

    transport.close()

except Thrift.TException as tx:
    print(f"{tx.message}")
