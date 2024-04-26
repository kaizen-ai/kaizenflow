import sys
from gen_py.SimpleService import SimpleService
from gen_py.SimpleService.ttypes import *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

class SimpleServiceHandler:
    def sayHello(self, name):
        return f"Hello, {name}!"

    def addNumbers(self, num1, num2):
        return num1 + num2

if __name__ == "__main__":
    handler = SimpleServiceHandler()
    processor = SimpleService.Processor(handler)
    transport = TSocket.TServerSocket(host='127.0.0.1', port=9090)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print("Starting Python server...")
    server.serve()
    print("done!")
