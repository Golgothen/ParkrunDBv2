import socket, pickle
from message import Message
from mplogger import *

BUFFER_SIZE = 1024
SIZE_HEADER = 8
BYTE_ORDER = 'big'

__version__ = '0.0.1'

class Connection():
    def __init__(self, **kwargs):
        sock = None
        config = None
        self.host = 'localhost'
        self.port = 2345
        self.connected = False
        
        for k, v in kwargs.items():
            if k.upper() == 'CONFIG':
                config = v
            if k.upper() == 'SOCK':
                sock = v
            if k.upper() == 'HOST':
                self.host = v
            if k.upper() == 'PORT':
                self.port = v
        
        if config is None:
            raise ValueError('Mandatory parameter CONFIG is missing')
        else:
            logging.config.dictConfig(config)
            self.logger = logging.getLogger(__name__)
            self.logger.debug(f"Logger {__name__} started")
        
        if sock is None:
            self.logger.debug('Creating a new socket')
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.logger.debug('Using passed in socket')
            self.socket = sock
            self.connected = True
        
        self.bytesSent = 0
        self.bytesReceived = 0
        
    def connect(self):
        if not self.connected:
            try:
                self.logger.info('Connecting to {}:{}'.format(self.host, self.port))
                self.socket.connect((self.host, self.port))
            except:
                self.close()
                raise
            self.connected = True
            #self.send(Message('CLIENT_INFO', VERSION = __version__))
            #m = self.recv()
            #if m.message != 'OK':
            #    raise RuntimeError('Server reports incompatible client.')
            #    self.close()
    
    def send(self, data):
        if not self.connected:
            self.connect()
        self.logger.debug('Sending {}'.format(data))
        dump = pickle.dumps(data)
        size = len(dump).to_bytes(SIZE_HEADER, byteorder = BYTE_ORDER)
        self.logger.debug('Message size is {}'.format(len(dump)))
        try:
            sent = self.socket.send(size + dump)
        except:
            self.close()
            raise
        self.logger.debug('{} bytes sent'.format(sent))
        self.bytesSent += sent
        self.logger.debug('{} total bytes sent'.format(self.bytesSent))
    
    def recv(self):
        if not self.connected:
            raise RuntimeError('Cannot receive when not connected')
        try:
            data = self.socket.recv(SIZE_HEADER)
        except:
            self.close()
            raise
        messageSize = int.from_bytes(data, byteorder = BYTE_ORDER)
        if messageSize == 0:
            self.close()
            return(Message('CLOSE'))
        self.logger.debug('Message is {} bytes long'.format(messageSize))
        if messageSize <= BUFFER_SIZE:
            self.logger.debug('Pulling {} bytes from buffer'.format(messageSize))
            try:
                data = self.socket.recv(messageSize)
            except:
                self.close()
                raise
        else:
            data = b''
            while messageSize > BUFFER_SIZE:
                try:
                    d = self.socket.recv(BUFFER_SIZE)
                except:
                    self.close()
                    raise
                self.logger.debug('Pulling {} bytes from buffer'.format(len(d)))
                messageSize -= len(d)
                data += d
                self.logger.debug('Pulled {} bytes so far'.format(len(data)))
                self.logger.debug('{} bytes to go'.format(messageSize))
            if messageSize > 0:
                try:
                    d = self.socket.recv(messageSize)
                except:
                    self.close()
                    raise
                self.logger.debug('Pulling {} bytes from buffer'.format(len(d)))
                data += d
        self.logger.debug('Message was {} bytes in length'.format(len(data)))
        self.bytesReceived += (len(data) + SIZE_HEADER)
        self.logger.debug('{} total bytes received'.format(self.bytesReceived))
        try:
            d = pickle.loads(data)
        except:
            self.close()
            raise
        #self.logger.debug('Received: {}'.format(d))
        return d
    
    def close(self):
        self.socket.close()
        self.connected = False
        #recreate the socket ready for the next connection
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)