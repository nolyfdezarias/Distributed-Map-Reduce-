
import zmq
import random
from threading import Thread,Semaphore
import dill
import os
from more_itertools import chunked
import time
import socket
from PyQt5.QtNetwork import QNetworkInterface


transports = frozenset(['udp', 'tcp', 'ipc', 'inproc'])

#Ports
BROADCAST_PORT = 6666

#KEYWORDS
ADRESS = '0'
TYPE = '1'
MESSAGE = '2'
PORT = '3'
SIZE = '4'
NAME = '5'
MAPF = '6'
REDF = '7'
CLIENT = '8'
BOSS = '9'
FILE = '10'

#GLOBAL VALUES
SIZEOFTASK = 100
TIMEOUT = 6000
RETRYTIME = 3

def map1(key,value):
    res = []
    for word in value.split(' '):
        res.append((word, 1))
    return res
    
def reduce1(key,values):
    res = 0
    for elem in values:
        res += int(elem)
    return res

def sendMessage(_type,_message,_dadress,_dport,_adress,_port ,c = None, _size = 0 , _mapf = None , _redf = None, _name = '',_client = None,_file= None):
        s = c.socket(zmq.PUSH)
        pull_addr = zmq_addr(_dport,transport = 'tcp',host = _dadress)
        #print('the pull addr to ping')
        print(pull_addr)
        s.connect(pull_addr)
        print('here sending')
        data = {
                ADRESS : _adress,
                PORT : _port , 
                TYPE : _type,
                MESSAGE : _message,
                SIZE : _size ,
                MAPF : _mapf ,
                REDF : _redf ,
                NAME : _name ,
                CLIENT : _client,
                FILE : _file

        }
        dataSend = dill.dumps(data)
        s.send(dataSend,zmq.NOBLOCK)
        

def sendTask(_message,_dadress,_dport,_adress,_port , _size = 0 ,c = None, _mapf = None , _redf = None, _name = '',_client = None):
        s = c.socket(zmq.PUSH)
        s1 = c.socket(zmq.PULL)
        s1_port = s1.bind_to_random_port('tcp://' + _adress)
        pol = zmq.Poller()
        pol.register(s1 , zmq.POLLIN)
        pull_addr = zmq_addr(_dport,transport = 'tcp',host = _dadress)
        print(pull_addr)
        s.connect(pull_addr)
        #print('here sending')
        data = {
                ADRESS : _adress,
                PORT : s1_port , 
                TYPE : 'task',
                MESSAGE : _message,
                SIZE : _size ,
                MAPF : _mapf ,
                REDF : _redf ,
                NAME : _name ,
                CLIENT : _client

        }
        dataSend = dill.dumps(data)

        s.send(dataSend,zmq.NOBLOCK)

        for i in range(0,RETRYTIME):
            print(f'Retrytime {i}')
            ans = dict(pol.poll(TIMEOUT))
            if ans != {} :
                data = s1.recv()
                data = dill.loads(data)
                
                return True , data[MESSAGE] , data[TYPE]
            time.sleep(2)
        return False , None , None

def pubMessage(s,_type,_message,_adress,_port , _size = 0 , _mapf = None , _redf = None, _name = '',_client = None, _file = None):
        data = {
                ADRESS : _adress,
                PORT : _port , 
                TYPE : _type,
                MESSAGE : _message,
                SIZE : _size ,
                MAPF : _mapf ,
                REDF : _redf ,
                NAME : _name ,
                CLIENT : _client,
                FILE : _file

        }
        dataSend = dill.dumps(data)
        s.send(dataSend,zmq.NOBLOCK)

def zmq_addr(port, transport=None, host=None):
    if host is None:
        host = '127.0.0.1'

    if transport is None:
        transport = 'tcp'

    assert transport in transports

    return '{transport}://{host}:{port}'.format(
        transport = transport,
        host      = host,
        port      = port,
    )

def waiting_to_broadcast(pull_port):
    #Get My Adress
    address = QNetworkInterface.allAddresses()
    ip = address[2].toString()

    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
    client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    client.bind(('',BROADCAST_PORT))

    while True:
        data, addr = client.recvfrom(1024)
        print(addr)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        while True:
            try:
                sock.connect((addr[0], addr[1]))
                break
            except (ConnectionRefusedError, OSError):
                print("Can't connect to " + str((addr[0], addr[1])))
                break
        try:
            sock.send(('tcp://' + ip +':' +str(pull_port)).encode())
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, ConnectionError, ConnectionRefusedError):
            continue

    client.close()

def do_broadcast(b_Port):
    address = QNetworkInterface.allAddresses()
    ip = address[2].toString()
    # Sending broadcast
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    # Set a timeout so the socket does not block
    rport = None
    while True:
        rport = random.randint(3000, 5000)
        try :
            client.bind((ip, rport))
            break
        except OSError :
            pass

    message = b'hello Server'
    for i in range(RETRYTIME * 3):
        if i > 0:
            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            client.bind((ip, rport))
        client.sendto(message, ('<broadcast>', b_Port))
        print('I send the broadcast message')    

        # Then we listen in a TCP socket
        # Listening response
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Set timeout
        try:
            client.bind((ip, rport))
        except OSError:
            return
        client.settimeout(3)
        client.listen(1000)
        try:
            s, addr_info = client.accept()
            s_ip = s.recv(1024).decode()
            if s_ip != '':
                return s_ip
        except:        
            print('ERROR', i)
            client.close()
            continue
    return ''