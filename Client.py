
from utils import * 

class Client():
    #def __init__(self,host,serverHost,pull_port,data):
    def __init__(self,bPort,data):
        #self.serverHost = serverHost
        __address = QNetworkInterface.allAddresses()
        ip = __address[2].toString()
        self.host = ip#host#socket.gethostbyname(socket.gethostname())
        #print(f'my ip is {self.host} ')
        print('my ip is ' + self.host)
        self.pull_port = None
        self.ping_port = None
        self.pub_port = None
        self.sub_port = None
        #self.server_pull_port = pull_port
        self.ping_socket = None
        self.pub_socket = None
        #self.sub_socket = None
        self.pull_socket = None

        self.data = data
        self.mapf = map1
        self.parsef = None
        self.reducef = reduce1
        self.id = None
        pass
    
    def __call__(self):
        c = zmq.Context()
        self.pull_socket = c.socket(zmq.PULL)
        self.pull_port = self.pull_socket.bind_to_random_port('tcp://' + self.host)
        print('my pull port is ' + str(self.pull_port))
        self.pub_socket = c.socket(zmq.PUB)
        self.pub_port = self.pub_socket.bind_to_random_port('tcp://' + self.host)
        print('my pub port is ' + str(self.pub_port))

        self.ping_socket = c.socket(zmq.PULL)
        self.ping_port = self.ping_socket.bind_to_random_port('tcp://' + self.host)

        #self.sub_socket = c.socket(zmq.SUB)
        _addr = do_broadcast(BROADCAST_PORT)
        if _addr == '':
            print('I dont found a Server ')
            return
        serverHost = _addr.split('//')[1].split(':')[0]
        pull_port = int(_addr.split('//')[1].split(':')[1])

        self.make_threads(c)
        sendMessage(c=c,_name= 1,_type = 'id', _message = (self.host,self.pull_port,self.pub_port,self.ping_port,self.sub_port), _dadress = serverHost,_dport = pull_port , _adress = self.host,_port = self.pull_port)
    
    def make_threads(self,c):
        self.listen_thread = Thread(target=self.collectTask,args=(c,))
        self.listen_thread.start()
        print('Worker is On and Listen')

        self.listenp_thread = Thread(target=self.listenping,args=(c,))
        self.listenp_thread.start()
        print('Worker is ready ')
        pass
    
    def listenping(self,c):
        while True:
            data = self.ping_socket.recv()
            try:
                data = data.decode()
                print('.....................................................')
                print(data)
                
                s1 = c.socket(zmq.PUSH)
                s1.connect(data)
                s1.send(b'')
            except:
                break

    def collectTask(self,c):
        while True:
            print('heeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee')
            data = self.pull_socket.recv()
            data = dill.loads(data)
            print('/////////////////////////////////////////')
            print(data)
            _type = data[TYPE]

            if _type == 'id':
                self.id = data[MESSAGE]
                #print(f'I am the Client with id: {data[MESSAGE]}')
                print('I am the Client with id: ' + str(data[MESSAGE]))
                pubMessage(s =self.pub_socket,_type = 'sendData', _message = 'sendData' ,_adress = self.host,_port = self.pull_port )
                print('I send the message')
                pass
            if _type == 'task':

                print('server is ready')
               
                fd = open(os.path.join(self.data), 'a')
                size = fd.tell()
                fd.close()

                fd = open(os.path.join(self.data), 'r+b')

                pubMessage(s=self.pub_socket,_adress = self.host,_port=self.pull_port,_type='nameData',_file=self.data,_message='nameData',_size=size,_name=self.id)
                
                while size > 0:
                    _read = fd.read( 4 * 1024 if 4 * 1024 < size else size)
                    size -= len(_read)
                    pubMessage(s= self.pub_socket,_adress = self.host,_port = self.pull_port,_type='Data',_message = _read,_file=self.data,_name=self.id,_size=len(_read))
                
                pubMessage(s=self.pub_socket,_adress = self.host,_port = self.pull_port,_message = 1,_type='FinishC',_mapf=map1,_redf=reduce1,_file= self.data,_name=self.id)
                
                fd.close()

                pass
            
            if _type == 'reciveData':
                print('Server has taken the Data ')
            
            if _type == 'sendAnswer':
                location = f'my {data[NAME]}'
                fd = open(os.path.join(location), 'a')
                fd.write(str(data[MESSAGE]))
                fd.close()
            
            if _type == 'FinishAnswer':
                print('I get my Answer')
                break

#a = Client(host = input('- My ip adress '),serverHost=input('> Master adress ') , pull_port = int(input('> master pullport ')), data = input('> dame el archivo '))
a = Client(bPort=int(input('> BroadCastPort : ')), data = input('> dame el archivo '))
a()
