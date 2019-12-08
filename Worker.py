
from utils import * 

class Worker():
    def __init__(self):
        __address = QNetworkInterface.allAddresses()
        ip = __address[2].toString()
        self.host = ip#host#socket.gethostbyname(socket.gethostname())
        print('my ip is ' + self.host)
        self.pull_port = None
        self.ping_port = None
        self.pub_port = None
        self.sub_port = None
        self.ping_socket = None
        self.pub_socket = None
        self.pull_socket = None

        self.data = None
        self.mapf = None
        self.parsef = None
        self.reducef = None
    
    def __call__(self):
        c = zmq.Context()
        self.pull_socket = c.socket(zmq.PULL)
        self.pull_port = self.pull_socket.bind_to_random_port('tcp://' + self.host)

        self.pub_socket = c.socket(zmq.PUB)
        self.pub_port = self.pub_socket.bind_to_random_port('tcp://' + self.host)

        self.ping_socket = c.socket(zmq.PULL)
        self.ping_port = self.ping_socket.bind_to_random_port('tcp://' + self.host)

        _addr = do_broadcast(BROADCAST_PORT)
        if _addr == '':
            print('I dont found a Server ')
            return
        serverHost = _addr.split('//')[1].split(':')[0]
        pull_port = int(_addr.split('//')[1].split(':')[1])
        self.make_threads(c)
        sendMessage(c=c,_name=0,_type = 'id', _message = (self.host,self.pull_port,self.pub_port,self.ping_port,self.sub_port) ,_dadress = serverHost,_dport = pull_port , _adress = self.host,_port = self.pull_port)
        print('I have send the message')

    def make_threads(self,c):
        self.listen_thread = Thread(target=self.collectTask,args=(c,))
        self.listen_thread.start()
        print('Worker is On and Listen')

        self.listenp_thread = Thread(target=self.listenping,args=(c,))
        self.listenp_thread.start()
        print('Worker is ready ')

    def listenping(self,c):
        while True:
            data = self.ping_socket.recv()
            data = data.decode()
            print('.....................................................')
            print(data)
            
            s1 = c.socket(zmq.PUSH)
            s1.connect(data)
            s1.send(b'')

    def collectTask(self,c):
        while True:
            data = self.pull_socket.recv()
            data = dill.loads(data)
            print('/////////////////////////////////////////')
            print(data)
            _type = data[TYPE]

            if _type == 'id':
                print('I am the Worker with id: ' + str(data[MESSAGE]))

                pass
            if _type == 'task':
                if data[NAME] == 0:
                    self.data = data[MESSAGE]
                    self.mapf = data[MAPF]
                    self.mapFunc(data,c)
                else:
                    self.data = data[MESSAGE]
                    self.reducef = data[REDF]
                    self.redFunc(data,c)
            
    def mapFunc(self,data,c):
        lines = [l.strip() for l in self.data.split('\n')]
        res = []
        for x in lines:
            
            if len(x) > 0:
                key , value = x.split('-')
                res += self.mapf(key,value)

        print('I have finish my task')
        sendMessage(c= c,_type = 0,_message = res , _dadress = data[ADRESS],_dport = data[PORT] , _adress = self.host , _port = self.pull_port )            
        print('I send the answer')

    def redFunc(self,data,c):
        key , _list = self.data
        res = self.reducef(key,_list)
        print('I have finish my task')
        sendMessage(c = c,_type = 1,_message = (key,res) , _dadress = data[ADRESS],_dport = data[PORT] , _adress = self.host , _port = self.pull_port )
        print('I send the answer')
    

a = Worker()
a()