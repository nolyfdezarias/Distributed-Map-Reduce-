 
from utils import * 

class Master():
    #def __init__(self,host,boss,serverHost = '', pull_port = None , pub_port = None , ping_port = None):
    def __init__(self,boss):
        __address = QNetworkInterface.allAddresses()
        ip = __address[2].toString()

        self.host = ip#host #socket.gethostbyname(socket.gethostname())
        #print(f'my ip is {self.host} ')
        print('my ip is ' + self.host)
        #self.server_pull_port = None
        #self.server_pub_port = None
        #self.server_sub_port = None
        #self.server_ping_port = None

        self.leader = True

        if boss == 0:
            self.leader = False

        #if serverHost == '':
        #    self.serverHost = self.host
        #else:
        #    self.serverHost = serverHost
        #    self.server_pull_port = pull_port
        #    self.server_pub_port = pub_port
        #    self.server_ping_port = ping_port
        
        self.pull_port = None
        self.ping_port = None
        self.pub_port = None
        self.sub_port = None
        
        self.ping_socket = None
        self.pub_socket = None
        self.sub_socket = None
        self.pull_socket = None
        
        self.MasterList = None
        self.clients  = []
        self.clientsFuncs = {}
        self.clients_map = {}
        self.clients_red = {}
        self.workers = []
        #self.Bworkers = []
        self.task = []
        self.buffer = {}
        self.look = Semaphore()
        self.lastID = 0
        

        pass
    
    def __call__(self):
        c = zmq.Context()
        self.pull_socket = c.socket(zmq.PULL)
        #aa = f'tcp://{self.host}'
        aaa = 'tcp://' + self.host
        self.pull_port = self.pull_socket.bind_to_random_port(aaa)
        #print(f'my pull port is {self.pull_port}')
        print('my pull port is ' + str(self.pull_port))

        self.pub_socket = c.socket(zmq.PUB)
        self.pub_port = self.pub_socket.bind_to_random_port(aaa)
        #print(f'my pub port is {self.pub_port}')
        print('my pub port is ' + str(self.pub_port))

        self.ping_socket = c.socket(zmq.PULL)
        self.ping_port = self.ping_socket.bind_to_random_port(aaa)
        #print(f'my ping port is {self.ping_port}')
        print('my ping port is ' + str(self.ping_port))
        self.sub_socket = c.socket(zmq.SUB)

        if self.leader:
            self.MasterList = [(self.host,self.pull_port,self.pub_port,self.ping_port)]
        

        

        if not self.leader:
            sub_addr = do_broadcast(BROADCAST_PORT)
            if sub_addr == '':
                print('I dont found a Server to support')
                return
            self.make_threads(c)
            #sub_addr = zmq_addr(self.server_pub_port,transport = 'tcp',host = self.serverHost)
            self.sub_socket.connect(sub_addr)
            self.sub_socket.subscribe('')
            #todo poner un canal de escucha si no eres lider
            serverHost = sub_addr.split('//')[1].split(':')[0]
            pull_port = int(sub_addr.split('//')[1].split(':')[1])

            sendMessage(c=c,_type = 'newMaster' , _message = (self.host,self.pull_port,self.pub_port,self.ping_port) ,_dadress = serverHost, _dport = pull_port , _adress = self.host , _port =self.pull_port)
            print('I send the connect message')
        else:
            self.make_threads(c)

    def make_threads(self,c):
        self.listen_thread = Thread(target=self.listen,args=(c,))
        self.listen_thread.start()
        print('Server is On and Listen')

        self.listenp_thread = Thread(target=self.listenping,args=(c,))
        self.listenp_thread.start()
        print('Server is ready ')

        self.listens_thread = Thread(target=self.subListen,args=(c,))
        self.listens_thread.start()
        print('Server is wait for workers and Clients message')

        self.task_thread = Thread(target=self.doingTask,args=(c,))
        self.task_thread.start()
        print('Server is wait for workers and Clients message')

        self.p_thread = Thread(target=self.__ping,args=(c,))
        #self.p_thread.start()
        #print('Server is wait for workers and Clients message')

        self.broadcast_server_thread = Thread(target=waiting_to_broadcast,args=(self.pull_port,))
        if self.leader:
            self.broadcast_server_thread.start()
            print('Server broadcast is ready')

    def doingTask(self,c):
        #c = zmq.Context()
        while True:
            print('I am in the while True')
            print(len(self.task))
            self.look.acquire()
            if self.leader:
                self.look.release()
                self.look.acquire()
                if len(self.task) > 0:
                    self.look.release()
                    readyWorkers = self.getWorkers(c)
                    if len(readyWorkers) > 0 :
                        
                        for x in readyWorkers:
                            print('I am in the readyWorkers')
                            self.look.acquire()
                            if len(self.task) > 0:
                                _adress , _port , _data , _type = self.task[0]
                                #print(f'this are the client {_adress} : {_port}')
                                print('this are the client ' + _adress + ' : ' + str(_port))
                                mapf , redf = self.clientsFuncs[(_adress , _port)]
                                aux , aux1 = x
                                _wadress, _wport , _,_,_ = aux
                                print('I send this data')
                                #print(_data)
                                val , ans , _type = sendTask(c = c,_name= _type,_message = _data,_dadress = _wadress , _dport = _wport ,_adress = self.host , _port = self.pull_port,_mapf=mapf,_redf=redf )
                                if val :
                                    #print(f'I have finish the task {self.task.pop(0)}')
                                    print('I have finish the task ' + str(self.task.pop(0)))
                                    #time.sleep(1)
                                    if _type == 0:
                                        
                                        try :
                                            #print(f'this are the client {_adress} : {_port}')
                                            print('this are the client ' + _adress + ' : ' + str(_port))
                                            self.buffer[(_adress , _port)].append((ans,(_adress , _port)))
                                        except : 
                                            #print(f'this are the client {_adress} : {_port}')
                                            print('this are the client ' + _adress + ' : ' + str(_port))
                                            self.buffer[(_adress , _port)] = [ (ans,(_adress , _port) )]

                                        self.clients_map[(_adress , _port)] -= 1
                                        if self.clients_map[(_adress , _port)] == 0:
                                            self.make_reduce_task(self.buffer[(_adress , _port)])
                                            #print(f'I finish the map with the client {(_adress , _port)}')
                                            print('I finish the map with the client  ' + _adress + ' : ' + str(_port))
                                            #print(self.buffer[(_adress , _port)][0])
                                        
                                        self.pubMessage(_adress = -1,_port = -1,_type = 'Change' , _name= 0,_message = (self.task,self.buffer,self.clients_map,self.clients_red))

                                    else:
                                        #print(f'this are the client {_adress} : {_port}')
                                        print('this are the client ' + _adress + ' : ' + str(_port))
                                        self.clients_red[(_adress , _port)] -= 1
                                        location = f'Ans_{_adress}_{_port}'
                                        fd = open(os.path.join(location), 'a')
                                        fd.close()

                                        fd = open(os.path.join(location), 'r+b')
                                        lines = fd.readlines()
                                        fd.close()

                                        _key , _value = ans
                                        text = f'{_key} : {_value} \n'
                                        
                                        aux =[ x for x in lines if x.decode() == text]
                                        if len(aux) == 0:
                                            fd = open(os.path.join(location), 'a')
                                            fd.write(text)
                                            fd.close()
                                        
                                        self.pubMessage(_adress = -1,_port = -1,_type = 'WChange' , _name= location,_message = text)

                                        if self.clients_red[(_adress , _port)] == 0:
                                            #print(f'I finish the red with the client {(_adress , _port)}')
                                            print('I finish the red with the client  ' + _adress + ' : ' + str(_port))

                                            fd = open(os.path.join(location), 'r+b')
                                            lines = fd.readlines()
                                            fd.close()
                                            #todo enviar pa que todos los master se enteren
                                            
                                            toping = -1
                                            for x in self.clients:
                                                aux, aux1 = x
                                                _a , _pullP , _, _pingP , _ = aux
                                                if _a == _adress and _pullP == _port:
                                                    toping = _pingP
                                                    break
                                            
                                            sendMessage(c=c,_dadress = _adress,_dport = _port , _type = 'sendAnswerF',_message = 'sendAnswerF' ,_name=location,_adress = self.host,_port = self.pull_port)
                                                    
                                            for line in lines:
                                                
                                                if self.ping(_adress,toping,c):
                                                    sendMessage(c=c,_dadress = _adress,_dport = _port , _type = 'sendAnswer' , _message = line.decode(), _size= len(line),_name=location,_adress = self.host,_port = self.pull_port)
                                                    print('Envie la linea')
                                                    #time.sleep(2)

                                                else:
                                                    print('Client is Down by Send')
                                                    todel = []
                                                    for x in self.task:
                                                        _Tadress , _Tport , _ , _ = x
                                                        if _adress == _Tadress and _port == _Tport :
                                                            todel.append(x)
                                                    for x in todel:
                                                        self.task.remove(x)
                                                    for x in self.clients:
                                                        aux,aux1 = x
                                                        _host,pull,_,_,_ = aux
                                                        if _host == _adress and pull == _port:
                                                            #todel.append(x)
                                                            self.clients.remove(x)
                                                            self.clients_map.pop((_adress , _port))
                                                            self.clients_red.pop((_adress , _port))
                                                            self.clientsFuncs.pop((_adress , _port))
                                                            #self.buffer.pop((_adress , _port))
                                                            break
                                                    
                                                    
                                                    break

                                                    

                                            #print(f'I have finished with the client {_adress} : {_port}')
                                            print('I finish finished with the client  ' + _adress + ' : ' + str(_port))
                                            
                                            sendMessage(c=c,_dadress = _adress,_dport = _port ,  _type = 'FinishAnswer' , _message = 'FinishAnswer',_adress = self.host,_port = self.pull_port)
                                            sendMessage(c=c,_dadress = _adress,_dport = toping ,  _type = 'goBye' , _message = 'FinishAnswer',_adress = self.host,_port = self.pull_port)

                                            #todel = []
                                            for x in self.clients:
                                                aux,aux1 = x
                                                _host,pull,_,_,_ = aux
                                                if _host == _adress and pull == _port:
                                                    #todel.append(x)
                                                    self.clients.remove(x)
                                                    self.clients_map.pop((_adress , _port))
                                                    self.clients_red.pop((_adress , _port))
                                                    self.clientsFuncs.pop((_adress , _port))
                                                    #self.buffer.pop((_adress , _port))
                                                    break
                                            #for x in todel:
                                            #    self.clients.remove(x)
                                            self.pubMessage(_adress = -1,_port = -1,_type = 'Change' , _name= 2,_message = (self.task,self.clients_red,self.clients_map,self.clientsFuncs,self.clients))

                                        else:
                                            self.pubMessage(_adress = -1,_port = -1,_type = 'Change' , _name= 1,_message = (self.task,self.clients_red))
                                        
                                        pass
                                else:
                                    pass

                                self.look.release()
                            else:
                                self.look.release()
                                print('We dont have task to do now')
                                break


                    else:
                        print('We dont have workers ready now')
                        time.sleep(3)
                else:
                    self.look.release()
                    print('We dont have task to do now')
                    time.sleep(5)
            else:
                self.look.release()
                print('no soy el lider')
                time.sleep(5)
        
    def __ping(self,c):
        
        while True:
            if not self.leader :
                time.sleep(15)
                print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                print('I am pining')
                toDel = []
                print(self.MasterList)
                for x in self.MasterList:
                    _host , _pull , _pub , _ping = x
                    print(x)
                    if _host == self.host and _pull == self.pull_port and _pub == self.pub_port and _ping == self.ping_port:
                        self.look.acquire()
                        self.leader = True
                        
                        print('I am the new Master :) XDDDDD')
                        print(len(self.task))
                        self.broadcast_server_thread.start()

                        self.look.release()
                        break
                    else:
                        print('I am not the leader')

                        if self.ping(_host,_ping,c):

                            self.look.acquire()
                            self.serverHost = _host
                            self.server_pull_port = _pull
                            self.server_pub_port = _pub
                            self.server_ping_port = _ping
                            #print(f'the current Server ist {x}')
                            print('the current Server ist ' + str(x))
                            self.look.release()
                            break
                        else:
                            print('no')
                            toDel.append(x)
                for x in toDel:
                    self.look.acquire()
                    self.MasterList.remove(x)
                    self.look.release()
                
    def listenping(self,c):
        while True:
            data = self.ping_socket.recv()
            data = data.decode()
            print('.....................................................')
            print(data)
            print('I am here')
            
            s1 = c.socket(zmq.PUSH)
            s1.connect(data)
            s1.send(b'')
            print('I am alive')
            
    def listen(self,c):
        while True:
            data = self.pull_socket.recv()
            data = dill.loads(data)
            print('########################################################')
            print(data)
            _type = data[TYPE]

            if _type == 'id':
                self.look.acquire()

                self.lastID += 1
                _,pull,pub,ping,_ = data[MESSAGE]
                sub_addr = zmq_addr(pub,'tcp',data[ADRESS])
                print(sub_addr)
                self.sub_socket.connect(sub_addr)
                self.sub_socket.subscribe('')

                if data[NAME] == 0:
                    self.pubMessage(_adress =   -1 ,_port = -1 , _type = 'newWorker' , _message = data[MESSAGE]  )
                    self.workers.append((data[MESSAGE],self.lastID))
                else:
                    self.pubMessage(_adress =   -1 ,_port = -1 , _type = 'newClient' , _message = data[MESSAGE] )
                    self.clients.append((data[MESSAGE],self.lastID))


                sendMessage(c=c,_type = 'id' , _dadress =  data[str(ADRESS)],_dport = data[str(PORT)],_message =self.lastID ,_adress = self.host,_port = self.pull_port)
                print('Envie un mensaje a esta direccion' + data[ADRESS] + str(data[PORT]))

                self.look.release()

            if _type == 'newMaster':
                self.look.acquire()
                self.MasterList.append(data[MESSAGE])
                sendMessage(c=c,_type = 'Welcome' , _message = (self.MasterList,self.workers,self.clients,self.clients_map,self.clients_red,self.clientsFuncs,self.buffer,self.task,self.lastID) ,_dadress = data[ADRESS], _dport = data[PORT] , _adress = self.host , _port =self.pull_port)
                
                for x in self.clients:
                    aux,aux1 = x
                    _a,_p,_,_,_ = aux
                    location = f'Ans_{_a}_{_p}'
                    try:
                        fd = open(os.path.join(location), 'r+b')
                        lines = fd.readlines()
                        fd.close()
                        for line in lines:
                            sendMessage(c=c,_type = 'Files' , _message = line.decode() , _name=location,_dadress = data[ADRESS], _dport = data[PORT] , _adress = self.host , _port =self.pull_port)
                    except:
                        pass

                time.sleep(2)
                print('I have send the Welcome message')
                self.look.release()
                pass
            if _type == 'Files':
                self.look.acquire()
                fd = open(os.path.join(data[NAME]), 'a')
                fd.write(data[MESSAGE])
                fd.close()
                self.look.release()

            if _type == 'Welcome':
                self.look.acquire()

                aux = data[MESSAGE]
                aux1,aux2,aux3,aux4,aux5,aux6,aux7,aux8,aux9 = aux
                self.MasterList = list(aux1)
                self.workers = list(aux2)
                self.clients = list(aux3)
                self.clients_map = dict(aux4)
                self.clients_red = dict(aux5)
                self.clientsFuncs = dict(aux6)
                self.buffer = dict(aux7)
                self.task = list(aux8)
                self.lastID = aux9

                for x in range(0,len(self.MasterList)-1):
                    _host , _pull , _pub , _ping = self.MasterList[x]

                    sub_addr = zmq_addr(_pub,transport = 'tcp',host = _host)
                    self.sub_socket.connect(sub_addr)
                    self.sub_socket.subscribe('')


                self.p_thread.start()
                self.look.release()
                pass

    def subListen(self,c):
        while True:
            data = self.sub_socket.recv()
            data = dill.loads(data)
            print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
            print(data)
            _type = data[TYPE]

            if _type == 'sendData':
                if self.leader:
                    sendMessage(c=c,_dadress = data[str(ADRESS)],_dport = data[str(PORT)] , _type = 'task' , _message = 'task',_adress = self.host,_port = self.pull_port)
           

            if _type == 'nameData':
                #location = f'{data[str(FILE)]}_{data[str(NAME)]}'
                self.pubMessage(_adress =   data[ADRESS] ,_port = data[PORT] , _type = 'nameData' , _message = data[MESSAGE] ,_file = data[FILE],_name=data[NAME])
                location = data[str(FILE)] + '_' + str(data[str(NAME)])
                fd = open(os.path.join(location), 'w')
                fd.close()
                pass

            if _type == 'Data':
                #location = f'{data[str(FILE)]}_{data[str(NAME)]}'
                self.pubMessage(_adress =   data[ADRESS],_port = data[PORT], _type = 'Data' , _message = data[MESSAGE] ,_file = data[FILE],_name=data[NAME])
                location = data[str(FILE)] + '_' + str(data[str(NAME)])
                fd = open(os.path.join(location), 'a')
                fd.write(data[str(MESSAGE)].decode())
                fd.close()
                pass
            if _type == 'FinishC':
                #location = f'{data[str(FILE)]}_{data[str(NAME)]}'
                self.pubMessage(_adress =   data[ADRESS],_port = data[PORT] , _type = 'FinishC' , _message = data[MESSAGE] ,_file = data[FILE],_name=data[NAME],_mapf=data[MAPF],_redf=data[REDF])
                location = data[str(FILE)] + '_' + str(data[str(NAME)])
                self.clientsFuncs[(data[str(ADRESS)],data[str(PORT)])] = (data[str(MAPF)],data[str(REDF)])
                
                #print(f'I get the data from Client:{data[str(ADRESS)]} : {data[str(PORT)]} ')
                print('I get the data from Client:' + data[str(ADRESS)] + ' : ' + str(data[str(PORT)]))
                self.look.acquire()
                self.make_map_task(location, data[str(ADRESS)], data[str(PORT)] )
                self.look.release()
                sendMessage(c=c,_dadress = data[str(ADRESS)],_dport = data[str(PORT)] , _type = 'reciveData' , _message = 'reciveData',_adress = self.host,_port = self.pull_port)
            
                
                pass
            
            if _type == 'newMasterIn':
                pass
            if _type == 'newWorker':
                self.look.acquire()

                self.lastID += 1
                host,pull,pub,ping,_ = data[MESSAGE]
                sub_addr = zmq_addr(pub,'tcp',host)
                print(sub_addr)
                self.sub_socket.connect(sub_addr)
                self.sub_socket.subscribe('')

                self.workers.append((data[MESSAGE],self.lastID))

                self.look.release()

            if _type == 'newClient':

                self.look.acquire()

                self.lastID += 1
                host,pull,pub,ping,_ = data[MESSAGE]
                sub_addr = zmq_addr(pub,'tcp',host)
                print(sub_addr)
                #self.sub_socket.connect(sub_addr)
                #self.sub_socket.subscribe('')
                self.clients.append((data[MESSAGE],self.lastID))

                self.look.release()

            if _type == 'Change':
                if data[NAME] == 0:
                    self.look.acquire()
                    aux = data[MESSAGE]
                    aux1,aux2,aux3,aux4 = aux
                    self.task = list(aux1)
                    self.buffer = dict(aux2)
                    self.clients_map = dict(aux3)
                    self.clients_red = dict(aux4)
                    self.look.release()
                elif data[NAME] == 1: 
                    self.look.acquire()
                    aux = data[MESSAGE]
                    aux1,aux2= aux
                    self.task = list(aux1)
                    self.clients_red = dict(aux2)
                    self.look.release()
                else:
                    self.look.acquire()
                    aux = data[MESSAGE]
                    aux1,aux2,aux3,aux4,aux5 = aux
                    self.task = list(aux1)
                    self.clients_red = dict(aux2)
                    self.clients_map = dict(aux3)
                    self.clientsFuncs = dict(aux4)
                    self.clients = list(aux5)
                    self.look.release()
                
            if _type == 'WChange':
                self.look.acquire()
                fd = open(os.path.join(data[NAME]), 'a')
                fd.write(data[MESSAGE])
                fd.close()
                self.look.release()


    def make_map_task(self,location,adress,port):
        initVal = len(self.task)
        fd = open(os.path.join(location), 'r+b')
        l = fd.readlines()
        fd.close()
        l = list(chunked(l,SIZEOFTASK))
        for x in l:
            text = ''
            for i in x:
                text += i.decode()
            self.task.append((adress,port,text,0))
        
        print(len( self.task))
        self.clients_map[(adress,port)] = len(self.task) - initVal
        #print(f'the client {adress}:{port} needs {len(self.task) - initVal} maps ')
        print('the client ' + adress + ':' + str(port) + ' needs ' + str(len(self.task) - initVal) + ' maps ')
        pass

    def make_reduce_task(self,_list):
        initVal = len(self.task)
        dic = {}
        _ , _client = _list[0]
        _adress , _port = _client
        for data, _  in _list:
            for x in data:
                key , value = x
                try:
                    dic[key].append(value)
                except:
                    dic[key] = [value]
        
        for key in dic.keys():
            self.task.insert(0,(_adress,_port, (key,dic[key]) ,1))

        self.buffer.pop((_adress,_port))
        self.clients_red[(_adress,_port)] = len(self.task) - initVal
        #print(f'the client {_adress}:{_port} needs {len(self.task) - initVal} reducers')
        print('the client ' + _adress + ':' + str(_port) + ' needs ' + str(len(self.task) - initVal) + ' reducers ')
        #time.sleep(5)
        
        

    def getWorkers(self,c):
        res = []
        td = []
        print(']]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]ooooooooooooooooooooooooooooooooooooo')
        for i in range(0,len(self.workers)):
            
            _adress , _id = self.workers[i]
            _wadress,_wport , _ , pingport, _ = _adress
            if self.ping(_wadress,pingport,c):
                res.append(self.workers[i])
            else:
                print('This Worker No')
                td.append(self.workers[i])
        
        if len(td) > 0:
            for x in td:
                self.workers.remove(x)
        print(res)      
        return res
    
    def ping(self,adress,port,c):
        s = c.socket(zmq.PULL)
        _port = s.bind_to_random_port('tcp://' + self.host)
        

        s1 = c.socket(zmq.PUSH)
        print(zmq_addr(port,'tcp',adress))
        s1.connect(zmq_addr(port,'tcp',adress))

        p = zmq.Poller()
        p.register(s,zmq.POLLIN)

        for i in range(0,RETRYTIME):
            s1.send(zmq_addr(_port,'tcp',self.host).encode())
            d = dict(p.poll(2000))
            if d != {}:
                data = s.recv()
                print(data)
                return True
            else:
                print('No')
        return False

    def pubMessage(self,_type,_adress,_port,_message,_size = 0 , _name = '',_mapf = None , _redf = None , _client = None, _nonBlock = 0,_file =None):
        if self.leader or _nonBlock:
            data = {
                    ADRESS : _adress,
                    PORT : _port, 
                    TYPE : _type,
                    SIZE : _size,
                    MESSAGE : _message,
                    MAPF : _mapf ,
                    REDF : _redf , 
                    CLIENT : _client,
                    NAME : _name,
                    FILE : _file

            }
        
            data = dill.dumps(data)
            self.pub_socket.send(data,zmq.NOBLOCK)



#a = Master(host=input('> mi direccion'),boss=int(input('>tu eres master ')),serverHost=input('>master adress ' ) , pull_port= int(input('>master pull ' )) , pub_port= int(input('>master pub ')), ping_port=int(input('> master ping ')))
a = Master(boss=int(input('>tu eres master ')))
a()

#todo enviar ping al client eventualmente(cada vez q se finalice una tarea asociada a el)