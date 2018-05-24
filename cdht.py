##Please run in python3!!

import sys
import threading
import socket
import time
import os

peerid, first_successor, second_successor = sys.argv[1:]
peers = [int(peerid), int(first_successor), int(second_successor)]

class peer:
    def __init__(self,peers):
        self.id = peers[0]
        self.first = peers[1]
        self.second = peers[2]
        self.serverport = 50000 + int(self.id)
        self.serverhost = "127.0.0.1"
        self.flag = 0
        self.flag_1 = 0
        self.fa = 0
        self.mark = 0
        self.mark_1 = 0
        self.ex_peers = []



    def server(self):
        #this is the UDP ping server
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((self.serverhost, self.serverport))
        while True:
            try:
                data, client_address = s.recvfrom(2048)
                data = data.decode()
                data = data.split(' ')
                if data[0] == 'F':
                    self.ex_peers = []
                    continue
                elif data[0] == 'R':
                    if data[-1] == 'first':
                        peer = data[1]
                        self.mark_1 = int(data[2])-1

                        print(f'A ping response message was received from Peer {peer}.')
                    else:
                        peer = data[1]

                        print(f'A ping response message was received from Peer {peer}.')
                    
                        
                else:
                    if data[-1] == 'first':
                        print(f'A ping request message was received from Peer {data[0]}.')
                        self.fa = data[0]
                        if int(data[0]) not in self.ex_peers:
                            self.ex_peers.append(int(data[0]))
                        address = (self.serverhost, int(self.fa) + 50000)
                        num = int(data[1])
                        msg = f'R {self.id} {num} first'
                        s.sendto(msg.encode(), address)
                    else:
                        print(f'A ping request message was received from Peer {data[0]}.')
                        self.fa = data[0]
                        if int(data[0]) not in self.ex_peers:
                            self.ex_peers.append(int(data[0]))
                        address = (self.serverhost, int(self.fa) + 50000)
                        msg = f'R {self.id} second'
                        s.sendto(msg.encode(), address)
            
                
                
                    
            except KeyboardInterrupt:
                print('error!')
                break
        s.close()


    def client_1(self):
        #this is the first ping UDP client
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while True:
            try:
                address = (self.serverhost, 50000 + self.first)
                self.mark += 1
                
                msg =f'{self.id} {self.mark} first'
                sock.sendto(msg.encode(), address)
                if self.mark -self.mark_1 > 4:
                    self.flag = 1
                if self.flag == 1:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((self.serverhost,self.second+50000))
                    msg = f'K {self.id} {self.first}'
                    s.send(msg.encode())
                    s.close()
                    self.mark = 0
                    self.mark_1 = 0
                time.sleep(11)

            except KeyboardInterrupt:
                print('error!')
                break
        sock.close()

    def client_2(self):
        #this is the second UDP client
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while True:
            try:
                address = (self.serverhost, 50000 + self.second)
                msg =f'{self.id} second'
                sock.sendto(msg.encode(), address)
                
                time.sleep(11)
            except KeyboardInterrupt:
                print('error!')
                break
        sock.close()

    def __hash_function(self,filename):
        return filename % 256

    def TCPserver(self):
        #this is the TCP server
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.serverhost,self.serverport))
        s.listen(5)
        while True:
            try:
                conn, addr = s.accept()
                data = conn.recv(1024)
                data = data.decode()
                data = data.split(' ')
                mode = data[0]
                number = data[1]
                from_who = data[2]
                ex_peers = sorted(self.ex_peers)
                if mode == 'Q':
                    number = int(number)
                    file = self.__hash_function(number)
                    if file > self.id:
                        if self.id < ex_peers[0] and file > ex_peers[1]:
                            print(f'File {number} is here.')
                            print(f'A response message, destined for peer {from_who}, has been sent.')
                            msg = f'R {number} {self.id}'
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.connect((self.serverhost, int(from_who) + 50000))
                            sock.send(msg.encode())
                        else:
                            print(f'File {number} is not stored here.')
                            print('File request message has been forwarded to my successor.')
                            msg = f'Q {number} {from_who}'
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.connect((self.serverhost, self.first + 50000))
                            sock.send(msg.encode())
                    elif self.id == file:
                        print(f'File {number} is here.')
                        print(f'A response message, destined for peer {from_who}, has been sent.')
                        msg = f'R {number} {self.id}'
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect((self.serverhost, int(from_who) + 50000))
                        sock.send(msg.encode())
                    else:
                        if file < self.id and file > ex_peers[0]:
                            print(f'File {number} is here.')
                            print(f'A response message, destined for peer {from_who}, has been sent.')
                            msg = f'R {number} {self.id}'
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.connect((self.serverhost, int(from_who) + 50000))
                            sock.send(msg.encode())
                        else:
                            print(f'File {number} is not stored here.')
                            print('File request message has been forwarded to my successor.')
                            msg = f'Q {number} {from_who}'
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.connect((self.serverhost, self.first + 50000))
                            sock.send(msg.encode())
                elif mode == 'R':
                    print(f'Received a response message from peer {from_who}, which has the file {number}.')
                elif mode == 'D':
                    second = data[3]
                    print(f'Peer {number} will depart from the network.')
                    print(f'My first successor is now peer {from_who}.')
                    print(f'My second successor is now peer {second}.')
                    self.first = int(from_who)
                    self.second = int(second)
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock_1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    address1 = (self.serverhost, self.first+50000)
                    address2 = (self.serverhost, self.second+50000)
                    msg = 'F'
                    sock.sendto(msg.encode(),address1)
                    sock_1.sendto(msg.encode(),address2)
                    conn.send('msg1 received'.encode())
                    self.mark = 0
                    self.mark_1 = 0
                elif mode == 'B':
                    print(f'Peer {number} will depart from the network.')
                    print(f'My first successor is now peer {self.first}.')
                    print(f'My second successor is now peer {from_who}.')
                    self.second = int(from_who)
                    conn.send('msg2 received'.encode())
                elif mode == 'K':
                    s_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s_2.connect((self.serverhost, int(number) + 50000))
                    msg = f'G {self.first} {from_who}'
                    s_2.send(msg.encode())
                elif mode == 'G':
                    print(f'Peer {self.first} is no longer alive.')
                    self.first = self.second
                    self.second = int(number)
                    
                    print(f'My first successor is now peer {self.first}.')
                    print(f'My second successor is now peer {number}.')
                    s_3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    if self.id > ex_peers[0] and self.id < ex_peers[1]:
                        s_3.connect((self.serverhost, ex_peers[0]+50000))
                        msg = f'H {self.first} {from_who}'
                        s_3.send(msg.encode())
                    else:
                        s_3.connect((self.serverhost, ex_peers[1]+50000))
                        msg = f'H {self.first} {from_who}'
                        s_3.send(msg.encode())
                    self.flag = 0
                elif mode == 'H':
                    self.second = int(number)
                    print(f'Peer {from_who} is no longer alive.')
                    print(f'My first successor is now peer {self.first}.')
                    print(f'My second successor is now peer {number}.')
                    self.flag = 0







            except KeyboardInterrupt:
                print('error!')
                break
        sock.close()
        sock_1.close()
        s_2.close()
        s_3.close()




    def TCPclient(self):
        while True:
            try:
                request = input().split(' ')
                #this is the part that a TCP connection is built to request file.
                if request[0] == 'request':
                    filename = int(request[1])
                    ex_peers = sorted(self.ex_peers)
                    flag = 0
                    if filename == self.id:
                        print(f'File {filename} is here.')
                        flag = 1
                    if self.id < ex_peers[1] and ex_peers[0] > self.id:
                        if self.__hash_function(filename) > ex_peers[1]:
                            print(f'File {filename} is here.')
                            flag = 1
                    elif self.id > ex_peers[0] and ex_peers[1]:
                        if self.__hash_function(filename) > ex_peers[0] and self.__hash_function(filename) < self.id:
                            print(f'File {filename} is here.')
                            flag = 1
                    else:
                        if self.__hash_function(filename) > ex_peers[1] and self.__hash_function(filename) < self.id:
                            print(f'File {filename} is here.')
                            flag =1
                    if flag == 0:
                        print(f'File request message for {filename} has been sent to my successor.')
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect((self.serverhost,self.first+50000))
                        filename = f'Q {filename} {self.id}'
                        s.send(filename.encode())
                        s.close()
                #this is the part that a TCP connection is built to quit this peer
                if request[0] == 'quit':
                    while True:
                        ex_peers = sorted(self.ex_peers)
                        
                        s_1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        if self.id > ex_peers[0] and self.id < ex_peers[1]:
                            s_1.connect((self.serverhost, ex_peers[0] + 50000))
                            s_2.connect((self.serverhost, ex_peers[1]+ 50000))
                            msg_1 = f'D {self.id} {self.first} {self.second}'
                            msg_2 = f'B {self.id} {self.first} {self.second}'
                            s_1.send(msg_1.encode())
                            s_2.send(msg_2.encode())
                            data1 = s_1.recv(1024)
                            data2 = s_2.recv(1024)
                            data1 = data1.decode().split(' ')
                            data2 = data2.decode().split(' ')
                            if data1[0] == 'msg1' and data2[0] == 'msg2':
                                s_1.close()
                                s_2.close()
                                break
                            else:
                                continue

                        else:
                            s_1.connect((self.serverhost, ex_peers[1] + 50000))
                            s_2.connect((self.serverhost, ex_peers[0] + 50000))
                            msg_1 = f'D {self.id} {self.first} {self.second}'
                            msg_2 = f'B {self.id} {self.first} {self.second}'
                            s_1.send(msg_1.encode())
                            s_2.send(msg_2.encode())
                            data1 = s_1.recv(1024)
                            data2 = s_2.recv(1024)
                            data1 = data1.decode().split(' ')
                            data2 = data2.decode().split(' ')
                            if data1[0] == 'msg1' and data2[0] == 'msg2':
                                s_1.close()
                                s_2.close()
                                break
                            else:
                                continue
                    os._exit(0)
            except KeyboardInterrupt:
                print('error!')
                break











pe = peer(peers)

try:
    iThread = threading.Thread(target=pe.server,args=[])
    iThread1 = threading.Thread(target=pe.client_1,args=[])
    iThread2 = threading.Thread(target=pe.client_2,args=[])
    iThread3 = threading.Thread(target=pe.TCPserver,args=[])
    iThread4 = threading.Thread(target=pe.TCPclient,args=[])


    iThread.start()
    iThread1.start()
    iThread2.start()
    iThread3.start()
    iThread4.start()
except KeyboardInterrupt:
    print('error!')
    









