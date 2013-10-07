#!/usr/bin/env python3
import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_ISDIR, S_ISREG
from sys import argv, exit
from time import time, sleep

import socket
import sys
import select
import threading
import queue
import os
import datetime
import pickle
import os
import string
import hashlib

class Task():
    def __init__(self, desc):
        self.queuetime = time()
        self.starttime = None
        self.endtime = None
        self.desc = desc
        self.ret = None
        self.key = self.desc+str(self.queuetime)

class Logger():
    def __init__(self, logfile):
        self.fd = open(logfile, "a+")

    def log(self, info, function, message):
        self.fd.write("%s: %s %s %s\n" % (str(datetime.datetime.now()), info, function, message))
        self.fd.flush()

class TCPclient():
    def init(self, ip):
        global logger
        logger.log("INFO", "TCPclient_init", "connet to "+ip)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        port = 55002
        connected = 0
        while connected == 0:
            try:
                sock.connect((ip, port))
            except socket.error:
                logger.log("ERROR", "TCPclient_init", "connect "+ip+" failed, try again")
                #sleep(1)
                continue
            else:
                connected = 1
        return sock

    def sendmsg(self, filename, msg):
        global logger
        logger.log("INFO", "TCPclient_sendmsg", filename+", "+msg)
        size=1024
        ip = '127.0.0.1'

        try:
            s = self.init(ip)            
            s.send(bytes(filename+'#'+msg, "utf8"))
            if msg == 'STATE':
                ret = s.recv(10)
                qsize = int(ret.decode("utf8").strip('\0'))
                temp = b''
                while len(temp) < qsize:
                    data = s.recv(qsize-len(temp))
                    temp = temp+data                
                ret =pickle.loads(temp)
                s.send(bytes('0', 'utf8'))
            else:
                ret = s.recv(size)

        except socket.error as msg:
            logger.log("ERROR", "TCPclient_sendmsg", "Socket Exception: "+str(msg))
        except Exception as msg:
            logger.log("ERROR", "TCPclient_sendmsg", "Otehr Exception: "+str(msg))
        finally:
            s.close()
            if msg == 'STATE':
                return ret
            else:
                return ret.decode("utf8").strip('\0')
        
class AMFSclient():
    def __init__(self):
        self.tcpclient = TCPclient()
        
    def multi(self, path, algo):
        ret = self.tcpclient.sendmsg(path, "MULTI#"+algo)
        return ret
    def gather(self, path, algo):
        ret = self.tcpclient.sendmsg(path, "GATHER#"+algo)
        return ret
    def allgather(self, path, algo):
        ret = self.tcpclient.sendmsg(path, "ALLGATHER#"+algo)
        return ret
    def scatter(self, path, algo):
        ret = self.tcpclient.sendmsg(path, "SCATTER#"+algo)
        return ret
    def shuffle(self, path, algo, dst):
        ret = self.tcpclient.sendmsg(path, "SHUFFLE#"+algo+"#"+dst)
        return ret
    def queue(self, task):
        #ret = self.tcpclient.sendmsg(task, "QUEUE")
        fd = open('/tmp/task.txt', 'a+')
        fd.write(task.strip(' ')+'\n')
        fd.close()
        return 0
    def execute(self):
        ret = self.tcpclient.sendmsg("/", "EXECUTE")
        return ret
    def state(self):
        ret = self.tcpclient.sendmsg("/", "STATE")
        return ret
    def load(self, path, dst):
        ret = self.tcpclient.sendmsg(path, "LOAD#"+dst)
        return ret
    def dump(self, path, dst):
        ret = self.tcpclient.sendmsg(path, "DUMP#"+dst)
        return ret
    
if __name__ == '__main__':
    global logger
    logger = Logger("/tmp/amfora-client.log")
    logger.log("INFO", "main", "AMFORA client start")
    client = AMFSclient()
    op = sys.argv[1]
    if op == "multicast":
        algo = 'mst'
        path = sys.argv[2]
        logger.log("INFO", "main", "multicast "+path+" start")
        start = time()
        ret = client.multi(path, algo)
        end = time()
        if int(ret) == 0:
            logger.log("INFO", "main", "multicast "+path+" succeeded in "+str(end-start)+" seconds")
        else:
            logger.log("ERROR", "main", "multicast "+path+" failed")
    elif op == "gather":
        algo = 'mst'
        path = sys.argv[2]
        logger.log("INFO", "main", "gather "+path+" start")
        start = time()
        ret = client.gather(path, algo)
        end = time()
        if int(ret) == 0:
            logger.log("INFO", "main", "gather "+path+" succeeded in "+str(end-start)+" seconds")
        else:
            print(op+" "+path+" failed")
            logger.log("ERROR", "main", "gather "+path+" failed")
            sys.exit(1)
    elif op == "allgather":
        algo = 'mst'
        path = sys.argv[2]
        logger.log("INFO", "main", "all gather "+path+" start")
        start = time()
        ret = client.allgather(path, algo)
        end = time()
        if int(ret) == 0:
            logger.log("INFO", "main", "allgather "+path+" succeeded in "+str(end-start)+" seconds")
        else:
            print(op+" "+path+" failed")
            logger.log("ERROR", "main", "allgather "+path+" failed")
            sys.exit(1)
    elif op == "scatter":
        algo = 'mst'
        path = sys.argv[2]
        logger.log("INFO", "main", "scatter "+path+" start")
        start = time()
        ret = client.scatter(path, algo)
        end = time()
        if int(ret) == 0:
            logger.log("INFO", "main", "scatter "+path+" succeeded in "+str(end-start)+" seconds")
        else:
            print(op+" "+path+" failed")
            logger.log("ERROR", "main", "scatter "+path+" failed")
            sys.exit(1)
    elif op == "shuffle":
        algo = 'mst'
        src = sys.argv[2]
        dst = sys.argv[3]
        logger.log("INFO", "main", "shuffle the data in "+src+" to "+dst)
        start = time()
        ret = client.shuffle(src, algo, dst)
        end = time()
        if int(ret) == 0:
            logger.log("INFO", "main", "shuffle "+src+" to "+dst+" succeeded in "+str(end-start)+" seconds")
        else:
            print(op+" "+path+" failed")
            logger.log("ERROR", "main", "shuffle "+src+" to "+dst+" failed")
            sys.exit(1)
    elif op == "queue":
        args = sys.argv[2:]
        task = ""
        for a in args:
            task = task+" "+a+" "
        task.strip(' ')
        logger.log("INFO", "main", "put into queue task: "+ task)
        ret = client.queue(task)
        if int(ret) == 0:
            logger.log("INFO", "main", "put into queue task:"+task+" succeeded")
        else:
            print(op+" "+task+" failed")
            logger.log("ERROR", "main", "put into queue task: "+task+" failed")
            sys.exit(1)
    elif op == "execute":
        logger.log("INFO", "main", "execute")
        start = time()
        ret = client.execute()
        end  = time()
        if int(ret) == 0:
            logger.log("INFO", "main", "execution succeeded in "+str(end-start)+" seconds")
            os.remove('/tmp/task.txt')
        else:
            print(op+" failed")
            logger.log("ERROR", "main", "execution failed")
            sys.exit(1)
    elif op == "load":
        src = sys.argv[2]
        dst = sys.argv[3]
        logger.log("INFO", "main", "load "+src+" to "+dst)
        start = time()
        ret = client.load(src, dst)
        end  = time()
        if int(ret) == 0:
            logger.log("INFO", "main", "load succeeded in "+str(end-start)+" seconds")
        else:
            print(op+" "+src+" to "+dst+" failed")
            logger.log("ERROR", "main", "load failed")
            sys.exit(1)
    elif op == "dump":
        src = sys.argv[2]
        dst = sys.argv[3]
        logger.log("INFO", "main", "dump "+src+" to "+dst)
        start = time()
        ret = client.dump(src, dst)
        end  = time()
        if int(ret) == 0:
            logger.log("INFO", "main", "dump succeeded in "+str(end-start)+" seconds")
        else:
            print(op+" "+src+" to "+dst+" failed")
            logger.log("ERROR", "main", "dump failed")
            sys.exit(1)
    elif op == "state":
        ofile = sys.argv[2]

        logger.log("INFO", "main", "state")
        start = time()
        retq = client.state()
        end  = time()

        logger.log("INFO", "main", "execution succeeded in "+str(end-start)+" seconds")
        fd = open(ofile, 'w')
        fd.write("ID\tDESC\tQTIME\tSTIME\tETIME\tRET\n")
        for t in retq:
            s = t.key+", "+t.desc+", "+str(t.queuetime)+", "+str(t.starttime)+", "+str(t.endtime)+", "+str(t.ret)
            fd.write(s+'\n')
        fd.close()

    else:
        logger.log("ERROR", "main", "operation: "+op+" not supported")
        sys.exit(1)
