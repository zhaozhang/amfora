#!/usr/bin/env python3
import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_ISDIR, S_ISREG
from sys import argv, exit
from time import time, sleep

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
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
import subprocess
import codecs
import zlib
import math
import re
import random
import shutil

'''
Amfora is a shared in-memory file system. Amfora is POSIX compatible.
NOTE: Methods that have not been implemented are removexattr, setxattr
'''

class Logger():
    def __init__(self, logfile):
        self.fd = open(logfile, "w")

    def log(self, info, function, message):
        #self.fd.write("%s: %s %s %s\n" % (str(datetime.datetime.now()), info, function, message))
        #self.fd.flush()
        print("%s: %s %s %s" % (str(datetime.datetime.now()), info, function, message))

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Amfora(LoggingMixIn, Operations):
    def __init__(self):
        '''
        Amfora data is organized as following:
        self.meta stores persistent metadata
        self.data stores persistent file data
        self.cmeta stores transient local metadata
        self.cdata stores transient local file data
        self.cdata is replicated in self.data once the write in self.cdata is completed (released).
        '''
        self.meta = {}
        self.data = defaultdict(bytes)
        self.cmeta = {}
        self.cdata = defaultdict(bytes)
        self.fd = 0
        self.recovery = {}
        self.stale = {}
        #initializing the root directory
        now = time()
        self.meta['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2, location=[], key=None, task="", e_recovery=0.0)

    '''    
    below are collective interface
    '''
    def multicast(self, path, algo):
        global logger
        global slist
        global mountpoint
        global misc
        tcpclient = TCPClient()
        apath = path[len(mountpoint):]
        logger.log("INFO", "MULTICAST", "multicast "+path+" "+apath)
        
        if path[:len(mountpoint)] != mountpoint:
            logger.log("ERROR", "MULTICAST", path[:len(mountpoint)]+" is not the mountpoint")
            return 1

        #if the meta data is not local, copy it to local meta data
        if apath not in self.meta:
            self.meta[apath] = self.getattr(apath, None)
        #if the file data is not local, copy it to local storage first
        if apath not in self.data:
            ip = self.meta[apath]['location'][0]
            logger.log("INFO", "READ", "read sent to remote server "+apath+" "+ip)
            packet = Packet(apath, "READ", {}, {}, 0, [ip], [0,0])
            
            rpacket = tcpclient.sendpacket(packet)
            if not rpacket.data:
                logger.log("ERROR", "READ", "remote read on "+path+" failed on "+ip)
            else:
                self.data[apath] = rpacket.data[apath]
        #assembe the multicast packet
        ddict = dict()
        ddict[apath] = self.data[apath]
        mdict = dict()
        mdict[apath] = self.meta[apath]
        packet=Packet(apath, "MULTICAST", mdict, ddict, 0, slist, None)
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "MULTICAST", "multicasting file: "+apath+" failed")
        return rpacket

    def allgather(self, path, algo):
        global logger
        global slist
        global mountpoint
        global misc
        #allgather is a two step procedure
        #1, gather the data to one node
        #2, multicast the data to all nodes
        tcpclient = TCPClient()
        apath = path[len(mountpoint):]
        logger.log("INFO", "ALLGATHER", "allgather "+path+" "+apath)
        ret, data, meta = self.gather(path, algo)
        packet=Packet(apath, "MULTICAST", meta, data, 0, slist, None)
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "ALLGATHER", "allgathering path: "+apath+" failed")
            return rpacket
        else:
            logger.log("INFO", "ALLGATHER", "allgathering path: "+apath+" finished")
            return rpacket
            

    def gather(self, path, algo):
        global logger
        global slist
        global mountpoint
        global misc
        tcpclient = TCPClient()
        apath = path[len(mountpoint):]
        logger.log("INFO", "GATHER", "gather "+path+" "+apath)
        
        if path[:len(mountpoint)] != mountpoint:
            logger.log("ERROR", "GATHER", path[:len(mountpoint)]+" is not the mountpint")
            return 1, None
        if path == mountpoint:
            apath = '/'
        if not S_ISDIR(self.meta[apath]['st_mode']):
            logger.log("ERROR", "GATHER", apath+" is not a directory")
            return 1, None
        #readdir to get the metadata
        packet = Packet(apath, "READDIR", {}, {}, 0, slist, 0)
        rpacket = tcpclient.sendallpacket(packet)
        nmeta = dict(rpacket.meta)
        gdict = dict()
        for m in rpacket.meta:
            if rpacket.meta[m]['location'][0] not in gdict:
                gdict[rpacket.meta[m]['location'][0]] = []
            gdict[rpacket.meta[m]['location'][0]].append(m)
        self.meta.update(rpacket.meta)    
        packet = Packet(apath, "GATHER", {}, {}, 0, slist, gdict)    
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "GATHER", "gather "+path+" failed")
            return rpacket, None, None
        else:
            self.data.update(rpacket.data)
            logger.log("INFO", "GATHER", "gather "+path+" finished")
            return rpacket, rpacket.data, nmeta

    def scatter(self, path, algo):
        global logger
        global slist
        global mountpoint
        global misc
        #scatter is a two step procedure
        #1, gather the data to one node
        #2, scatter the data to all nodes
        tcpclient = TCPClient()
        apath = path[len(mountpoint):]
        logger.log("INFO", "SCATTER", "scatter "+path+" "+apath)
        if apath not in self.meta:
            return Packet(apath, "SCATTER", None, None, 1, None, None)
        ret, data, meta = self.gather(path, algo)
        klist = list(sorted(meta.keys()))
        klist.reverse()
        #keep part of the data local
        num_files = math.ceil(len(meta)/len(slist))
        logger.log("INFO", "SCATTER", "This node keeps "+str(num_files)+" files")    
        logger.log("INFO", "SCATTER", str(klist))
        for i in range(num_files):
            k = klist.pop()
            v = meta.pop(k)
            data.pop(k)
            logger.log("INFO", "SCATTER", "this node keeps "+k)
        packet=Packet(apath, "SCATTER", meta, data, 0, slist, klist)
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "SCATTER", "allgathering path: "+apath+" failed")
            return rpacket
        else:
            self.meta.update(rpacket.meta)
            logger.log("INFO", "SCATTER", "allgathering path: "+apath+" finished")
            return rpacket

    def shuffle(self, path, algo, dst):
        global logger
        global slist
        global mountpoint
        global misc
        tcpclient = TCPClient()
        apath = path[len(mountpoint):]
        dpath = dst[len(mountpoint):]
        logger.log("INFO", "SHUFFLE", "shuffle from "+apath+" to "+dpath)
        
        if path[:len(mountpoint)] != mountpoint or dst[:len(mountpoint)] != mountpoint:
            logger.log("ERROR", "MULTICAST", path[:len(mountpoint)]+" or "+dst[:len(mountpoint)]+" is not the mountpoint")
            return 1
        if path == mountpoint:
            apath = '/'
        if not S_ISDIR(self.meta[apath]['st_mode']) or not S_ISDIR(self.meta[dpath]['st_mode']) :
            logger.log("ERROR", "GATHER", apath+" is not a directory")
            return 1

        #readdir to get the metadata
        packet = Packet(apath, "READDIR", {}, {}, 0, slist, 0)
        rpacket = tcpclient.sendallpacket(packet)
        nmeta = dict(rpacket.meta)

        #assemble the ip:hvalue hashmap
        ndict = dict()
        for m in nmeta:
            if nmeta[m]['location'][0] not in ndict:
                ndict[nmeta[m]['location'][0]] = []
            ndict[nmeta[m]['location'][0]].append(m)

        #start shuffle server
        logger.log("INFO", "SHUFFLE_START", str(slist))    
        packet=Packet(apath, "SHUFFLE_START", {}, {}, 0, slist, [ndict, dpath])
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "SHUFFLE_START", "shuffling from "+apath+" to "+dpath+" failed")

        #assemble the shuffle packet
        logger.log("INFO", "SHUFFLE", str(ndict))    
        packet=Packet(apath, "SHUFFLE", {}, {}, 0, slist, dpath)
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "SHUFFLE", "shuffling from "+apath+" to "+dpath+" failed")
        return rpacket
        
    def load(self, src, dst):
        global logger
        global slist
        global mountpoint
        global misc
        logger.log("INFO", "LOAD", "loading "+src+" to "+dst+" started")
        apath = dst[len(mountpoint):]
        if dst == mountpoint:
            apath = '/'

        if dst[:len(mountpoint)] != mountpoint:
            logger.log("ERROR", "LOAD", dst[:len(mountpoint)]+" is not the mountpint")    
            return 1
        if apath not in self.meta:
            logger.log("ERROR", "LOAD", "direcotry "+apath+" does not exist")    
            return 1
        if not os.path.exists(src):
            logger.log("ERROR", "LOAD", "directory: "+src+" does not exist")
            return 1

        tcpclient = TCPClient()
        #mkdir
        basename = os.path.basename(src)
        dirname = os.path.join(apath, basename)
        logger.log("INFO", "LOAD", "creating dir: "+dirname)
        if dirname not in self.meta:
            self.mkdir(dirname, self.meta['/']['st_mode'])
        
        #read file names in the src dir
        tlist = os.listdir(src)
        flist = []
        for f in tlist:
            #print(os.path.join(src, f))
            if os.path.isdir(os.path.join(src, f)):
                logger.log("INFO", "LOAD", "recursively load "+os.path.join(src, f)+" to "+os.path.join(mountpoint, basename))
                self.load(os.path.join(src, f), os.path.join(mountpoint, basename))
            else:    
                flist.append(os.path.join(src, f))
        flist.sort()
        logger.log("INFO", "LOAD", "loading the following files "+str(flist)+" from "+src+" to "+dirname)
        packet=Packet(dirname, "LOAD", {}, {}, 0, slist, flist)
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "LOAD", "loading "+src+" to "+dirname+" failed")
            return rpacket
        else:
            logger.log("INFO", "LOAD", "loading "+src+" to "+dirname+" finished")
            return rpacket

    def dump(self, src, dst):
        global logger
        global slist
        global mountpoint
        global misc
        logger.log("INFO", "DUMP", "dumping "+src+" to "+dst+" started")
        apath = src[len(mountpoint):]
        if src == mountpoint:
            apath = '/'

        if src[:len(mountpoint)] != mountpoint:
            logger.log("ERROR", "DUMP", src[:len(mountpoint)]+" is not the mountpint")    
            return 1
        if apath not in self.meta:
            logger.log("ERROR", "DUMP", "direcotry "+apath+" does not exist")    
            return 1
        if not os.path.exists(dst):
            logger.log("ERROR", "DUMP", "directory: "+dst+" does not exist")
            return 1

        tcpclient = TCPClient()
        
        #mkdir for dst
        basename = os.path.basename(src)
        dirname = os.path.join(dst, basename)
        if os.path.exists(dirname):
            logger.log("ERROR", "DUMP", "directory: "+dirname+" exists")
            #return 1
        else:
            os.mkdir(dirname)

        #recursively dumping sub-directory
        for k in self.meta:
            if S_ISDIR(self.meta[k]['st_mode']) and os.path.dirname(k) == apath:
                logger.log("INFO", "DUMP", "recursively dumping "+os.path.join(mountpoint, k[1:])+" "+dirname)
                self.dump(os.path.join(mountpoint, k[1:]), dirname)
     
        #read metadata of the files in this dir    
        packet = Packet(apath, "READDIR", {}, {}, 0, slist, 0)
        rpacket = tcpclient.sendallpacket(packet)
        meta = dict(rpacket.meta)
        fdict = dict() #key-ip, value-list of hvalue
        for k in meta:
            if meta[k]['location'][0] not in fdict:
                fdict[meta[k]['location'][0]] = []
            fdict[meta[k]['location'][0]].append([meta[k]['key'], os.path.join(dirname, k[len(apath)+1:])])   
        
        logger.log("INFO", "DUMP", "dumping the following files "+str(fdict)+" from "+src+" to "+dirname)
        packet=Packet(src, "DUMP", {}, {}, 0, slist, fdict)
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "DUMP", "dumping "+src+" to "+dirname+" failed")
            return rpacket
        else:
            logger.log("INFO", "DUMP", "dumping "+src+" to "+dirname+" finished")
            return rpacket
    
        
    def execute(self):
        global logger
        global slist
        global mountpoint
        global misc
        logger.log("INFO", "EXECUTE", "execute all tasks")
        tcpclient = TCPClient()
        taskl = misc.readtask()
        packet=Packet("", "EXECUTE", {}, {}, 0, slist, taskl)
        rpacket = tcpclient.sendallpacket(packet)
        if len(rpacket.misc) > 0:
            print("execution failed on "+str(rpacket.tlist))
            print("there are "+str(len(rpacket.misc))+" tasks remaining")
            print("rerun the failed tasks on "+str(slist))
            tlist = list(slist)
            for ip in rpacket.tlist:
                tlist.remove(ip)
            packet = Packet("", "EXECUTE", {}, {}, 0, tlist, rpacket.misc)
            rrpacket = tcpclient.sendallpacket(packet) 
            rpacket.meta.update(rrpacket.meta)
            rpacket.data.update(rrpacket.data)
            logger.log("INFO", "EXECUTE", "second round execution finished "+str(rrpacket.meta))
        if sum(rpacket.meta.values()) != 0:
            logger.log("ERROR", "EXECUTE", "execution failed "+str(rpacket.meta)+"\n"+str(rpacket.data))
            return rpacket
        else:
            logger.log("INFO", "EXECUTE", "execution finished "+str(rpacket.meta))
            return rpacket

    def run(self):
        global logger
        global slist
        global mountpoint
        global misc
        global master
        
        logger.log("INFO", "RUN", "execute all tasks")
        taskl = misc.readtask()
        print("ready to execute "+str(len(taskl))+" tasks")
        master.feed_task(taskl)

        while True:
            sleep(1)
            print(str(len(master.smap)+len(master.emap))+"    "+str(len(taskl)))
            if len(master.smap)+len(master.emap) == len(taskl):
                break

        packet=Packet("", "RUN", master.smap, master.emap, 0, None, None)
        return packet
        '''
        rpacket = tcpclient.sendallpacket(packet)
        if len(rpacket.misc) > 0:
            print("execution failed on "+str(rpacket.tlist))
            print("there are "+str(len(rpacket.misc))+" tasks remaining")
            print("rerun the failed tasks on "+str(slist))
            tlist = list(slist)
            for ip in rpacket.tlist:
                tlist.remove(ip)
            packet = Packet("", "EXECUTE", {}, {}, 0, tlist, rpacket.misc)
            rrpacket = tcpclient.sendallpacket(packet) 
            rpacket.meta.update(rrpacket.meta)
            rpacket.data.update(rrpacket.data)
            logger.log("INFO", "EXECUTE", "second round execution finished "+str(rrpacket.meta))
        if sum(rpacket.meta.values()) != 0:
            logger.log("ERROR", "EXECUTE", "execution failed "+str(rpacket.meta)+"\n"+str(rpacket.data))
            return rpacket
        else:
            logger.log("INFO", "EXECUTE", "execution finished "+str(rpacket.meta))
            return rpacket
        '''


    '''
    below are POSIX interface
    '''
    def chmod(self, path, mode):
        global logger
        global misc
        logger.log("INFO", "chmod", path+", "+str(mode))
        if path in self.meta: 
            self.meta[path]['st_mode'] &= 0o770000 
            self.meta[path]['st_mode'] |= mode
        else:
            logger.log("INFO", "CHMOD", "chmod sent to remote server "+path+" "+str(mode))
            #send a chmod message to remote server
            tcpclient = TCPClient()
            #chmod, needs to update to all metadata replicas
            ips = misc.findserver(path)
            for ip in ips:
                tcpclient = TCPClient(ip)
                packet = Packet(path, "CHMOD", {}, {}, 0, [ip], mode)
                ret=tcpclient.sendpacket(packet)
                if ret != 0:
                    logger.log("ERROR", "chmod", path+" with "+str(mode)+" failed on "+ip)
            
    def chown(self, path, uid, gid):
        global logger
        logger.log("INFO", "chown", path+", "+str(uid)+", "+str(gid))

    def create(self, path, mode):
        global logger
        global misc
        global localip
        #if path in self.recovery:
        #    path = path+".bak"
        logger.log("INFO", "CREATE", path+", "+str(mode))
        self.cmeta[path] =  dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                     st_size=0, st_ctime=time(), st_mtime=time(), 
                                     st_atime=time(), location=[localip], key=misc.hash(path), task="", e_recovery=0.0)
        self.cdata[path]=bytearray()
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        global logger
        global misc
        global localip
        logger.log("INFO", "getattr", path)
        #if path in self.recovery:
        #    if path in self.cmeta:
        #        return self.cmeta[path]
        #    else:
        #        raise OSError(ENOENT, '')


        #flag, check exception here
        ips = misc.findserver(path)
        ip = ips[0]
        logger.log("INFO", "getattr", "metadata of "+path+" host at "+ip)
        if path in self.meta:
            logger.log("INFO", "getattr", "metadata of "+path+" is self.meta ")
            return self.meta[path]
        elif path in self.cmeta:
            logger.log("INFO", "getattr", "metadata of "+path+" is self.cmeta ")
            return self.cmeta[path]

        if ip == localip:
            raise OSError(ENOENT, '')
        elif len(path) > 7 and path[:7] == "/.Trash":
            raise OSError(ENOENT, '')
        else:
            for ip in ips:
                if ip != localip:
                    logger.log("INFO", "GETATTR", "getattr sent to remote server: "+path)
                    tcpclient = TCPClient()
                    packet = Packet(path, "GETATTR", None, None, None, [ip], None)
                    ret = tcpclient.sendpacket(packet)
                    #return code 256 indicates there is an exception in sendpacket()
                    if ret.ret == 256:
                        continue
                    if not ret.meta:
                        continue
                        #raise OSError(ENOENT, '')
                    else:
                        self.meta[path]=ret.meta[path]
                        return self.meta[path]
            raise OSError(ENOENT, '')
       
    def getxattr(self, path, name, position=0):
        global logger
        logger.log("INFO", "getxattr", path+", "+name)
        #if empty return b''
        try:
            if path in self.cmeta:
                return self.cmeta[path][name]
            elif path in self.meta:
                return self.meta[path][name]
            else:
                global misc
                #flag, check exception here
                ips = misc.findserver(path)
                for ip in ips:
                    packet = Packet(path, "GETATTR", None, None, None, [ip], None)
                    tcpclient = TCPClient()
                    ret = tcpclient.sendpacket(packet)
                    if ret.ret == 256:
                        continue
                    if not ret.meta:
                        return b''
                    else:
                        return ret.meta[name]
        except KeyError:
            return b''

    def listxattr(self, path):
        global logger
        logger.log("INFO", "listxattr", path)
        if path in self.cmeta:
            return self.cmeta[path].keys()
        elif path in self.meta[path]:
            return self.meta[path].keys()
        else:
            global misc
            #flag, check exception here
            ips = misc.findserver(path)
            for ip in ips:
                packet = Packet(path, "GETATTR", None, None, None, [ip], None)
                tcpclient = TCPClient()
                ret = tcpclient.sendpacket(packet)
                if ret.ret == 256:
                    continue
                if not ret.meta:
                    raise OSError(ENOENT, '')
                else:
                    return ret.meta[path].keys()

    def mkdir(self, path, mode):
        global logger
        global slist
        logger.log("INFO", "MKDIR", path+", "+str(mode))
        parent = os.path.dirname(path)
        if parent not in self.meta:
            logger.log("ERROR", "MKDIR", parent+" does not exist")
            raise FuseOSError(ENOENT) 
        else:
            packet = Packet(path, "MKDIR", {}, {}, 0, slist, mode)
            tcpclient = TCPClient()
            rpacket = tcpclient.sendallpacket(packet)
            if rpacket.ret != 0:
                logger.log("ERROR", "MKDIR", "creating dir: "+path+" failed")
                raise FuseOSError(ENOENT) 
            else:
                self.local_mkdir(path, mode)

    def open(self, path, flags):
        global logger
        #if path in self.recovery:
        #    path = path+".bak"
        logger.log("INFO", "open", path+", "+str(flags))
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        global logger
        global misc
        global resilience_option
        #if path in self.recovery:
        #    path = path+".bak"
        #logger.log("INFO", "READ", path+", "+str(size)+", "+str(offset))

        if path in self.data:
            return bytes(self.data[path][offset:offset + size])
        elif path in self.cdata:
            return bytes(self.cdata[path][offset:offset+size])
        else:
            ips = self.meta[path]['location']
            #r = random.uniform(0, len(ips))
            #ip = ips[int(r)]
            for ip in ips:
                logger.log("INFO", "READ", "read sent to remote server "+path+" "+ip)
                packet = Packet(path, "READ", {}, {}, 0, [ip], [size, offset])
                tcpclient = TCPClient()
                rpacket = tcpclient.sendpacket(packet)
                if not rpacket.data:
                    logger.log("ERROR", "READ", "remote read on "+path+" failed on "+ip)
                    if len(ips) > 1:
                        continue
                    elif resilience_option == 0:
                        logger.log("ERROR", "READ", "file "+path+" is lost due to failure on "+ip)
                    elif resilience_option == 1 or resilience_option == 3:
                        self.rerun(path)
                        #newpath = path+".bak"
                        #return bytes(self.cdata[newpath][offset:offset + size])
                        return bytes(self.cdata[path][offset:offset + size])
                else:
                    self.data[path] = rpacket.data[path]
                    return bytes(self.data[path][offset:offset + size])

    def rerun(self, path):
        global logger
        global mountpoint
        logger.log("INFO", "RERUN", "Recover start "+path+" by running ("+self.meta[path]['task']+")")
        task = self.meta[path]['task']
        if len(path) > len(mountpoint) and path[:len(mountpoint)]==mountpoint:
            path = path[len(mountpoint):]
        
        self.recovery[path] = 0
        #newpath = path+".bak"
        #self.cmeta[path] =  dict(st_mode=(S_IFREG | 33188), st_nlink=1,
        #                             st_size=0, st_ctime=time(), st_mtime=time(), 
        #                             st_atime=time(), location=[localip], key=misc.hash(path), task="", e_recovery=0.0)
        #self.cdata[newpath]=bytearray()

        recover_dir = "/dev/shm/scratch"
        recover_file = recover_dir+path
        print("********"+recover_file+"********")
        if os.path.isfile(recover_file):
            statinfo = os.stat(recover_file)
            if statinfo.st_size == 0:
                os.remove(recover_file)
        if not os.path.isfile(recover_file):
            os.makedirs(recover_dir, exist_ok=True)
            new_task=""
            words = task.split()
            new_task = words[0]
            for word in words[1:]:
                if os.path.isfile(word):
                    if len(word) > len(mountpoint) and word[:len(mountpoint)] == mountpoint:
                        in_word = word[len(mountpoint)+1:] 

                        dirname = os.path.dirname(in_word)
                        filename = os.path.basename(in_word)
                        if dirname != "":
                            os.makedirs(os.path.join(recover_dir, dirname), exist_ok=True)
    
                        if word[len(mountpoint):] not in self.recovery:   
                            shutil.copy(word, os.path.join(recover_dir, dirname))
                        else:
                            recover_file=os.path.join(recover_dir, dirname)+"/"+filename
                        new_task = new_task+" "+os.path.join(os.path.join(recover_dir, dirname), filename)
                    else:
                        new_task = new_task+" "+word
                else:
                    new_task = new_task+" "+word
            print(new_task)        
            p = subprocess.Popen(new_task, shell=True, cwd=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            p.wait()
            stdout, stderr = p.communicate()
            print(stderr)

        fd = open(recover_file, 'rb')
        self.cdata[path] = fd.read()
        fd.close()

        logger.log("INFO", "RERUN", "Recover finish "+path+" by running ("+self.meta[path]['task']+")")

    def readdir(self, path, fh):
        global logger
        global slist
        logger.log("INFO", "readdir", path)
        if path not in self.meta:
            logger.log("ERROR", "READDIR", path+" does not exist")
            return FuseOSError(ENOENT)
        else:
            packet = Packet(path, "READDIR", {}, {}, 0, slist, fh)
            tcpclient = TCPClient()
            rpacket = tcpclient.sendallpacket(packet)
            self.meta.update(rpacket.meta)
            #filtering local metadata in the parent dir of path
            rlist = ['.', '..']
            for m in self.meta:
                if m != '/' and path == os.path.dirname(m):
                    b = os.path.basename(m)
                    rlist.append(b)
            return rlist
        
    def readlink(self, path):
        global logger
        logger.log("INFO", "readlink", path)
        pass

    def removexattr(self, path, name):
        #not implemented yet
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        global logger
        global misc
        global localip
        logger.log("INFO", "rename", "old: "+old+", new: "+new)
        '''
        oldhash = misc.hash(old)
        newhash = misc.hash(new)

        if old in self.meta:
            self.cmeta[new] = self.meta[old]
        else:    
            self.cmeta[new] = self.getattr(old, None)
 
        if old in self.cdata:
            self.cdata[new] = self.cdata[old]
            self.cdata.pop(old)
        else:
            ip = self.cmeta[new]['location'][0]
            logger.log("INFO", "READ", "read sent to remote server "+old+" "+ip)
            packet = Packet(old, "READ", {}, {}, 0, [ip], [0, 0])
            tcpclient = TCPClient()
            rpacket = tcpclient.sendpacket(packet)
            if not rpacket.data:
                logger.log("ERROR", "READ", "remote read on "+path+" failed on "+ip)
                return None
            else:
                self.cdata[new] = rpacket.data[old]

        self.cmeta[new]['key'] = misc.hash(new)
        self.cmeta[new]['location'] = [localip]
        
        if old in self.meta:
            self.meta.pop(old)

        #in this rename oepration, all replicas of metadata should be removed
        ips = misc.findserver(old)
        for ip in ips:
            if ip != localip:
                tcpclient = TCPClient()
                packet = Packet(old, "RMMETA", None, None, None, [ip], None)
                ret = tcpclient.sendpacket(packet)
    
        self.release(new, 0)
        '''
    def rmdir(self, path):
        #rmdir is a two step procedure
        #Step 1, remove the dir path and return the file meta within
        #Step 2, remove all file data on all nodes
        global logger
        logger.log("INFO", "rmdir", path)
        if path not in self.meta:
            logger.log("ERROR", "RMDIR", path+" does not exist")
            return FuseOSError(ENOENT)
        else:
            for m in list(self.meta.keys()):
                if os.path.dirname(m) == path and S_ISDIR(self.meta[m]['st_mode']):
                    self.rmdir(m)
            packet = Packet(path, "RMDIR", {}, {}, 0, slist, [])
            tcpclient = TCPClient()
            rpacket = tcpclient.sendallpacket(packet)
            logger.log("INFO", "RMDIR", "removed "+str(rpacket.misc))
            return 0

    def setxattr(self, path, name, value, options, position=0):
        # not implemented yet
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        global logger
        logger.log("INFO", "symlink", "target: "+target+", source:"+source)
        pass

    def truncate(self, path, length, fh=None):
        global logger
        global misc
        #if path in self.recovery:
        #    path = path+".bak"
        logger.log("INFO", "truncate", path+", "+str(length))

        #if path in self.cdata:
            #self.cdata[path] = self.cdata[path][:length]
            #self.cmeta[path]['st_size'] = length
        #else:
            #print("truncate sent to remote server")
            #ip = misc.findserver(path)
            #packet = Packet(path, "truncate", None, None, None, ip, length)
            #tcpclient = TCPClient()
            #ret = tcpclient.sendpacket(packet)
            #if ret != 0:
            #    logger.log("ERROR", "truncate", "failed on "+path+" with length: "+str(length))

    def unlink(self, path):
        #Unlink is not well funcitoning for the moment
        global logger
        global localip
        global misc
        logger.log("INFO", "UNLINK", path)
        if path in self.recovery and path in self.meta:
            self.stale[path] = self.meta.pop(path)
        if path in self.cmeta:
            self.cmeta.pop(path)
        '''
        #unlink is a two step procedure, first, we need to find the metadata of this file then remove the meta
        #second, clear the actual data 
        tcpclient = TCPClient()
        dst = None
        #flag, check exception here
        ips = misc.findserver(path)
        for ip in ips:
            if ip == localip:
                dst = self.meta[path]['location'][0]
                self.meta.pop(path)
            else:
                packet = Packet(path, "UNLINK", {}, {}, 0, [ip], None)
                ret = tcpclient.sendpacket(packet)
                if not ret.meta:
                    logger.log("ERROR", "UNLINK", "unlink "+path+" failed")
                    raise FuseOSError(ENOENT)
                else:
                    dst = ret.meta[path]['location'][0]
        if path in self.meta:
            self.meta.pop(path)

        if not dst:
            logger.log("ERROR", "UNLINK", "unlink "+path+" failed")
            raise FuseOSError(ENOENT)
        else:
            #need to update file location with a list
            ip=dst
            if ip == localip:
                self.data.pop(path)
            else:
                packet = Packet(path, "REMOVE", {}, {}, 0, [ip], None)
                ret = tcpclient.sendpacket(packet)
                if ret.ret != 0:
                    logger.log("ERROR", "UNLINK", "remove "+path+" failed")
            if path in self.data:
                self.data.pop(path)
        '''        
    def utimens(self, path, times=None):
        global logger
        logger.log("INFO", "utimens", path)
        pass

    def write(self, path, data, offset, fh):
        global logger
        global misc
        #if path in self.recovery:
        #    path = path+".bak"
        #logger.log("INFO", "write", path+", length: "+str(len(data))+", offset: "+str(offset))
        #write to the right place
        if path in self.cdata:
            if offset == len(self.cdata[path]):
                self.cdata[path].extend(data)
            else:    
                self.cdata[path] = self.cdata[path][:offset]+data
        if path in self.data:
            if offset == len(self.data[path]):
                self.data[path].extend(data)
            else:    
                self.data[path] = self.data[path][:offset]+data
        if path not in self.cdata:
            print("write sent to remote server")
            #ip = misc.findserver(path)
            #packet = Packet(path, "locate", None, None, None, ip, None)
            #tcpclient = TCPClient()
            #ret = tcpclient.sendpacket(packet)
            #packet = packet(path, "write", None, None, None, ret, [data, offset])
            #ret = tcpclient.sendpacket(packet)
            
        #update the metadata
        if path in self.cmeta:
            self.cmeta[path]['st_size'] = len(self.cdata[path])
            #self.cmeta[path]['st_size'] = self.cmeta[path]['st_size']+len(data)
            if path in self.meta:
                self.meta[path] = self.cmeta[path]
        else:
            if path in self.recovery:
                pass
            else:
                print("write+update+meta sent to remote server")
            #ip = misc.findserver(path)
            #packet = Packet(path, "updatesize", None, None, None, ip, data)
            #tcpclient = TCPClient()
            #ret = tcpclient.sendpacket(packet)
        return len(data)    
            
    def release(self, path, fh):
        global logger
        global misc
        global localip
        global bandwidth
        logger.log("INFO", "RELEASE", path)
        #this release function should update metadta to all replications
        
        if path in self.recovery:
            return 0

        ips = misc.findserver(path)

        if path in self.cmeta and path not in self.meta:
            self.cmeta[path]['e_recovery'] = 1.0*self.cmeta[path]['st_size']/bandwidth
            if path not in self.data:
                self.data[path] = self.cdata[path]
            self.meta[path] = self.cmeta[path] 
            while not path in self.data:
                continue
            ret = 0
            for ip in ips:
                if ip == localip:
                    logger.log("INFO", "RELEASE", "release sent to local server: "+path+" "+ip)
                    continue
                else:
                    logger.log("INFO", "RELEASE", "release sent to remote server: "+path+" "+ip)
                    tempdict = dict()
                    tempdict[path] = self.cmeta[path]
                    packet = Packet(path, "RELEASE", tempdict, None, None, [ip], None)
                    tcpclient = TCPClient()
                    rpacket = tcpclient.sendpacket(packet)
                    if rpacket.ret != 0:
                        logger.log("ERROR", "RELEASE", path+" failed")
                    ret = ret + rpacket.ret  

            print(path+": "+str(self.meta[path]))
            return ret        
        else:
            if path in self.meta:
                print(path+": "+str(self.meta[path]))
            return 0

    def type(self, src, typedef):
        #This type function can transform the format of a given file
        #Supported tranformations are:
        #File-to-row_table, file-to-dir
        #File-to-column_table, file-to-dir
        #File-to-row_matrix, file-to-dir
        #File-to-column_matrix, file-to-dir
        #File-to-tile_matrix, file-to-dir

        #In the current implemetation, we assume the file is local to the login node
        global logger
        global slist
        global mountpoint
        global misc
        global ddict
        ddict = defaultdict(bytes)
        tcpclient = TCPClient()
        apath = src[len(mountpoint):]
        logger.log("INFO", "TYPE", "Transfer "+apath +" to type "+typedef)
        
        if src[:len(mountpoint)] != mountpoint:
            logger.log("ERROR", "TYPE", src[:len(mountpoint)]+" is not the mountpoint")
            return 1
        if apath not in self.data:
            logger.log("ERROR", "TYPE", apath+" does not exist")
            return 1
        elif typedef == "column_table":
            ddict = misc.to_column_table(self.data[apath])
        elif typedef == "row_table":
            ddict = misc.to_row_table(self.data[apath])
        elif typedef == "column_matrix":
            ddict = misc.to_column_matrix(self.data[apath])
        elif typedef == "row_matrix":
            ddict = misc.to_row_matrix(self.data[apath])
        elif typedef == "tile_matrix":
            ddict = misc.to_tile_matrix(self.data[apath])
        else:
            logger.log("ERROR", "transformation "+typedef+" is not supported")

        basename = os.path.basename(apath)
        dirname = os.path.dirname(apath)
        if dirname == '/':
            dirname = ''
        dirname = dirname+'/.'+basename+"_"+typedef    
        if dirname not in self.meta:
            self.mkdir(dirname, self.meta['/']['st_mode'])
        klist = list(sorted(ddict.keys()))
        for k in klist:
            fname = os.path.join(dirname, k)
            self.create(fname, 33188)
            self.cdata[fname] = ddict.pop(k)
            self.cmeta[fname]['st_size'] = len(self.cdata[fname])
            self.release(fname, 0)
        return 0
    
    def local_chmod(self, path, mode):
        global logger
        logger.log("INFO", "local_chmod", path+", "+str(mode))
        pass

    def local_chown(self, path, uid, gid):
        global logger
        logger.log("INFO", "local_chown", path+", "+str(uid)+", "+str(gid))
        pass

    def local_create(self, path, mode, ip):
        global logger
        logger.log("INFO", "local_create", path+", "+str(mode)+", "+str(ip))
        pass

    def local_getxattr(self, path, name, position=0):
        global logger
        logger.log("INFO", "local_getxattr", path+", "+str(name))
        pass

    def local_listxattr(self, path):
        global logger
        logger.log("INFO", "local_listxattr", path)
        pass

        
    def local_mkdir(self, path, mode):
        global logger
        logger.log("INFO", "local_mkdir", path+", "+str(mode))
        parent = os.path.dirname(path)
        if parent not in self.meta:
            logger.log("ERROR", "local_mkdir", parent+" does not exist")
            return 1
        else:
            nlink = self.meta[parent]['st_nlink']
            self.meta[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=nlink+1, st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time(), location=[], key=None, task="", e_recovery=0.0)
            self.meta[parent]['st_nlink'] += 1
            return 0

    def local_readdir(self, path, fh):
        global logger
        logger.log("INFO", "local_readdir", path)
        rdict = dict()
        for m in self.meta:
            if path == os.path.dirname(m) and not S_ISDIR(self.meta[m]['st_mode']):
                rdict[m] = self.meta[m]
        return rdict        

    def local_readlink(self, path):
        global logger
        logger.log("INFO", "local_readlink", path)
        pass

    def local_removexattr(self, path, name):
        global logger
        logger.log("INFO", "local_removeattr", path+", "+name)
        pass

    def local_read(self, path, size, offset):
        global logger
        logger.log("INFO", "local_read", path+", "+str(offset)+", "+str(size))

        if path in self.data:
            tempdict = defaultdict(bytes)
            tempdict[path] = self.data[path]
            return tempdict
        else:
            logger.log("ERROR", "local_read", path+" is not in self.data")
            return None

    def local_rename(self, old, new):
        global logger
        logger.log("INFO", "local_rename", "old: "+old+" new: "+new)
        pass

    def local_insert(self, path, data):
        global logger
        logger.log("INFO", "local_insert", path)
        if path not in self.data:
            #self.data[path] = data[path]
            self.data[path] = data
        else:
            #self.data[path].extend(data[path])
            self.data[path].extend(data)
        return 0

    def local_rmdir(self, path):
        global logger
        logger.log("INFO", "local_rmdir", path)
        rlist = []
        for m in list(self.meta.keys()):
            if os.path.dirname(m) == path or m == path:
                self.meta.pop(m)
                if m not in rlist:
                    rlist.append(m)
        for m in list(self.cmeta.keys()):
            if os.path.dirname(m) == path or m == path:
                self.cmeta.pop(m)
                if m not in rlist:
                    rlist.append(m)
        for m in list(self.data.keys()):
            if os.path.dirname(m) == path:
                self.data.pop(m)
                if m not in rlist:
                    rlist.append(m)
        for m in list(self.cdata.keys()):
            if os.path.dirname(m) == path:
                self.cdata.pop(m)
                if m not in rlist:
                    rlist.append(m)
        return rlist
            
    def local_setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def local_symlink(self, target, source, ip):
        global logger
        logger.log("INFO", "local_symlink", "target: "+target+" source: "+source)

    def local_truncate(self, path, length, fh=None):
        global logger
        logger.log("INFO", "local_truncate", path+", "+str(length))

    def local_unlink(self, path):
        global logger
        logger.log("INFO", "local_unlink", path)
        if path not in self.meta:
            return None
        else:
            rdict = dict()
            rdict[path] = self.meta[path]
            self.meta.pop(path)
            return rdict

    def local_remove(self, path):
        global logger
        global misc
        logger.log("INFO", "local_remove", path)
        
        if path not in self.data:
            return 1
        else:
            self.data.pop(path)
            return 0

    def local_rmmeta(self, path):    
        global logger
        global misc
        logger.log("INFO", "local_rmmeta", path)
        if path in self.cmeta:
            self.cmeta.pop(path)
        if path in self.meta:
            self.meta.pop(path)
        return 0
    
    def local_utimens(self, path, times=None):
        global logger
        logger.log("INFO", "local_utimens", path)

    def local_append(self, path, offset, data):
        global logger
        logger.log("INFO", "local_append", path+", "+str(offset)+", "+str(len(data)))

    def local_getattr(self, path):
        global logger
        logger.log("INFO", "local_getattr", path)
        if path in self.meta:
            tempdict = dict()
            tempdict[path] = self.meta[path]
            return tempdict
        else:
            return None

    def local_release(self, path, meta):
        global logger
        logger.log("INFO", "local_release", path)
        if path not in self.meta:
            self.meta[path] = meta[path]
            
    def local_updatemeta(self, path, meta):
        global logger
        logger.log("INFO", "local_updatemeta", path+" location: "+str(meta[path]['location']))
        if path not in self.meta:
            self.meta[path] = meta[path]
            return 0
        else:
            if self.meta[path] == "":
                self.meta[path] = meta[path]
            elif len(meta[path]['location']) > len(self.meta[path]['location']):
                self.meta[path] = meta[path]
            elif self.meta[path]['task'] == "":
                self.meta[path] = meta[path]
            return 0

    def local_load(self, dst, filel):
        global logger
        logger.log("INFO", "local_load", "loading "+str(filel)+" to "+dst)
        for f in filel:
            basename = os.path.basename(f)
            dstf = os.path.join(dst, basename)
            self.create(dstf, 33188)
            fd = open(f, 'rb')
            
            self.cdata[dstf] = fd.read()
            fd.close()
            self.cmeta[dstf]['st_size'] = len(self.cdata[dstf])
            self.release(dstf, 0)
        logger.log("INFO", "local_load", "finished loading "+str(filel)+" to "+dst)
    def local_dump(self, pairl):
        global logger
        logger.log("INFO", "local_dump", "dumping"+str(pairl))
        for p in pairl:
            hvalue, fname = p
            fd = open(fname, 'wb')
            fd.write(self.data[fname])
            fd.close()
        logger.log("INFO", "local_dump", "finished dumping")    


class TCPClient():
    def __init__(self):
        self.bufsize = 1048576
        self.psize = 16

    def init_port(self, ip, port):
        global logger
        #logger.log("INFO", "TCPclient_init_port", "connecting to "+ip+":"+str(port))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.setblocking(True)
        #sock.settimeout(3.0)
        connected = 0
        while connected == 0:
            try:
                sock.connect((ip, port))
            except Exception:
                logger.log("ERROR", "TCPclient_init_port", "connect "+ip+" failed")
                return None
                #sleep(1)
                #continue
            else:
                connected = 1
                #logger.log("INFO", "TCPclient_init_port", "connected to "+ip+":"+str(port))
        return sock
    
    def init_server(self, host, port):
        global logger
        server = None
        while server == None: 
            try:
                logger.log("INFO", "TCPclient_init_server", "starting server TCP socket")
                server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.bind((host, port))
                server.listen(5)
            except socket.error as msg:
                logger.log("ERROR", "TCPclient_init_server", msg)
                server = None
                sleep(1)
            else:
                logger.log("INFO", "TCPclient_init_server", "server TCP socket started")
                return server

    def sendpacket(self, packet):
        global logger
        global localip
        global amfora
        logger.log("INFO", "TCPclient_sendpacket() start", packet.path+" "+packet.op+" to "+str(packet.tlist))
        tlist = list(packet.tlist)
        #Packet sent to a single host
        if len(packet.tlist) > 0:
            try:
                #initialize the socket
                s = self.init_port(packet.tlist[0], 55000)            
                if not s:
                    raise Exception('connect failed')

                #dump packet into binary format
                bpacket = pickle.dumps(packet)

                #get the packet size
                length = len(bpacket)
                #logger.log("INFO", "TCPclient.sendpacket()", "ready to send "+str(length)+" bytes")

                #paddling the length of the packet to a 16 bytes number
                slength = str(length)
                while len(slength) < self.psize:
                    slength = slength + '\0'

                #send the length, and wait for an ack    
                s.send(bytes(slength, 'utf-8'))
                s.recv(1)
            
                #send the bpacket data
                sent = 0
                while sent < length:
                    if length - sent > self.bufsize:
                        sent_iter = s.send(bpacket[sent:sent+self.bufsize])
                    else:
                        sent_iter = s.send(bpacket[sent:])
                    sent = sent + sent_iter
                    #logger.log("INFO", "TCPclient.sendpacket()", "send "+str(sent_iter)+" bytes")
                #logger.log("INFO", "TCPclient.sendpacket()", "totally send "+str(sent)+" bytes")    

                #receive the size of the returned packet
                data = s.recv(self.psize)
                length = int(data.decode('utf8').strip('\0'))
                s.send(bytes('0', 'utf8'))
                data = b''
                rect = 0
                while rect < length:
                    if length - rect > self.bufsize:
                        temp = s.recv(self.bufsize)
                    else:
                        temp = s.recv(length-rect)
                    rect = rect + len(temp)
                    data = data + temp
                    #logger.log("INFO", "TCPclient.sendpacket()", "receive "+str(len(temp))+" bytes")
                #logger.log("INFO", "TCPclient.sendpacket()", "totally receive "+str(len(data))+" bytes")    
                s.close()
                packet = pickle.loads(data)
            except Exception as msg:
                logger.log("ERROR", "TCPclient_sendpacket()", "Exception: "+str(msg))
                packet = Packet("", "", None, None, 256, None, None)
                return packet
            finally:
                logger.log("INFO", "TCPclient_sendpacket() finish", packet.path+" "+packet.op+" to "+str(tlist))
                return packet

    def one_sided_sendpacket(self, packet, port):
        global logger
        global localip
        global amfora
        logger.log("INFO", "TCPclient_one_sided_sendpacket()", packet.path+" "+packet.op)

        #Packet sent to a single host
        if len(packet.tlist) > 0:
            try:
                #initialize the socket
                s = self.init_port(packet.tlist[0], port)            

                if not s:
                    raise Exception('connect failed')

                #dump packet into binary format
                bpacket = pickle.dumps(packet)

                #get the packet size
                length = len(bpacket)
                logger.log("INFO", "TCPclient.one_sided_sendpacket()", "ready to send "+str(length)+" bytes")

                #paddling the length of the packet to a 16 bytes number
                slength = str(length)
                while len(slength) < self.psize:
                    slength = slength + '\0'

                #send the length, and wait for an ack    
                s.send(bytes(slength, 'utf-8'))
                s.recv(1)
            
                #send the bpacket data
                sent = 0
                while sent < length:
                    if length - sent > self.bufsize:
                        sent_iter = s.send(bpacket[sent:sent+self.bufsize])
                    else:
                        sent_iter = s.send(bpacket[sent:])
                    sent = sent + sent_iter
                    #logger.log("INFO", "TCPclient.one_sided_sendpacket()", "send "+str(sent_iter)+" bytes")
                #logger.log("INFO", "TCPclient.one_sided_sendpacket()", "totally send "+str(sent)+" bytes")    
            except socket.error as msg:
                logger.log("ERROR", "TCPclient.one_sided_sendpacket()", "Socket Exception: "+str(msg))
            except Exception as msg:
                logger.log("ERROR", "TCPclient.one_sided_sendpacket()", "Other Exception: "+str(msg))
                return None
        return 0
    
    def sendallpacket(self, packet):
        global logger
        global misc
        global sdict
        global localip
        global shuffleserver
        global shufflethread

        logger.log("INFO", "TCPclient_sendallpacket", packet.op+" "+packet.path+" "+str(packet.misc))
        #partition the target list in the packet to reorganize the nodes into an MST
        #The current implementation of partition_list() is MST
        #SEQ algorithm can be implemented by modifying the code in partition_list()
        olist = misc.partition_list(packet.tlist)
        #start an asynchronous server to receive acknowledgements of collective operations
        logger.log("INFO", "TCPClient_sendallpacket", "num_targets: "+str(olist))
        if len(olist) > 0:
            if packet.op == "EXECUTE":
                server = self.init_server('', 55005)
            else:    
                server = self.init_server('', 55001)
        else:
            server=None
        #rdict tracks the immediate children of this node
        #initiate the status of each node, 0 means not returned,
        #1 means returned
        rdict = dict()
        for ol in olist:
            rdict[ol[0]] = 0

        colthread = CollectiveThread(server, rdict, packet)
        colthread.start()

        unfinished_tasks = []
        failed_ip = []

        meta = dict(packet.meta)
        data = dict(packet.data)
        total_tasks = 0
        total_files = 0
        total_target = 0
        if packet.op == "EXECUTE":
            total_tasks = len(packet.misc)
            total_target = len(packet.tlist)
        elif packet.op == "SCATTER":
            total_files = len(meta)
        elif packet.op == "LOAD":
            total_files = len(packet.misc)
            total_target = len(packet.tlist)

        for ol in olist:
            if packet.op == "SCATTER":
                mdict = {}
                ddict = {}
                klist = list(sorted(packet.misc))
                num_files = math.ceil(len(ol)*total_files/(len(packet.tlist)-1))
                #print(str(num_files)+" "+str(len(ol))+" "+str(total_files)+" "+str(len(packet.tlist)))
                
                oklist = []
                for i in range(num_files):
                    if len(klist) > 0:
                        k = klist.pop()
                        packet.misc.remove(k)
                        mdict[k] = meta.pop(k)
                        ddict[k] = data.pop(k)
                        oklist.append(k)
                op = Packet(packet.path, packet.op, mdict, ddict, packet.ret, ol, sorted(oklist))    
            elif packet.op == "EXECUTE":
                taskl = []
                #num_tasks = math.ceil(len(packet.misc)/2)
                num_tasks = math.ceil(len(ol)*total_tasks/total_target)
                total_tasks = total_tasks - num_tasks
                total_target = total_target - len(ol) 
                for i in range(num_tasks):
                    taskl.append(packet.misc.pop())
                op = Packet(packet.path, packet.op, packet.meta, packet.data, packet.ret, ol, taskl) 
            elif packet.op == "LOAD":
                filel = []
                #num_files = math.ceil(len(packet.misc)/2)
                num_files = math.ceil(len(ol)*total_files/total_target)
                total_files = total_files-num_files
                total_target = total_target - len(ol)
                #print(str(num_files)+" "+str(len(ol))+" "+str(total_files)+" "+str(len(packet.tlist)))
                for i in range(num_files):
                    filel.append(packet.misc.pop())
                op = Packet(packet.path, packet.op, packet.meta, packet.data, packet.ret, ol, filel)
            else:
                op = Packet(packet.path, packet.op, packet.meta, packet.data, packet.ret, ol, packet.misc)
                
            ret = self.one_sided_sendpacket(op, 55000)
            if ret == None:
                logger.log("ERROR", "TCPclient.sendallpacket()", "Remove  "+op.tlist[0]+" from rlist")
                if op.op == "EXECUTE":
                    unfinished_tasks.extend(op.misc)
                    failed_ip.append(op.tlist[0])
                rdict.pop(op.tlist[0])
        
        #start shuffleserver as a thread and shuffleclient    
        if packet.op == "SHUFFLE_START":
            logger.log("INFO", "TCPclient_sendallpacket()", "ready to start shuffle")
            global amfora
            global localip
            #global slist
            #global sdict
            #global shuffleserver
            retdict = defaultdict(bytes)
            for ip in slist:
                retdict[ip] = b''
            nextip = misc.nextip()
            logger.log("INFO", "TCPclient_sendallpacket()", "Shuffle_start: "+str(packet.misc))
            if localip in packet.misc[0]:
                sdict = misc.shuffle(packet.misc[0][localip])
            else:
                sdict = misc.shuffle(dict())
            #logger.log("INFO", "TCPclient_sendallpacket()", "Shuffle_start sdict: "+str(sdict))
            retdict[localip] = sdict.pop(localip)
            
            shuffleserver.reset(retdict, packet)
            while not shufflethread.is_alive():
                shufflethread.start()    

            #s_server = self.init_server('', 55003)
            #shuffleserver = ShuffleServer(s_server, retdict, packet)
            #shuffleserver.start()

        elif packet.op =="SHUFFLE":
            logger.log("INFO", "TCP_sendallpacket()", "ready to shuffle")

            #nextip = misc.nextip()
            iplist = misc.reorderip()
            logger.log("INFO", "TCP_sendallpacket()", "target ip order: "+str(iplist))
            for ip in iplist:
                tempdict = dict()
                tempdict[ip] = sdict.pop(ip)
                p = Packet(packet.path, "SHUFFLETHREAD", {}, tempdict, 0, [ip], [localip, packet.misc])
                shufflethread.push(p)

            while shuffleserver.status == 1:
                sleep(0.1)

        elif packet.op == "EXECUTE":
            logger.log("INFO", "TCPclient_sendallpacket()", "ready to execute: "+str(len(packet.misc))+" tasks")
            executor = Executor(packet.misc)
            executor.run()
            packet.meta.update(executor.smap)
            packet.data.update(executor.emap)
            packet.misc = unfinished_tasks
            packet.tlist = failed_ip
            logger.log("INFO", "TCPclient_sendallpacket()", "finished executing: "+str(len(packet.meta.keys())+len(packet.data.keys()))+" tasks")
        elif packet.op == "LOAD":
            global amfora
            logger.log("INFO", "TCPclient_sendallpacket()", "read to load: "+str(packet.misc))
            amfora.local_load(packet.path, packet.misc)
            logger.log("INFO", "TCPclient_sendallpacket()", "finished loading: "+str(packet.misc))
        elif packet.op == "DUMP":
            #global amfora
            #global localip
            logger.log("INFO", "TCPclient_sendallpacket()", "read to dump: "+str(packet.misc))
            if localip in packet.misc:
                amfora.local_dump(packet.misc[localip])
            logger.log("INFO", "TCPclient_sendallpacket()", "finished dumping: "+str(packet.misc))
        else:
            pass
        #while colthread.is_alive():
        #    sleep(0.1)
        #    pass
            #logger.log("INFO", "TCPclient_sendallpacket()", "waiting for colthread to finish")
        colthread.join()
        if server:
            server.shutdown(socket.SHUT_RDWR)
            server.close()
        return packet    
    
class ShuffleServer(threading.Thread):
    def __init__(self, name, port):
        threading.Thread.__init__(self)
        self.id = name
        self.host = ''
        self.port = port
        self.server = None
        self.bufsize = 1048576
        self.psize = 16
        self.retdict = None
        self.packet = None
        self.dst = None
        self.status = 0 #0 means stall, 1 means running

    def reset(self, retdict, packet):
        self.retdict = retdict
        self.packet = packet
        self.dst = packet.misc[1]
        self.status = 1

    def open_socket(self):
        global logger
        try:
            logger.log("INFO", "ShuffleServer_opensocket", "Open server socket")
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.host, self.port))
            self.server.listen(5)
        except socket.error as msg:
            logger.log("ERROR", "ShuffleServer_opensocket", msg)
            self.server = None


    def run(self):
        global logger
        global localip
        global amfora
        global slist
        global shufflethread
        logger.log("INFO", "ShuffleServer_run()", "thread started")
        self.open_socket()
        counter = 0
        
        tcpclient = TCPClient()
        nextip = misc.nextip()

        while True:
            #return if this server receives slist-1 packets
            if counter == len(slist)-1 and len(slist) > 1:
                logger.log("INFO", "ShufflerServer_run()", "received "+str(counter)+" packets, now terminates")
                counter = 0
                index = slist.index(localip)
                if self.dst == '/':
                    fname = '/'+str(index)
                else:
                    fname = self.dst+'/'+str(index)
                amfora.create(fname, 33188)    
                temp = bytearray()
                #logger.log("INFO", "ShuffleServer_run()", "retdict: "+str(self.retdict))
                for k in self.retdict:
                    temp.extend(self.retdict[k])
                    logger.log("INFO", "ShuffleServer_run()", "processing record from "+k)
                amfora.cdata[fname] = bytes(temp)
                amfora.cmeta[fname]['st_size'] = len(amfora.cdata[fname])
                amfora.release(fname, 0)
                logger.log("INFO", "ShuffleServer_run()", "shuffle finished")        
                self.status = 0
                del self.retdict
                #break
            else:
                conn, addr = self.server.accept()
                try:
                    peer = conn.getpeername()[0]
                    data = conn.recv(self.psize)
                    length = int(data.decode('utf8').strip('\0'))
                    logger.log("INFO", "ShuffleServer_run()", "ready to receive "+str(length)+" bytes")
                    conn.send(bytes('0', 'utf8'))
                    data = b''
                    rect = 0
                    while rect < length:
                        if length - rect > self.bufsize:
                            temp = conn.recv(self.bufsize)
                        else:
                            temp = conn.recv(length-rect)
                        rect = rect + len(temp)
                        data = data + temp
                        logger.log("INFO", "ShuffleServer_run()", "receive "+str(len(temp))+" bytes")
                    logger.log("INFO", "ShufflerServer_run()", "totally receive "+str(len(data))+" bytes")    

                    #keep local data
                    tp = pickle.loads(data)
                    self.retdict[tp.misc[0]]=tp.data.pop(localip)
                    
                    #logger.log("INFO", "ShufflerServer_run()", "remained dict: "+str(tp.data))
                    '''
                    if len(tp.data.keys()) > 0:
                        packet = Packet(tp.path, tp.op, tp.meta, tp.data, tp.ret, [nextip], tp.misc)
                        shufflethread.push(packet)
                    '''    

                except socket.error as msg:
                    logger.log("ERROR", "ShuffleServer_run()", "Socket Exception: "+str(msg))
                except Exception as msg:
                    logger.log("ERROR", "ShuffleServer_run()", "Other Exception: "+str(msg))
                finally:
                    #conn.close()
                    counter = counter + 1
                    logger.log("INFO", "ShuffleServer_run()", "received "+str(counter)+" packets")
        #now the shuffle server has all shuffled data 
        '''
        logger.log("INFO", "ShuffleServer_run()", "now server has "+str(len(self.retdict.keys()))+" records")            
        index = slist.index(localip)
        if self.dst == '/':
            fname = '/'+str(index)
        else:
            fname = self.dst+'/'+str(index)
        amfora.create(fname, 33188)    
        temp = bytearray()
        logger.log("INFO", "ShuffleServer_run()", "retdict: "+str(self.retdict))
        for k in self.retdict:
            temp.extend(self.retdict[k])
            logger.log("INFO", "ShuffleServer_run()", "processing record from "+k)
        amfora.cdata[fname] = bytes(temp)
        amfora.cmeta[fname]['st_size'] = len(amfora.cdata[fname])
        amfora.release(fname, 0)
        logger.log("INFO", "ShuffleServer_run()", "shuffle finished")        
        self.server.shutdown(socket.SHUT_RDWR)
        self.server.close()
        self.server=None
        '''

class ShuffleThread(threading.Thread):
    def __init__(self):
        global misc
        threading.Thread.__init__(self)
        self.queue = queue.Queue()
        self.nextip = misc.nextip()
        self.sock = None
        self.psize = 16
        self.bufsize = 1048576

    def init_port(self, ip, port):
        global logger
        logger.log("INFO", "ShuffleThread_init_port", "connecting to "+ip+":"+str(port))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connected = 0
        while connected == 0:
            try:
                sock.connect((ip, port))
            except socket.error:
                logger.log("ERROR", "ShuffleThread_init_port", "connect "+ip+" failed, try again")
                sleep(1)
                continue
            else:
                connected = 1
                logger.log("INFO", "ShuffleThread_init_port", "connected to "+ip+":"+str(port))
        return sock

    def one_sided_sendpacket(self, packet):
        global logger
        global localip
        global amfora
        logger.log("INFO", "ShuffleThread_one_sided_sendpacket()", packet.path+" "+packet.op)

        try:
            #dump packet into binary format
            bpacket = pickle.dumps(packet)
            
            #get the packet size
            length = len(bpacket)
            logger.log("INFO", "ShuffleThread.one_sided_sendpacket()", "ready to send "+str(length)+" bytes")

            #paddling the length of the packet to a 16 bytes number
            slength = str(length)
            while len(slength) < self.psize:
                slength = slength + '\0'

            #send the length, and wait for an ack    
            self.sock.send(bytes(slength, 'utf-8'))
            self.sock.recv(1)
            
            #send the bpacket data
            sent = 0
            while sent < length:
                if length - sent > self.bufsize:
                    sent_iter = self.sock.send(bpacket[sent:sent+self.bufsize])
                else:
                    sent_iter = self.sock.send(bpacket[sent:])
                sent = sent + sent_iter
                logger.log("INFO", "ShuffleThread.one_sided_sendpacket()", "send "+str(sent_iter)+" bytes")
            logger.log("INFO", "ShuffleThread.one_sided_sendpacket()", "totally send "+str(sent)+" bytes")    
        except socket.error as msg:
            logger.log("ERROR", "ShuffleThread.one_sided_sendpacket()", "Socket Exception: "+str(msg))
        except Exception as msg:
            logger.log("ERROR", "ShuffleThread.one_sided_sendpacket()", "Other Exception: "+str(msg))

    def push(self, packet):
        self.queue.put(packet, True, None)

    def run(self):    
        global logger
        #self.sock = self.init_port(self.nextip, 55003)
        while True:
            packet = self.queue.get(True, None)
            logger.log("INFO", "ShuffleThread", "Sending pakcet to "+self.nextip)
            tcpclient = TCPClient()
            tcpclient.one_sided_sendpacket(packet, 55003)
            #self.one_sided_sendpacket(packet)
            #del packet
 

class TCPserver(threading.Thread):
    def __init__(self, workerid, port):
        threading.Thread.__init__(self)
        self.id = workerid
        self.host = ''
        self.port = port
        self.psize = 16
        self.bufsize = 1048576
        self.server = None

    def open_socket(self):
        global logger
        try:
            logger.log("INFO", "TCPserver_opensocket", "Open server socket")
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.host, self.port))
            self.server.listen(5)
        except socket.error as msg:
            logger.log("ERROR", "TCPserver_opensocket", msg)
            self.server = None

    def run(self):
        global logger
        global amfora
        global tcpqueue
        global localip
        self.open_socket()
        
        while True:
            conn, addr = self.server.accept()
            try:
                data = conn.recv(self.psize)
                slength = str(data.decode('utf8').strip('\0'))
                logger.log("INFO", "TCPServer.run()", "ready to receive "+slength+" bytes")
                length = int(slength)
                conn.send(bytes('0', 'utf-8'))
                rect = 0
                bpacket = b''

                while rect < length:
                    if length - rect > self.bufsize:
                        temp = conn.recv(self.bufsize)
                    else:
                        temp = conn.recv(length-rect)
                    rect = rect + len(temp)
                    bpacket = bpacket+temp
                    #logger.log("INFO", "TCPServer.run()", "receive "+str(len(temp))+" bytes")
                #logger.log("INFO", "TCPServer.run()", "totally receive "+str(len(bpacket))+" bytes")    
            except socket.error:
                logger.log("ERROR", "TCPserver_run", "socket exception when receiving message "+str(socket.error))
                break

            packet = pickle.loads(bpacket)
            logger.log("INFO", "TCPserver_run", "received: "+packet.op+" "+packet.path+" "+str(packet.tlist))
            tcpqueue.put([conn, packet], True, None)

class TCPworker(threading.Thread):
    def __init__(self, workerid):
        threading.Thread.__init__(self)
        self.id = workerid
        self.psize = 16
        self.bufsize = 1048576

    def sendpacket(self, sock, packet):
        logger.log("INFO", self.id+" TCPWorker_sendpacket() start", packet.path+" "+packet.op+" to "+str(packet.tlist))
        try:
            #dump packet into binary format
            bpacket = pickle.dumps(packet)

            #get the packet size
            length = len(bpacket)
            #logger.log("INFO", "TCPworker.sendpacket()", "ready to send "+str(length)+" bytes")

            #paddling the length of the packet to a 16 bytes number
            slength = str(length)
            while len(slength) < self.psize:
                slength = slength + '\0'

            #send the length, and wait for an ack    
            sock.send(bytes(slength, 'utf-8'))
            sock.recv(1)
            
            #send the bpacket data
            sent = 0
            while sent < length:
                if length - sent > self.bufsize:
                    sent_iter = sock.send(bpacket[sent:sent+self.bufsize])
                else:
                    sent_iter = sock.send(bpacket[sent:])
                sent = sent + sent_iter
                #logger.log("INFO", "TCPworker.sendpacket()", "send "+str(sent_iter)+" bytes")
            #logger.log("INFO", "TCPworker.sendpacket()", "totally send "+str(sent)+" bytes")    
        except socket.error as msg:
            logger.log("ERROR", self.id+" TCPworker.sendpacket()", "Socket Exception: "+str(msg))
        except Exception as msg:
            logger.log("ERROR", self.id+" TCPworker.sendpacket()", "Other Exception: "+str(msg))
        finally:
            logger.log("INFO", self.id+" TCPWorker_sendpacket() finish", packet.path+" "+packet.op+" to "+str(packet.tlist))
            return sent

    def run(self):
        global logger
        global tcpqueue
        global locaip
        global amfora
        
        while True:
            conn, packet = tcpqueue.get(True, None)
            
            if packet.op == 'CREATE':
                filename = packet.path
                mode = packet.misc
                remoteip, remoteport = conn.getpeername()
                ret = amfora.local_create(filename, mode, remoteip)
                p = Packet(packet.path, packet.op, None, None, 0, [remoteip], None)
                self.sendpacket(conn, p)
                conn.close()
            elif packet.op == 'RELEASE':
                filename = packet.path
                ret = amfora.local_release(filename, packet.meta)
                remoteip, remoteport = conn.getpeername()
                p = Packet(packet.path, packet.op, None, None, 0, [remoteip], None)
                self.sendpacket(conn, p)
                conn.close()
            elif packet.op == 'READ':
                filename = packet.path
                remoteip, remoteport = conn.getpeername()
                ret = amfora.local_read(filename, packet.misc[0], packet.misc[1])
                p = Packet(packet.path, packet.op, None, ret, 0, [remoteip], None)
                self.sendpacket(conn, p)
                conn.close()
            elif packet.op == 'COPY':
                key = el[0]
                ret = ramdisk.data[key]
                data = pickle.dumps(ret)
                length = len(data)
                dsize = str(length)
                while len(dsize) < 10:
                    dsize = dsize + '\0'
                conn.send(bytes(dsize, 'utf8'))
                conn.recv(1)
                tcp_big = TCP_big()
                tcp_big.send(conn, data, length)
                conn.recv(1)
                conn.close()
            elif packet.op == 'GETATTR':
                filename = packet.path
                remoteip, remoteport = conn.getpeername()
                ret = amfora.local_getattr(filename)
                p = Packet(packet.path, packet.op, ret, None, 0, [remoteip], None)
                self.sendpacket(conn, p)
                conn.close()
            elif packet.op == 'GETXATTR':
                filename = el[0]
                ret = None
                if filename in ramdisk.files:
                    ret = ramdisk.files[filename].get('attrs', {})
                conn.send(pickle.dumps(ret))
            elif packet.op == 'CHMOD':
                filename = packet.path
                mode = packet.misc
                ret = amfora.local_chmod(filename, mode)
                remoteip, remoteport = conn.getpeername()
                p = Packet(packet.path, packet.op, ret, None, 0, [remoteip], None)
                self.sendpacket(conn, p)
                conn.close()
            elif packet.op == 'CHOWN':
                filename = el[0]
                uid = int(el[2])
                gid = int(el[3])
                ret = ramdisk.local_chown(filename, uid, gid)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif packet.op == 'TRUNCATE':
                filename = el[0]
                length = int(el[2])
                ramdisk.local_truncate(filename, length)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
            elif packet.op == 'READDIR':
                path = packet.path
                remoteip, remoteport = conn.getpeername()
                tcpclient = TCPClient()
                rpacket = tcpclient.sendallpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "READDIR", "reading dir: "+path+" failed")
                rpacket.tlist = [remoteip]    
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
                conn.close()
            elif packet.op == 'RMDIR':
                path = packet.path
                remoteip, remoteport = conn.getpeername()
                tcpclient = TCPClient()
                rpacket = tcpclient.sendallpacket(packet)
                rpacket.tlist = [remoteip]    
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
                conn.close()

            elif packet.op == 'MKDIR':
                path = packet.path
                mode = packet.misc
                remoteip, remoteport = conn.getpeername()
                tcpclient = TCPClient()
                rpacket = tcpclient.sendallpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "MKDIR", "creating dir: "+path+" failed")
                rpacket.tlist = [remoteip]    
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
                conn.close()
            elif packet.op == 'UNLINK':
                path = packet.path
                remoteip, remoteport = conn.getpeername()
                ret = amfora.local_unlink(path)
                if not ret:
                    logger.log("ERROR", "UNLINK", "unlinking "+path+" failed")
                p = Packet(packet.path, packet.op, ret, {}, 0, [remoteip], None)
                self.sendpacket(conn, p)
                conn.close()
            elif packet.op == 'REMOVE':
                path = packet.path
                remoteip, remoteport = conn.getpeername()
                ret = amfora.local_remove(path)
                if ret != 0:
                    logger.log("ERROR", "REMOVE", "removing "+path+" failed")
                p = Packet(packet.path, packet.op, {}, {}, ret, [remoteip], None)
                self.sendpacket(conn, p)
                conn.close()
            elif packet.op == 'RMMETA':
                path = packet.path
                remoteip, remoteport = conn.getpeername()
                ret = amfora.local_rmmeta(path)
                if ret != 0:
                    logger.log("ERROR", "REMOVE", "removing "+path+" failed")
                p = Packet(packet.path, packet.op, {}, {}, ret, [remoteip], None)
                self.sendpacket(conn, p)
                conn.close()
            elif packet.op == 'SYMLINK':
                path = el[0]
                source = el[2]
                remoteip, remoteport = conn.getpeername()
                ramdisk.local_symlink(path, source, remoteip)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
            elif packet.op == 'READLINK':
                path = el[0]
                data = ramdisk.local_readlink(path)
                conn.send(bytes(data, "utf8"))
                conn.close()
            elif packet.op == 'RENAME':
                old = el[0]
                new = el[2]
                data = ramdisk.local_rename(old, new)
                conn.send(bytes(str(data), "utf8"))
                conn.close()
            elif packet.op == 'INSERTDATA':
                path = packet.path
                data = packet.data
                remoteip, remoteport = conn.getpeername()
                ret = amfora.local_insert(path, data)
                p = Packet(packet.path, packet.op, None, None, ret, [remoteip], None)
                self.sendpacket(conn, p)
                conn.close()
            elif packet.op == 'APPENDDATA':
                path = el[0]
                offset = int(el[3])
                data = conn.recv(msize)
                content = pickle.loads(data)
                data = ramdisk.local_append(path, offset, content)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
            elif packet.op == 'UPDATE':
                path = packet.path
                meta = packet.meta
                remoteip, remoteport = conn.getpeername()
                ret = amfora.local_updatemeta(path, meta)
                p = Packet(packet.path, packet.op, None, None, ret, [remoteip], None)
                self.sendpacket(conn, p)
                conn.close()
            elif packet.op == 'MULTICAST':
                path = packet.path
                remoteip, remoteport = conn.getpeername()
                tcpclient = TCPClient()
                rpacket = tcpclient.sendallpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "MULTICAST", "multicasting file: "+path+" failed")
                rpacket.tlist = [remoteip]    
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
                conn.close()
            elif packet.op == 'GATHER':
                path = packet.path
                remoteip, remoteport = conn.getpeername()
                tcpclient = TCPClient()
                rpacket = tcpclient.sendallpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "GATHER", "gathering dir: "+path+" failed")
                rpacket.tlist = [remoteip]    
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
                conn.close()
            elif packet.op == 'SCATTER':
                path = packet.path
                remoteip, remoteport = conn.getpeername()
                #keep part of the data local
                num_files = math.ceil(len(packet.meta)/len(packet.tlist))
                logger.log("INFO", "SCATTER", "This node keeps "+str(num_files)+" files")    
                rmeta = dict()
                for i in range(num_files):
                    k = packet.misc.pop()
                    v = packet.meta.pop(k)
                    amfora.meta[k] = v
                    amfora.meta[k]['location'] = [localip]
                    amfora.data[k] = packet.data.pop(k)
                    amfora.cdata[k] = amfora.data[k]
                    rmeta[k] = amfora.meta[k]
                    logger.log("INFO", "SCATTER", "This node keeps "+k)
                tcpclient = TCPClient()
                rpacket = tcpclient.sendallpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "SCATTER", "scattering dir: "+path+" failed")
                rpacket.tlist = [remoteip]    
                rpacket.meta.update(rmeta)
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
                conn.close()
            elif packet.op == 'SHUFFLE_START':
                path = packet.path
                dst = packet.misc[1]
                remoteip, remoteport = conn.getpeername()
                tcpclient = TCPClient()
                logger.log("INFO", "TCPserver_shuffle_start", str(packet.misc))
                rpacket = tcpclient.sendallpacket(packet)

                if rpacket.ret != 0:
                    logger.log("ERROR", "SHUFFLE_SHUFFLE_START", "start shuffling from "+path+" to "+dst+" failed")
                rpacket.tlist = [remoteip]
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
                conn.close()
            elif packet.op == 'SHUFFLE':
                path = packet.path
                dst = packet.misc
                remoteip, remoteport = conn.getpeername()
                tcpclient = TCPClient()
                logger.log("INFO", "TCPserver_shuffle", str(packet.misc))
                rpacket = tcpclient.sendallpacket(packet)

                if rpacket.ret != 0:
                    logger.log("ERROR", "SHUFFLE", "shuffling from "+path+" to "+dst+" failed")
                rpacket.tlist = [remoteip]
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
                conn.close()
            elif packet.op  == 'EXECUTE':
                path = packet.path
                remoteip, remoteport = conn.getpeername()
                tcpclient = TCPClient()
                rpacket = tcpclient.sendallpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "EXECUTE", "Executing failed")
                rpacket.tlist = [remoteip]    
                tcpclient.one_sided_sendpacket(rpacket, 55005)    
                conn.close()
            elif packet.op  == 'LOAD':
                path = packet.path
                remoteip, remoteport = conn.getpeername()
                tcpclient = TCPClient()
                rpacket = tcpclient.sendallpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "LOAD", "Loading failed")
                rpacket.tlist = [remoteip]    
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
                conn.close()
            elif packet.op  == 'DUMP':
                path = packet.path
                remoteip, remoteport = conn.getpeername()
                tcpclient = TCPClient()
                rpacket = tcpclient.sendallpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "DUMP", "Dumping failed")
                rpacket.tlist = [remoteip]    
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
                conn.close()
            else:
                logger.log("ERROR", "TCPserver.run()", "Invalid op "+packet.op)


class Interfaceserver(threading.Thread):
    def __init__(self, workerid, port):
        threading.Thread.__init__(self)
        self.id = workerid
        self.host = ''
        self.port = port
        self.psize = 16
        self.bufsize = 1048576
        self.server = None

    def open_socket(self):
        global logger
        try:
            logger.log("INFO", "Interfaceserver_opensocket", "Open server socket")
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.host, self.port))
            self.server.listen(5)
        except socket.error as msg:
            logger.log("ERROR", "Interfaceserver_opensocket", msg)
            self.server = None

    def sendpacket(self, sock, packet):
        logger.log("INFO", "Interfaceserver_sendpacket()", "sending packet to "+str(packet.tlist))
        try:
            #dump packet into binary format
            bpacket = pickle.dumps(packet)

            #get the packet size
            length = len(bpacket)
            #logger.log("INFO", "Interfaceserver_sendpacket()", "ready to send "+str(length)+" bytes")

            #paddling the length of the packet to a 16 bytes number
            slength = str(length)
            while len(slength) < self.psize:
                slength = slength + '\0'

            #send the length, and wait for an ack    
            sock.send(bytes(slength, 'utf-8'))
            sock.recv(1)
            
            #send the bpacket data
            sent = 0
            while sent < length:
                if length - sent > self.bufsize:
                    sent_iter = sock.send(bpacket[sent:sent+self.bufsize])
                else:
                    sent_iter = sock.send(bpacket[sent:])
                sent = sent + sent_iter
                #logger.log("INFO", "Interfaceserver_sendpacket()", "send "+str(sent_iter)+" bytes")
            #logger.log("INFO", "Interfaceserver_sendpacket()", "totally send "+str(sent)+" bytes")    
        except socket.error as msg:
            logger.log("ERROR", "Interfaceserver__sendpacket()", "Socket Exception: "+str(msg))
        except Exception as msg:
            logger.log("ERROR", "Interfaceserver_sendpacket()", "Other Exception: "+str(msg))
        finally:
            return sent

    def run(self):
        global logger
        global amfora
        #global executor
        self.open_socket()
        
        while True:
            conn, addr = self.server.accept()
            try:
                data = conn.recv(self.bufsize)
            except socket.error:
                logger.log("ERROR", "Interfaceserver_run", "socket exception when receiving message "+str(socket.error))
                break

            msg = data.decode("utf8").strip()
            logger.log("INFO", "Interfaceserver_run", "received: "+str(msg))
            el = msg.split('#')
            if el[1] == 'MULTI':
                path = el[0]
                algo = el[2]
                ret = amfora.multicast(path, algo)
                self.sendpacket(conn, ret)
                conn.close()
            elif el[1] == 'GATHER':
                path = el[0]
                algo = el[2]
                ret, ddict, mdict = amfora.gather(path, algo)
                self.sendpacket(conn, ret)
                conn.close()
            elif el[1] == 'ALLGATHER':
                path = el[0]
                algo = el[2]
                ret = amfora.allgather(path, algo)
                self.sendpacket(conn, ret)
                conn.close()
            elif el[1] == 'SCATTER':
                path = el[0]
                algo = el[2]
                ret = amfora.scatter(path, algo)
                self.sendpacket(conn, ret)
                conn.close()
            elif el[1] == 'SHUFFLE':
                path = el[0]
                algo = el[2]
                dst = el[3]
                ret = amfora.shuffle(path, algo, dst)
                self.sendpacket(conn, ret)
                conn.close()
            elif el[1] == 'EXECUTE':
                ret = amfora.execute()
                self.sendpacket(conn, ret)
                conn.close()
            elif el[1] == 'RUN':
                ret = amfora.run()
                self.sendpacket(conn, ret)
                conn.close()
            elif el[1] == 'STATE':
                ret = executor.state()
                self.sendpacket(conn, ret)
                conn.close()
            elif el[1] == 'LOAD':
                src = el[0]
                dst = el[2]
                ret = amfora.load(src, dst)
                self.sendpacket(conn, ret)
                conn.close()
            elif el[1] == 'DUMP':
                src = el[0]
                dst = el[2]
                ret = amfora.dump(src, dst)
                self.sendpacket(conn, ret)
                conn.close()
            elif el[1] == 'TYPE':
                src = el[0]
                typedef = el[2]
                ret = amfora.type(src, typedef)
                rpacket = Packet(src, typedef, None, None, ret, None, None)
                self.sendpacket(conn, rpacket)
                conn.close()
            

class Misc():
    def __init__(self):
        pass

    def findserver(self, fname):
        global slist
        global logger
        global replication_factor
        rlist = []
        value = zlib.adler32(bytes(fname, 'utf8')) & 0xffffffff
        for i in range(replication_factor):
            if i < len(slist):
                rlist.append(slist[(value%(len(slist))+i)%len(slist)])
        logger.log("INFO", "Misc.findserver()", "metadata of "+fname+" are stored on: "+str(rlist))        
        return rlist

    def hash(self, fname):
        return zlib.adler32(bytes(fname, 'utf8')) & 0xffffffff

    def partition_list(self, slist):
        tlist = []
        global localip
        vlist = list(slist)
        vlist.remove(localip)
        #comment the following code to disable SEQ algorith
        while len(vlist) > 0:
            temp = []
            ip = vlist.pop()
            temp.append(ip)
            tlist.append(temp)    
        return tlist
        #uncomment the following code to enable MST algorithm
        #while len(vlist) > 0:
        #    temp = []
        #    for i in range(math.ceil(len(vlist)/2)):
        #        ip = vlist.pop()
        #        temp.append(ip)
        #    tlist.append(temp)    
        #return tlist
    
    def nextip(self):
        global localip
        global slist
        index = slist.index(localip)
        if index+1 == len(slist):
            nextindex = 0
        else:
            nextindex = index + 1
        nextip = slist[nextindex]
        return nextip

    def findneighbor(self):
        global localip
        global slist
        global replication_factor
        index = slist.index(localip)
        
        rlist=[]
        for i in range(replication_factor-1):
            if i < len(slist):
                rlist.append(slist[(index+i+1)%len(slist)])
        return rlist

    def reorderip(self):
        global localip
        global slist
        rlist = []
        index = slist.index(localip)
        for i in range(len(slist)-1):
            newindex = index+i+1
            if newindex >= len(slist):
                newindex = newindex - len(slist)
            rlist.append(slist[newindex])
        return rlist
    
    def shuffle(self, hlist):
        #hlist is a list containing the hash values in local storage
        global logger
        global amfora
        global slist
        hdict = defaultdict(bytes)
        for ip in slist:
            hdict[ip] = bytearray()
        for h in hlist:
            if h in amfora.data and h in amfora.cdata:
                bdata = amfora.data[h]
                lines = bdata.split(b'\n')
                logger.log("INFO", "MISC_shuffle()", "lines: "+str(len(lines)))
                for line in lines[:len(lines)-1]:
                    #change to regular expression in next release
                    #m = re.match(b"(?P<key>\w+)\s+(?P<value>\w+)", line)
                    #k = m.group('key')
                    #v = m.group('value')
                    k, v = line.split()
                    #logger.log("INFO", "MISC_shuffle()", "key: "+k.decode('utf8')+" values: "+v.decode('utf8'))
                    value = zlib.adler32(k) & 0xffffffff
                    ip = slist[value%len(slist)]
                    hdict[ip].extend(line+b'\n')
            else:
                logger.log("ERROR", "MISC_shuffle()", "hash value "+str(h)+" is not in local storage")
        return hdict        

    def readtask(self):
        fd = open("/tmp/amfora-task.txt", 'r')
        lines = fd.readlines()
        taskl = []
        for l in lines:
            task = Task(l.strip('\n'))
            taskl.append(task)
        return taskl

    def to_column_table(self, data):
        ddict = defaultdict(bytes)
        rows = data.split(b'\n')
        num_rows = len(rows)-1
        if len(rows) == 0:
            return ddict
        columns = len(rows[0].split(b'\t'))
        for i in range(columns):
            ddict[str(i)] = bytearray()
        row_tag = 0    
        for row in rows[:len(rows)-1]:
            row_tag = row_tag+1
            elist = row.split(b'\t')
            for i in range(len(elist)):
                ddict[str(i)].extend(elist[i]+b'\n')
                #if row_tag < num_rows:
                #    ddict[str(i)].extend(elist[i]+b'\t')
                #else:
                #    ddict[str(i)].extend(elist[i]+b'\n')
        return ddict        

    def to_row_table(self, data):
        global slist
        ddict = defaultdict(bytes)
        rows = data.split(b'\n')
        if len(rows) == 0:
            return ddict

        num_rows = math.ceil((len(rows)-1)/len(slist))
        for i in range(len(slist)):
            ddict[str(i)] = bytearray()
        for i in range(len(rows)-1):
            key = str(int(i/num_rows))
            ddict[key].extend(rows[i]+b'\n')
        return ddict

    def to_column_matrix(self, data):
        global slist
        ddict = defaultdict(bytes)
        rows = data.split(b'\n')
        if len(rows) == 0:
            return ddict
        columns = len(rows[0].split(b'\t'))
        num_col = math.ceil(columns/len(slist))
        for i in range(len(slist)):
            ddict[str(i)] = bytearray()
        for row in rows[:len(rows)-1]:
            elist = row.strip(b'\n').split(b'\t')
            row_tag = 0
            for i in range(len(elist)):
                key = str(int(i/num_col))
                ddict[key].extend(elist[i])
                row_tag = row_tag+1
                if row_tag < num_col:
                    ddict[key].extend(b'\t')
                else:
                    ddict[key].extend(b'\n')
                    row_tag = 0
        return ddict

    def to_row_matrix(self, data):
        global slist
        ddict = defaultdict(bytes)
        rows = data.split(b'\n')
        if len(rows) == 0:
            return ddict

        num_rows = math.ceil((len(rows)-1)/len(slist))
        for i in range(len(slist)):
            ddict[str(i)] = bytearray()
        for i in range(len(rows)-1):
            key = str(int(i/num_rows))
            ddict[key].extend(rows[i]+b'\n')
        return ddict

    def to_tile_matrix(self, data):
        global slist
        ddict = defaultdict(bytes)
        rows = data.split(b'\n')
        if len(rows) == 0:
            return ddict

        sqrt_num_files = int(math.sqrt(len(slist)))
        for i in range(sqrt_num_files):
            for j in range(sqrt_num_files):
                ddict[str(i)+str(j)] = bytearray()

        num_rows = int(len(rows)/sqrt_num_files)
        columns = rows[0].split(b'\t')
        num_columns = int(len(columns)/sqrt_num_files)

        for i in range(len(rows)-1):
            elist = rows[i].split(b'\t')
            column_tag = 0
            for j in range(len(elist)):
                key = str(int(i/num_rows))+str(int(j/num_columns))
                ddict[key].extend(elist[j])
                column_tag = column_tag+1
                if column_tag < num_columns:
                    ddict[key].extend(b'\t')
                else:
                    ddict[key].extend(b'\n')
                    column_tag = 0
        return ddict
            
class CollectiveThread(threading.Thread):
    def __init__(self, server, rdict, packet):
        threading.Thread.__init__(self)
        self.server = server
        self.rdict = rdict
        self.packet = packet
        self.bufsize = 1048576
        self.psize = 16

    def run(self):
        global logger
        global localip
        global amfora
        logger.log("INFO", "CollThread_run()", "thread started")
        while True:
            #return if all immediate childrens return
            summ = sum(self.rdict.values())
            if summ == len(self.rdict):
                break

            #if this is the leaf node
            #the above code can check leaf too
            #if len(self.packet.tlist)==1 and self.packet.tlist[0]==localip:
            #    pass
            else:
                try:
                    conn, addr = self.server.accept()
                    peer = conn.getpeername()[0]
                    data = conn.recv(self.psize)
                    length = int(data.decode('utf8').strip('\0'))
                    #logger.log("INFO", "CollThread_run()", "ready to receive "+str(length)+" bytes")
                    conn.send(bytes('0', 'utf8'))
                    data = b''
                    rect = 0
                    while rect < length:
                        if length - rect > self.bufsize:
                            temp = conn.recv(self.bufsize)
                        else:
                            temp = conn.recv(length-rect)
                        rect = rect + len(temp)
                        data = data + temp
                        #logger.log("INFO", "CollThread_run()", "receive "+str(len(temp))+" bytes")
                    #logger.log("INFO", "CollThread_run()", "totally receive "+str(len(data))+" bytes")    
                    conn.close()
                    tp = pickle.loads(data)
                    self.packet.meta.update(tp.meta)
                    self.packet.data.update(tp.data)
                    self.packet.ret = self.packet.ret | tp.ret
                except socket.error as msg:
                    logger.log("ERROR", "CollThread_run()", "Socket Exception: "+str(msg))
                except Exception as msg:
                    logger.log("ERROR", "CollThread_run()", "Other Exception: "+str(msg))
                finally:
                    conn.close()
                    self.rdict[peer] = 1

        if self.packet.op == "MKDIR":
            ret = amfora.local_mkdir(self.packet.path, self.packet.misc)
            #mkdir raises FuseOSError(ENOENT) if parent dir does not exist
            self.packet.ret = self.packet.ret | ret
            logger.log("INFO", "CollThread_run()", self.packet.op+" "+self.packet.path+" finished")
        elif self.packet.op == "READDIR":
            ret = amfora.local_readdir(self.packet.path, self.packet.misc)
            self.packet.meta.update(ret)
            self.packet.ret = self.packet.ret | 0
            logger.log("INFO", "CollThread_run()", self.packet.op+" "+self.packet.path+" finished")
        elif self.packet.op == "RMDIR":
            ret = amfora.local_rmdir(self.packet.path)
            self.packet.misc.extend(ret)
            self.packet.ret = self.packet.ret | 0
            logger.log("INFO", "CollThread_run()", self.packet.op+" "+self.packet.path+" finished")
        elif self.packet.op == "MULTICAST":
            amfora.meta.update(self.packet.meta)
            amfora.data.update(self.packet.data)
            self.packet.ret = self.packet.ret | 0
            logger.log("INFO", "CollThread_run()", self.packet.op+" "+self.packet.path+" finished")
        elif self.packet.op == "GATHER":
            if localip in self.packet.misc:
                for k in self.packet.misc[localip]:
                    self.packet.data[k] = amfora.data[k]
            self.packet.ret = self.packet.ret | 0
            logger.log("INFO", "CollThread_run()", self.packet.op+" "+self.packet.path+" finished")
        elif self.packet.op == "SCATTER":
            self.packet.ret = self.packet.ret | 0
            #self.packet.meta = {}
            self.packet.data = {}
            logger.log("INFO", "CollThread_run()", self.packet.op+" "+self.packet.path+" finished")
        elif self.packet.op == "SHUFFLE_START":
            self.packet.ret = self.packet.ret | 0
            self.packet.meta = {}
            self.packet.data = {}
            logger.log("INFO", "CollThread_run()", self.packet.op+" "+self.packet.path+" finished")
        elif self.packet.op == "SHUFFLE":
            self.packet.ret = self.packet.ret | 0
            self.packet.meta = {}
            self.packet.data = {}
            logger.log("INFO", "CollThread_run()", self.packet.op+" "+self.packet.path+" finished")
        elif self.packet.op == "EXECUTE":
            self.packet.ret = self.packet.ret | 0
            logger.log("INFO", "CollThread_run()", self.packet.op+" "+self.packet.path+" finished")
        elif self.packet.op == "DUMP":
            self.packet.ret = self.packet.ret | 0
            logger.log("INFO", "CollThread_run()", self.packet.op+" "+self.packet.path+" finished")
        elif self.packet.op == "LOAD":
            self.packet.ret = self.packet.ret | 0
            self.misc = None
            logger.log("INFO", "CollThread_run()", self.packet.op+" "+self.packet.path+" finished")
        else:
            logger.log("ERROR", "CollThread_run()", "operation: "+self.packet.op+" not supported")
   
class Packet():
    def __init__(self, path, op, meta, data, ret, tlist, misc):
        '''
        The packet class defines the packet format used for inter-node communication.
        self.path [string] specifies the file name that is being operated on
        self.op [string] specifies the operation
        self.meta [dict] specifies the metadata needs to be transferred related to this operation
        self.data [defaultdict(bytes)] specifies the file data needs to be transferred related to this operation
        self.ret [int] specifies the return value of the operation
        self.tlist [string[]] specifies the targes that this packet is being routed to
        self.misc [dynamic] specifies the opeartion parameter
        '''
        self.path = path
        self.op = op
        self.meta = meta
        self.data = data
        self.ret = ret
        self.tlist = tlist
        self.misc = misc

class Task():
    def __init__(self, desc):
        global localip
        self.queuetime = time()
        self.starttime = None
        self.endtime = None
        self.desc = desc
        self.ret = None
        randomkey = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
        tempkey = self.desc+str(self.queuetime)+randomkey
        self.key = zlib.adler32(bytes(tempkey, 'utf8')) & 0xffffffff

        
class Executor():
    def __init__(self, tlist):
        self.queue = []
        self.fqueue = []
        self.smap = {}
        self.emap = {}
        self.readyqueue = queue.Queue()
        self.fmap = {} #key-file, value-task
        self.tlist = tlist
        #count how many tasks are there in the beginning for space constraint calulation
        for t in tlist:
            self.readyqueue.put(t, True, None)
    
    def run(self):
        global logger
        global amfora
        global mountpoint
        global resilience_option
        
        logger.log('INFO', 'Executor_run', 'executor started')
        while True:
            if self.readyqueue.empty():
                break
            task = self.readyqueue.get(True, None)
            logger.log('INFO', 'Executor_run start', '----------------------------------')
            logger.log('INFO', 'Executor_run', 'running task: '+task.desc)
            #The following code is to figure out the input and output files of this task,
            #This is a two step procedure
            inlist=[]
            outlist=[]
            files=task.desc.split()
            for f in files:
                if len(f) > len(mountpoint) and f[:len(mountpoint)]==mountpoint:
                    f = f[len(mountpoint):]
                if f in amfora.cdata or f in amfora.data:
                    inlist.append(f)

            task.starttime = time()
            p = subprocess.Popen(task.desc, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            p.wait()
            stdout, stderr = p.communicate()
            task.endtime = time()
            task.ret = p.returncode
            self.smap[task.desc+" "+str(task.key)] = task.ret
            if task.ret != 0:
                self.emap[task.desc+" "+str(task.key)] = stderr

                
            #This is the second step
            for f in files:
                if len(f) > len(mountpoint) and f[:len(mountpoint)]==mountpoint:
                    f = f[len(mountpoint):]
                if f in amfora.cdata and f not in inlist and f not in amfora.recovery:
                    outlist.append(f)
                elif f in amfora.data and f not in inlist:
                    inlist.append(f)
                elif f in amfora.recovery and f not in inlist:
                    inlist.append(f)

            #while f not in amfora.meta:
            #    continue

            logger.log('INFO', 'Executor_run', 'finishing task: '+task.desc)
            logger.log('INFO', 'Executor_run', 'input files: '+str(inlist))
            logger.log('INFO', 'Executor_run', 'output files: '+str(outlist))
            if resilience_option == 1:
                self.replicate_temporal(outlist, task.desc, 0.0)
            elif resilience_option == 2:
                self.replicate_spatial(outlist, task.desc, 0.0)
            elif resilience_option == 3:
                global bandwidth
                global latency
                global replication_factor
                total_data = 0
                #for f in inlist:
                #    total_data = total_data + len(amfora.data[f])
                expected_sum = 0.0
                t_tran = 0.0
                for i in range(len(inlist)):
                    expected_sum = expected_sum+amfora.meta[inlist[i]]['e_recovery']
                expected_temporal = (task.endtime-task.starttime)+1.0*expected_sum/MTTF    
                score_temporal = latency*(replication_factor-1)+expected_temporal

                for f in outlist:
                    logger.log('INFO', 'FILE_META', "file: "+f+", running time: "+str(task.endtime-task.starttime)+", size: "+str(len(amfora.cdata[f]))+", expected_input: "+str(expected_sum))
                    total_data = total_data + len(amfora.cdata[f])
                expected_spatial = latency+1.0*total_data/bandwidth+1.0*latency/MTTF
                score_spatial = (replication_factor-1)*(latency+1.0*total_data/bandwidth)+expected_spatial

                logger.log('INFO', 'Executor_run', "file: "+str(outlist)+"  expected_temporal: "+str(expected_temporal)+"    expected_spatial: "+str(expected_spatial))
                logger.log('INFO', 'Executor_run', "file: "+str(outlist)+"  score_temporal: "+str(score_temporal)+"    score_spatial: "+str(score_spatial))
                if score_temporal > score_spatial:
                    self.replicate_spatial(outlist, task.desc, expected_spatial)
                elif score_temporal <= score_spatial:
                    self.replicate_temporal(outlist, task.desc, expected_temporal)
            logger.log('INFO', 'Executor_run end', '----------------------------------')        
        logger.log('INFO', 'Executor_run', 'all tasks finished')

    #replication functions
    def replicate_temporal(self, outlist, task, e_recovery) :
        global logger
        global misc
        global localip
        global amfora
        logger.log("INFO", "replicate_temporal", "replicate_temporal: "+str(outlist)+" "+task)
        
        for f in outlist:
            ips = misc.findserver(f)

            #insert task to metadata locally
            amfora.cmeta[f]['e_recovery'] = e_recovery
            #amfora.meta[f]['e_recovery'] = e_recovery
            amfora.cmeta[f]['task'] = task
            #amfora.meta[f]['task'] = task
            amfora.meta[f] = amfora.cmeta[f]
            amfora.data[f] = amfora.cdata[f]
            ret = 0    
            for ip in ips:
                if ip == localip:
                    continue
                tempdict = dict()
                tempdict[f] = amfora.cmeta[f]
                packet = Packet(f, "UPDATE", tempdict, None, None, [ip], None)
                tcpclient = TCPClient()
                rpacket = tcpclient.sendpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "UPDATE", f+" failed")
        return 0    


    def replicate_spatial(self, outlist, task, e_recovery) :
        global logger
        global misc
        global localip
        global amfora
        logger.log("INFO", "replicate_spatial", "replicate_spatial: "+str(outlist)+" "+task)
        chunk_size = 64000000

        for f in outlist:
            suc_ip = []
            ips = misc.findneighbor()
            logger.log("INFO", "replicate_spatial", f+" is replicated at: "+str(ips))    

            #replicate data to neighbors
            for ip in ips:
                if ip == localip:
                    continue
                tempdict = dict()
                total_size = len(amfora.cdata[f])
                logger.log("INFO", "replicate_spatial", f+" size is: "+str(total_size))    
                sent_size = 0
                rpacket = None
                if total_size > chunk_size:
                    while sent_size < total_size:
                        if total_size - sent_size > chunk_size:
                            #tempdict[f] = amfora.cdata[f][sent_size:sent_size+chunk_size]
                            data = amfora.cdata[f][sent_size:sent_size+chunk_size]
                            sent_size = sent_size+chunk_size
                        else:
                            #tempdict[f] = amfora.cdata[f][sent_size:]
                            data = amfora.cdata[f][sent_size:]
                            #sent_size = sent_size+len(tempdict[f])
                            sent_size = sent_size+len(data)
                        #packet = Packet(f, "INSERTDATA", None, tempdict, None, [ip], None)
                        packet = Packet(f, "INSERTDATA", None, data, None, [ip], None)
                        tcpclient = TCPClient()
                        rpacket = tcpclient.sendpacket(packet)
                else:
                    #tempdict[f] = amfora.cdata[f]
                    data = amfora.cdata[f]
                    #packet = Packet(f, "INSERTDATA", None, tempdict, None, [ip], None)
                    packet = Packet(f, "INSERTDATA", None, data, None, [ip], None)
                    tcpclient = TCPClient()
                    rpacket = tcpclient.sendpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "INSERTDATA", f+" failed")
                else:
                    suc_ip.append(ip)

            #insert task to metadata locally
            amfora.cmeta[f]['location'].extend(suc_ip)
            amfora.cmeta[f]['task'] = task
            amfora.cmeta[f]['e_recovery'] = e_recovery
            amfora.meta[f] = amfora.cmeta[f]
            amfora.data[f] = amfora.cdata[f]

            #update metadata remotely

            ips = misc.findserver(f)
            ret = 0    
            for ip in ips:
                if ip == localip:
                    continue
                tempdict = dict()
                tempdict[f] = amfora.cmeta[f]
                print(tempdict[f]['location'])
                packet = Packet(f, "UPDATE", tempdict, None, None, [ip], None)
                tcpclient = TCPClient()
                rpacket = tcpclient.sendpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "UPDATE", f+" failed")
        return 0

class Master(threading.Thread):
    def __init__(self, workerid, port):
        threading.Thread.__init__(self)
        self.id = workerid
        self.host = ''
        self.port = port #55007
        self.psize = 16
        self.bufsize = 1048576
        self.server = None
        self.socket_queue = queue.Queue()
        self.task_queue = queue.Queue()
        self.ip_sock_map = dict()
        self.smap = dict()
        self.emap = dict()

    def open_socket(self):
        global logger
        try:
            logger.log("INFO", "Master_opensocket", "Open server socket")
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.host, self.port))
            self.server.listen(5)
        except socket.error as msg:
            logger.log("ERROR", "Master_opensocket", msg)
            self.server = None

    def sendpacket(self, sock, packet):
        logger.log("INFO", "Master_sendpacket()", "sending packet to "+str(packet.tlist))
        try:
            bpacket = pickle.dumps(packet)

            length = len(bpacket)

            slength = str(length)
            while len(slength) < self.psize:
                slength = slength + '\0'

            sock.send(bytes(slength, 'utf-8'))
            sock.recv(1)
            
            #send the bpacket data
            sent = 0
            while sent < length:
                if length - sent > self.bufsize:
                    sent_iter = sock.send(bpacket[sent:sent+self.bufsize])
                else:
                    sent_iter = sock.send(bpacket[sent:])
                sent = sent + sent_iter
        except socket.error as msg:
            logger.log("ERROR", "Master__sendpacket()", "Socket Exception: "+str(msg))
        except Exception as msg:
            logger.log("ERROR", "Master_sendpacket()", "Other Exception: "+str(msg))
        finally:
            return sent

    def feed_task(self, input_task_list):
        logger.log("INFO", "Master_feed-task()", "feeding "+str(len(input_task_list))+" tasks")
        self.reset()
        for task in input_task_list:
            self.task_queue.put(task, True, None)

    def reset(self):
        self.smap.clear()
        self.emap.clear()

    def run(self):
        global logger
        global amfora
        #global executor
        global slist
        self.open_socket()
        
        while True:
            try:
                conn, addr = self.server.accept()
                self.socket_queue.put(conn, True, None)
                self.ip_sock_map[addr[0]] = conn
                if self.socket_queue.qsize() == len(slist):
                    logger.log("INFO", "Master_run", "master now has "+str(self.socket_queue.qsize())+" slaves")
                    break
            except socket.error:
                logger.log("ERROR", "Master_run", "socket exception when accepting connection: "+str(socket.error))
                break
        while True:
            task = self.task_queue.get(True, None)
            sock = self.socket_queue.get(True, None)
            peer = sock.getpeername()[0]
            packet = Packet('/', "TASK", {}, {}, 0, [peer], task)
            self.sendpacket(sock, packet)


class Master_receiver(threading.Thread):
    def __init__(self, workerid, port):
        threading.Thread.__init__(self)
        self.id = workerid
        self.host = ''
        self.port = port #55008
        self.psize = 16
        self.bufsize = 1048576
        self.server = None
        self.socket_queue = queue.Queue()
        self.task_queue = queue.Queue()
        self.ip_sock_map = dict()

    def open_socket(self):
        global logger
        try:
            logger.log("INFO", "Master_receiver_opensocket", "Open server socket")
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.host, self.port))
            self.server.listen(5)
        except socket.error as msg:
            logger.log("ERROR", "Master_opensocket", msg)
            self.server = None

    def recvpacket(self, s):
        logger.log("INFO", "Master_recvpacket()", "receiving a packet from "+str(s.getpeername()[0]))
        try:
            #receive the size of the returned packet
            data = s.recv(self.psize)
            length = int(data.decode('utf8').strip('\0'))
            s.send(bytes('0', 'utf8'))
            data = b''
            rect = 0
            while rect < length:
                if length - rect > self.bufsize:
                    temp = s.recv(self.bufsize)
                else:
                    temp = s.recv(length-rect)
                rect = rect + len(temp)
                data = data + temp
            s.close()
            packet = pickle.loads(data)

        except socket.error as msg:
            logger.log("ERROR", "Master_recvpacket()", "Socket Exception: "+str(msg))
        except Exception as msg:
            logger.log("ERROR", "Master_recvpacket()", "Other Exception: "+str(msg))
        finally:
            return packet

    def run(self):
        global logger
        global amfora
        global master
        global slist
        self.open_socket()
        
        while True:
            try:
                conn, addr = self.server.accept()
                packet = self.recvpacket(conn)
                print(str(packet.meta))
                print(str(packet.data))
                master.smap.update(packet.meta)
                master.emap.update(packet.data)
                master.socket_queue.put(master.ip_sock_map[addr[0]], True, None)
            except socket.error:
                logger.log("ERROR", "Master_run", "socket exception when accepting connection: "+str(socket.error))
                break

class Slave(threading.Thread):
    def __init__(self, workerid, iport, oport):
        threading.Thread.__init__(self)
        self.id = workerid
        self.host = ''
        self.iport = iport
        self.oport = oport
        self.psize = 16
        self.bufsize = 1048576

    def init_port(self, ip, port):
        global logger
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connected = 0
        while connected == 0:
            try:
                sock.connect((ip, port))
            except Exception:
                logger.log("ERROR", "Slave_init_port", "connect "+ip+" failed")
                return None
            else:
                connected = 1
        return sock

    def recvpacket(self, s):
        logger.log("INFO", "Slave_recvpacket()", "receiving a packet from "+str(s.getpeername()[0]))
        try:
            #receive the size of the returned packet
            data = s.recv(self.psize)
            length = int(data.decode('utf8').strip('\0'))
            s.send(bytes('0', 'utf8'))
            data = b''
            rect = 0
            while rect < length:
                if length - rect > self.bufsize:
                    temp = s.recv(self.bufsize)
                else:
                    temp = s.recv(length-rect)
                rect = rect + len(temp)
                data = data + temp
            #s.close()
            packet = pickle.loads(data)

        except socket.error as msg:
            logger.log("ERROR", "Slave__recvpacket()", "Socket Exception: "+str(msg))
        except Exception as msg:
            logger.log("ERROR", "Slave_recvpacket()", "Other Exception: "+str(msg))
        finally:
            return packet

    def sendpacket(self, sock, packet):
        logger.log("INFO", "Slave_sendpacket()", "sending packet to "+str(packet.tlist))
        try:
            bpacket = pickle.dumps(packet)

            length = len(bpacket)

            slength = str(length)
            while len(slength) < self.psize:
                slength = slength + '\0'

            sock.send(bytes(slength, 'utf-8'))
            sock.recv(1)
            
            #send the bpacket data
            sent = 0
            while sent < length:
                if length - sent > self.bufsize:
                    sent_iter = sock.send(bpacket[sent:sent+self.bufsize])
                else:
                    sent_iter = sock.send(bpacket[sent:])
                sent = sent + sent_iter
                #logger.log("INFO", "Interfaceserver_sendpacket()", "send "+str(sent_iter)+" bytes")
            #logger.log("INFO", "Interfaceserver_sendpacket()", "totally send "+str(sent)+" bytes")    
        except socket.error as msg:
            logger.log("ERROR", "Slave_sendpacket()", "Socket Exception: "+str(msg))
        except Exception as msg:
            logger.log("ERROR", "Slave_sendpacket()", "Other Exception: "+str(msg))
        finally:
            return sent

    def run(self):
        global logger
        global slist
        master_ip = slist[0]
        isock = self.init_port(master_ip, self.iport)
        while True:
            packet = self.recvpacket(isock)
            task = packet.misc
            executor = Executor([task])
            executor.run()
            rpacket = Packet("", "TASK", executor.smap, executor.emap, 0, [master_ip], None)
            osock = self.init_port(master_ip, self.oport)
            self.sendpacket(osock, rpacket)


if __name__ == '__main__':
    if len(argv) != 9:
        print(('usage: %s <mountpoint> <amfs.conf> <localip> <replication_factor> <MTTF> <resilience_option> <bandwidth> <latency>' % argv[0]))
        exit(1)
        
    global logger
    logger = Logger("/tmp/amfora.log")
    global mountpoint
    mountpoint = argv[1]
    global localip
    localip = argv[3]
    global replication_factor 
    replication_factor = int(argv[4])
    global MTTF
    MTTF = int(argv[5])
    '''
    resilience_option is an integer,
    0 indicates doing nothing
    1 indicates using temporal replication
    2 indicates using spatial replication
    3 indicates using dynamic replication
    '''
    global resilience_option
    resilience_option = int(argv[6])

    global bandwidth
    bandwidth = int(argv[7])
    global latency
    latency = float(argv[8])

    logger.log("INFO", "main", "resilient feature: replication_factor: "+str(replication_factor)+"  MTTF: "+str(MTTF)+" Resilience_option: "+str(resilience_option)+" Bandwidth: "+str(bandwidth)+"B/s"+" Latency: "+str(latency)+"s")

    global parentip
    parentip = ''

    global misc
    misc = Misc()

    global sdict
    sdict = dict()

    global slist
    slist = []
    fd = open(argv[2], 'r')
    while True:
        line = fd.readline()
        if not line:
            break
        ip = line.strip('\n').strip()
        slist.append(ip)
    logger.log("INFO", "main", "Metadata Server List: "+str(slist))
    
    global amfora
    amfora=Amfora()

    global tcpqueue
    tcpqueue = queue.Queue()

    global shuffleserver
    shuffleserver = ShuffleServer('Shuffleserver', 55003)
    while not shuffleserver.is_alive():
        shuffleserver.start()
        
    global shufflethread
    shufflethread = ShuffleThread()

    tcpserver = TCPserver('TCPserver', 55000)
    while not tcpserver.is_alive():
        tcpserver.start()

    #start two worker threads to avoid the deadlock generated by concurrent collective operations and POSIX operations    
    workerl = []    
    for i in range(2):
        tcpworker = TCPworker('TCPworker-'+str(i))
        while not tcpworker.is_alive():
            tcpworker.start()
        workerl.append(tcpworker)    

    interfaceserver = Interfaceserver('Interfaceserver', 55002)
    while not interfaceserver.is_alive():
        interfaceserver.start()

    #The three threads below are a master-slave scheduler to try out the FIFO scheduling policy  
    if localip == slist[0]:    
        global master
        master = Master("master", 55007)
        while not master.is_alive():
            master.start()

        master_receiver = Master_receiver("master_receiver", 55008)    
        while not master_receiver.is_alive():
            master_receiver.start()

    sleep(5)        
    slave = Slave("slave", 55007, 55008)
    while not slave.is_alive():
        slave.start()
        
    fuse = FUSE(amfora, mountpoint, foreground=True, nothreads=False, big_writes=True, direct_io=True)
    
