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
        
        #initializing the root directory
        now = time()
        self.meta['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2, location=[], key=None)

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
        hvalue = misc.hash(apath)
        if hvalue not in self.data:
            ip = self.meta[apath]['location']
            logger.log("INFO", "READ", "read sent to remote server "+apath+" "+ip)
            packet = Packet(apath, "READ", {}, {}, 0, [ip], [0,0])
            
            rpacket = tcpclient.sendpacket(packet)
            if not rpacket.data:
                logger.log("ERROR", "READ", "remote read on "+path+" failed on "+ip)
            else:
                self.data[hvalue] = rpacket.data[hvalue]
        #assembe the multicast packet
        ddict = dict()
        ddict[hvalue] = self.data[hvalue]
        mdict = dict()
        mdict[apath] = self.meta[apath]
        packet=Packet(apath, "MULTICAST", mdict, ddict, 0, slist, None)
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "MULTICAST", "multicasting file: "+apath+" failed")
        return rpacket.ret

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
            return 1
        else:
            logger.log("INFO", "ALLGATHER", "allgathering path: "+apath+" finished")
            return 0
            

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
            if rpacket.meta[m]['location'] not in gdict:
                gdict[rpacket.meta[m]['location']] = []
            gdict[rpacket.meta[m]['location']].append(rpacket.meta[m]['key'])
        self.meta.update(rpacket.meta)    
        packet = Packet(apath, "GATHER", {}, {}, 0, slist, gdict)    
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "GATHER", "gather "+path+" failed")
        else:
            self.data.update(rpacket.data)
            logger.log("INFO", "GATHER", "gather "+path+" finished")
            return 0, rpacket.data, nmeta

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
        ret, data, meta = self.gather(path, algo)
        #keep part of the data local
        num_files = math.ceil(len(meta)/len(slist))
        logger.log("INFO", "SCATTER", "This node keeps "+str(num_files)+" files")    

        for i in range(num_files):
            k, v = meta.popitem()
            data.pop(v['key'])

        packet=Packet(apath, "SCATTER", meta, data, 0, slist, None)
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "SCATTER", "allgathering path: "+apath+" failed")
            return 1
        else:
            logger.log("INFO", "SCATTER", "allgathering path: "+apath+" finished")
            return 0

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
            if nmeta[m]['location'] not in ndict:
                ndict[nmeta[m]['location']] = []
            ndict[nmeta[m]['location']].append(nmeta[m]['key'])
        #assemble the shuffle packet
        logger.log("INFO", "SHUFFLE", str(ndict))    
        packet=Packet(apath, "SHUFFLE", {}, {}, 0, slist, [ndict, dpath])
        rpacket = tcpclient.sendallpacket(packet)
        if rpacket.ret != 0:
            logger.log("ERROR", "SHUFFLE", "shuffling from "+apath+" to "+dpath+" failed")
        return rpacket.ret
        
    def load(self, src, dst):
        pass
    def dump(self, src, dst):
        pass
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
        if sum(rpacket.meta.values()) != 0:
            logger.log("ERROR", "EXECUTE", "execution failed")
            return 1
        else:
            logger.log("INFO", "EXECUTE", "execution finished "+str(rpacket.meta))
            return 0


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
            ip = misc.findserver(path)
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
        logger.log("INFO", "CREATE", path+", "+str(mode))
        self.cmeta[path] =  dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                     st_size=0, st_ctime=time(), st_mtime=time(), 
                                     st_atime=time(), location=localip, key=misc.hash(path))
        hvalue = misc.hash(path)
        self.cdata[hvalue]=b'' 
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        global logger
        global misc
        global localip
        logger.log("INFO", "getattr", path)
        ip = misc.findserver(path)
        logger.log("INFO", "getattr", "metadata of "+path+" is at "+ip)
        if path in self.meta:
            logger.log("INFO", "getattr", "metadata of "+path+" is self.meta ")
            return self.meta[path]
        elif path in self.cmeta:
            logger.log("INFO", "getattr", "metadata of "+path+" is self.cmeta ")
            return self.cmeta[path]

        if ip == localip:
            raise OSError(ENOENT, '')
        else:
            logger.log("INFO", "GETATTR", "getattr sent to remote server: "+path)
            ip = misc.findserver(path)
            tcpclient = TCPClient()
            packet = Packet(path, "GETATTR", None, None, None, [ip], None)
            ret = tcpclient.sendpacket(packet)
            if not ret.meta:
                raise OSError(ENOENT, '')
            else:
                self.meta[path]=ret.meta[path]
                return self.meta[path]

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
                ip = misc.findserver(path)
                packet = Packet(path, "GETATTR", None, None, None, [ip], None)
                tcpclient = TCPClient()
                ret = tcpclient.sendpacket(packet)
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
            ip = misc.findserver(path)
            packet = Packet(path, "GETATTR", None, None, None, [ip], None)
            tcpclient = TCPClient()
            ret = tcpclient.sendpacket(packet)
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
        logger.log("INFO", "open", path+", "+str(flags))
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        global logger
        global misc
        logger.log("INFO", "READ", path+", "+str(size)+", "+str(offset))
        hvalue = misc.hash(path)
        if hvalue in self.cdata:
            return bytes(self.cdata[hvalue][offset:offset + size])
        elif hvalue in self.data:
            return bytes(self.data[hvalue][offset:offset + size])
        else:
            ip = self.meta[path]['location']
            logger.log("INFO", "READ", "read sent to remote server "+path+" "+ip)
            packet = Packet(path, "READ", {}, {}, 0, [ip], [size, offset])
            tcpclient = TCPClient()
            rpacket = tcpclient.sendpacket(packet)
            if not rpacket.data:
                logger.log("ERROR", "READ", "remote read on "+path+" failed on "+ip)
                return None
            else:
                self.data[hvalue] = rpacket.data[hvalue]
                return self.data[hvalue][offset:offset + size]

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
        logger.log("INFO", "rename", "old: "+old+", new: "+new)
        pass

    def rmdir(self, path):
        #rmdir is a two step procedure
        #Step 1, remove the dir path and return the file meta within
        #Step 2, remove all file data on all nodes
        global logger
        logger.log("INFO", "rmdir", path)
        pass

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
        logger.log("INFO", "truncate", path+", "+str(length))
        hvalue = misc.hash(path)
        if hvalue in self.cdata:
            self.cdata[path] = self.cdata[path][:length]
            self.cmeta[path]['st_size'] = length
        else:
            print("truncate sent to remote server")
            #ip = misc.findserver(path)
            #packet = Packet(path, "truncate", None, None, None, ip, length)
            #tcpclient = TCPClient()
            #ret = tcpclient.sendpacket(packet)
            #if ret != 0:
            #    logger.log("ERROR", "truncate", "failed on "+path+" with length: "+str(length))

    def unlink(self, path):
        #Unlink is not well funcitoning for the moment
        global logger
        global misc
        logger.log("INFO", "UNLINK", path)
        #unlink is a two step procedure, first, we need to find the metadata of this file then remove the meta
        #second, clear the actual data 
        tcpclient = TCPClient()
        dst = None
        if path in self.meta:
            dst = self.meta[path]['location']
            self.meta.pop(path)
        else:
            ip = misc.findserver(path)
            packet = Packet(path, "UNLINK", {}, {}, 0, [ip], None)
            ret = tcpclient.sendpacket(packet)
            if not ret.meta:
                logger.log("ERROR", "UNLINK", "unlink "+path+" failed")
                raise FuseOSError(ENOENT)
            else:
                dst = ret.meta[path]['location']
        if not dst:
            logger.log("ERROR", "UNLINK", "unlink "+path+" failed")
            raise FuseOSError(ENOENT)
        else:
            hvalue = misc.hash(path)
            if hvalue in self.data:
                self.data.pop(hvalue)
            else:
                packet = Packet(path, "REMOVE", {}, {}, 0, [dst], None)
                ret = tcpclient.sendpacket(packet)
                if ret.ret != 0:
                    logger.log("ERROR", "UNLINK", "remove "+path+" failed")

    def utimens(self, path, times=None):
        global logger
        logger.log("INFO", "utimens", path)
        pass

    def write(self, path, data, offset, fh):
        global logger
        global misc
        logger.log("INFO", "write", path+", length: "+str(len(data))+", offset: "+str(offset))
        hvalue = misc.hash(path)
        #write to the right place
        if hvalue in self.cdata:
            self.cdata[hvalue] = self.cdata[hvalue][:offset]+data
            self.data[hvalue] = self.data[hvalue][:offset]+data
        else:
            print("write sent to remote server")
            #ip = misc.findserver(path)
            #packet = Packet(path, "locate", None, None, None, ip, None)
            #tcpclient = TCPClient()
            #ret = tcpclient.sendpacket(packet)
            #packet = packet(path, "write", None, None, None, ret, [data, offset])
            #ret = tcpclient.sendpacket(packet)
            
        #update the metadata
        if path in self.cmeta:
            self.cmeta[path]['st_size'] = self.cmeta[path]['st_size']+len(data)
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
        logger.log("INFO", "RELEASE", path)
        ip = misc.findserver(path)
        hvalue = misc.hash(path)
        if path in self.cmeta:
            self.data[hvalue] = self.cdata[hvalue]
            if ip == localip:
                self.meta[path] = self.cmeta[path]
                return 0
            elif path in self.meta:
                self.local_release(path, fh)
                return 0
            else:
                logger.log("INFO", "RELEASE", "release sent to remote server: "+path+" "+ip)
                tempdict = dict()
                tempdict[path] = self.cmeta[path]
                packet = Packet(path, "RELEASE", tempdict, None, None, [ip], None)
                tcpclient = TCPClient()
                rpacket = tcpclient.sendpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "RELEASE", path+" failed")
                return rpacket.ret    
        elif hvalue in self.data:
             self.data.pop(hvalue)
        else:
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
            self.meta[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=nlink+1, st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time(), location=[], key=None)
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
        hvalue = misc.hash(path)
        if hvalue in self.data:
            tempdict = defaultdict(bytes)
            tempdict[hvalue] = self.data[hvalue]
            return tempdict
        else:
            return None

    def local_rename(self, old, new):
        global logger
        logger.log("INFO", "local_rename", "old: "+old+" new: "+new)
        pass

    def local_insert(self, path, meta):
        global logger
        logger.log("INFO", "local_insert", path)
        pass

    def local_rmdir(self, path):
        global logger
        logger.log("INFO", "local_rmdir", path)

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
        logger.log("INFO", "local_delete", path)
        hvalue = misc.hash(path)
        if hvalue not in self.data:
            return 1
        else:
            self.data.pop(hvalue)
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
            
    def local_updatelocation(self, path, meta):
        global logger
        logger.log("INFO", "local_updatelocation", path+" location: "+meta['location'])
        

class TCPClient():
    def __init__(self):
        self.bufsize = 1048576
        self.psize = 16

    def init_port(self, ip, port):
        global logger
        logger.log("INFO", "TCPclient_init_port", "connecting to "+ip+":"+str(port))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connected = 0
        while connected == 0:
            try:
                sock.connect((ip, port))
            except socket.error:
                logger.log("ERROR", "TCPclient_init_port", "connect "+ip+" failed, try again")
                sleep(1)
                continue
            else:
                connected = 1
                logger.log("INFO", "TCPclient_init_port", "connected to "+ip+":"+str(port))
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
            else:
                logger.log("INFO", "TCPclient_init_server", "server TCP socket started")
                return server

    def sendpacket(self, packet):
        global logger
        global localip
        global amfora
        logger.log("INFO", "TCPclient_sendpacket()", packet.path+" "+packet.op)

        #Packet sent to a single host
        if len(packet.tlist) > 0:
            try:
                #initialize the socket
                s = self.init_port(packet.tlist[0], 55000)            

                #dump packet into binary format
                bpacket = pickle.dumps(packet)

                #get the packet size
                length = len(bpacket)
                logger.log("INFO", "TCPclient.sendpacket()", "ready to send "+str(length)+" bytes")

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
                    logger.log("INFO", "TCPclient.sendpacket()", "send "+str(sent_iter)+" bytes")
                logger.log("INFO", "TCPclient.sendpacket()", "totally send "+str(sent)+" bytes")    

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
                    logger.log("INFO", "TCPclient.sendpacket()", "receive "+str(len(temp))+" bytes")
                logger.log("INFO", "TCPclient.sendpacket()", "totally receive "+str(len(data))+" bytes")    
                s.close()
                packet = pickle.loads(data)
            except socket.error as msg:
                logger.log("ERROR", "TCPclient_sendpacket()", "Socket Exception: "+str(msg))
            except Exception as msg:
                logger.log("ERROR", "TCPclient_sendpacket()", "Other Exception: "+str(msg))
            finally:
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
                    logger.log("INFO", "TCPclient.one_sided_sendpacket()", "send "+str(sent_iter)+" bytes")
                logger.log("INFO", "TCPclient.one_sided_sendpacket()", "totally send "+str(sent)+" bytes")    
            except socket.error as msg:
                logger.log("ERROR", "TCPclient.one_sided_sendpacket()", "Socket Exception: "+str(msg))
            except Exception as msg:
                logger.log("ERROR", "TCPclient.one_sided_sendpacket()", "Other Exception: "+str(msg))

        
    def sendallpacket(self, packet):
        global logger
        global misc
        logger.log("INFO", "TCPclient_sendallpacket", packet.op+" "+packet.path+" "+str(packet.misc))
        #partition the target list in the packet to reorganize the nodes into an MST
        #The current implementation of partition_list() is MST
        #SEQ algorithm can be implemented by modifying the code in partition_list()
        olist = misc.partition_list(packet.tlist)
        #start an asynchronous server to receive acknowledgements of collective operations
        logger.log("INFO", "TCPClient_sendallpacket", "num_targets: "+str(len(olist)))
        if len(olist) > 0:
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

        meta = dict(packet.meta)
        data = dict(packet.data)

        for ol in olist:
            if packet.op == "SCATTER":
                num_files = math.ceil(len(ol)*len(meta)/(len(packet.tlist)-1))
                mdict = {}
                ddict = {}
                for i in range(num_files):
                    k, v = meta.popitem()
                    mdict[k] = v
                    ddict[v['key']] = data.pop(v['key'])
                op = Packet(packet.path, packet.op, mdict, ddict, packet.ret, ol, packet.misc)    
            elif packet.op == "EXECUTE":
                taskl = []
                num_tasks = math.ceil(len(ol)*len(packet.misc)/len(packet.tlist))
                for i in range(num_tasks):
                    taskl.append(packet.misc.pop())
                op = Packet(packet.path, packet.op, packet.meta, packet.data, packet.ret, ol, taskl)    
            else:
                op = Packet(packet.path, packet.op, packet.meta, packet.data, packet.ret, ol, packet.misc)
            self.one_sided_sendpacket(op, 55000)
        
        #start shuffleserver as a thread and shuffleclient    
        if packet.op == "SHUFFLE":
            logger.log("INFO", "TCPclient_sendallpacket()", "ready to shuffle")
            global amfora
            global localip
            global slist
            retdict = defaultdict(bytes)
            for ip in slist:
                retdict[ip] = b''
            nextip = misc.nextip()
            logger.log("INFO", "TCPclient_sendallpacket()", "Shuffle: "+str(packet.misc))
            ddict = misc.shuffle(packet.misc[0][localip])
            logger.log("INFO", "TCPclient_sendallpacket()", "Shuffle ddict: "+str(ddict))
            retdict[localip] = ddict.pop(localip)

            server = self.init_server('', 55003)
            shuffleserver = ShuffleServer(server, retdict, packet)
            shuffleserver.start()
            p = Packet(packet.path, "SHUFFLETHREAD", {}, ddict, 0, [nextip], [localip, packet.misc[1]])
            #send a packet to next server
            self.one_sided_sendpacket(p, 55003)
            
            while shuffleserver.is_alive():
                pass
        elif packet.op == "EXECUTE":
            logger.log("INFO", "TCPclient_sendallpacket()", "ready to execute: "+str(len(packet.misc))+" tasks")
            executor = Executor(packet.misc)
            executor.run()
            packet.meta.update(executor.smap)
            logger.log("INFO", "TCPclient_sendallpacket()", "finished executing: "+str(len(packet.misc))+" tasks")
        else:
            pass
        while colthread.is_alive():
            pass
            #sleep(1)
            #logger.log("INFO", "TCPclient_sendallpacket()", "waiting for colthread to finish")
        return packet    
    
class ShuffleServer(threading.Thread):
    def __init__(self, server, retdict, packet):
        threading.Thread.__init__(self)
        self.server = server
        self.retdict = retdict
        self.packet = packet
        self.dst = packet.misc[1]
        self.bufsize = 1048576
        self.psize = 16

    def run(self):
        global logger
        global localip
        global amfora
        global slist
        logger.log("INFO", "ShuffleServer_run()", "thread started")
        counter = 0
        
        tcpclient = TCPClient()
        nextip = misc.nextip()
        while True:
            #return if this server receives slist-1 packets
            if counter == len(slist)-1:
                logger.log("INFO", "ShufflerServer_run()", "received "+str(counter)+" packets, now terminates")
                break
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
                    
                    logger.log("INFO", "ShufflerServer_run()", "remained dict: "+str(tp.data))
                    if len(tp.data.keys()) > 0:
                        packet = Packet(tp.path, tp.op, tp.meta, tp.data, tp.ret, [nextip], tp.misc)
                        tcpclient = TCPClient()
                        tcpclient.one_sided_sendpacket(packet, 55003)

                except socket.error as msg:
                    logger.log("ERROR", "ShuffleServer_run()", "Socket Exception: "+str(msg))
                except Exception as msg:
                    logger.log("ERROR", "ShuffleServer_run()", "Other Exception: "+str(msg))
                finally:
                    counter = counter + 1
        
        #now the shuffle server has all shuffled data 
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
        hvalue = misc.hash(fname)    
        amfora.cdata[hvalue] = bytes(temp)
        amfora.cmeta[fname]['st_size'] = len(amfora.cdata[hvalue])
        amfora.release(fname, 0)
        logger.log("INFO", "ShuffleServer_run()", "shuffle finished")        

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
                length = int(data.decode('utf8').strip('\0'))
                logger.log("INFO", "TCPServer.run()", "ready to receive "+str(length)+" bytes")
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
                    logger.log("INFO", "TCPServer.run()", "receive "+str(len(temp))+" bytes")
                logger.log("INFO", "TCPServer.run()", "totally receive "+str(len(bpacket))+" bytes")    
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
        logger.log("INFO", "TCPWorker.sendpacket()", "sending packet to "+str(packet.tlist))
        try:
            #dump packet into binary format
            bpacket = pickle.dumps(packet)

            #get the packet size
            length = len(bpacket)
            logger.log("INFO", "TCPworker.sendpacket()", "ready to send "+str(length)+" bytes")

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
                logger.log("INFO", "TCPworker.sendpacket()", "send "+str(sent_iter)+" bytes")
            logger.log("INFO", "TCPworker.sendpacket()", "totally send "+str(sent)+" bytes")    
        except socket.error as msg:
            logger.log("ERROR", "TCPworker_sendpacket()", "Socket Exception: "+str(msg))
        except Exception as msg:
            logger.log("ERROR", "TCPworker_sendpacket()", "Other Exception: "+str(msg))
        finally:
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
            elif packet.op == 'INSERTMETA':
                path = el[0]
                msize = int(el[2])
                #print("INSERTMETA: size: "+str(msize))
                conn.send(bytes('0', 'utf8'))
                data = conn.recv(msize)
                meta = pickle.loads(data)
                #print("INSERTMETA: meta: "+str(meta))
                data = ramdisk.local_insert(path, meta)
                conn.send(bytes(str(data), "utf8"))
                conn.close()
            elif packet.op == 'APPENDDATA':
                path = el[0]
                msize = int(el[2])
                offset = int(el[3])
                data = conn.recv(msize)
                content = pickle.loads(data)
                data = ramdisk.local_append(path, offset, content)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
            elif packet.op == 'UPDATE':
                path = el[0]
                msize = int(el[2])
                conn.send(bytes('0', 'utf8'))
                temp = b''
                while len(temp) < msize:
                    data = conn.recv(msize-len(temp))
                    temp = temp + data 
                meta = pickle.loads(temp)                
                ret = ramdisk.local_updatelocation(path, meta)
                conn.send(bytes(str(ret), "utf8"))
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
                for i in range(num_files):
                    k, v = packet.meta.popitem()
                    amfora.meta[k] = v
                    amfora.data[v['key']] = packet.data.pop(v['key'])

                tcpclient = TCPClient()
                rpacket = tcpclient.sendallpacket(packet)
                if rpacket.ret != 0:
                    logger.log("ERROR", "SCATTER", "scattering dir: "+path+" failed")
                rpacket.tlist = [remoteip]    
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
                conn.close()
            elif packet.op == 'SHUFFLE':
                path = packet.path
                dst = packet.misc[1]
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
        self.size = 1024
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

    def run(self):
        global logger
        global amfora
        #global executor
        self.open_socket()
        
        while True:
            conn, addr = self.server.accept()
            try:
                data = conn.recv(self.size)
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
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'GATHER':
                path = el[0]
                algo = el[2]
                ret, ddict, mdict = amfora.gather(path, algo)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'ALLGATHER':
                path = el[0]
                algo = el[2]
                ret = amfora.allgather(path, algo)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'SCATTER':
                path = el[0]
                algo = el[2]
                ret = amfora.scatter(path, algo)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'SHUFFLE':
                path = el[0]
                algo = el[2]
                dst = el[3]
                ret = amfora.shuffle(path, algo, dst)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
            elif el[1] == 'EXECUTE':
                ret = amfora.execute()
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'STATE':
                retdict = executor.state()
                dsize = str(len(pickle.dumps(retdict)))
                while len(dsize) < 10:
                    dsize = dsize + "\0"
                conn.send(bytes(dsize, "utf8"))
                conn.send(pickle.dumps(retdict))
                conn.recv(1)
                conn.close()
            elif el[1] == 'LOAD':
                src = el[0]
                dst = el[2]
                ret = ramdisk.load(src, dst)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'DUMP':
                src = el[0]
                dst = el[2]
                ret = ramdisk.dump(src, dst)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()


class Misc():
    def __init__(self):
        pass

    def findserver(self, fname):
        global slist
        value = zlib.adler32(bytes(fname, 'utf8')) & 0xffffffff
        return slist[value%(len(slist))]

    def hash(self, fname):
        return zlib.adler32(bytes(fname, 'utf8')) & 0xffffffff

    def partition_list(self, slist):
        tlist = []
        global localip
        vlist = list(slist)
        vlist.remove(localip)
        while len(vlist) > 0:
            temp = []
            for i in range(int(len(vlist)/2)+1):
                ip = vlist.pop()
                temp.append(ip)
            tlist.append(temp)    
        return tlist
    
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
                    m = re.match(b"(?P<key>\w+)\s+(?P<value>\w+)", line)
                    k = m.group('key')
                    v = m.group('value')
                    value = zlib.adler32(k) & 0xffffffff
                    ip = slist[value%len(slist)]
                    hdict[ip].extend(line+b'\n')
            else:
                logger.log("ERROR", "MISC_shuffle()", "hash value "+str(h)+" is not in local storage")
        return hdict        

    def readtask(self):
        fd = open("/tmp/task.txt", 'r')
        lines = fd.readlines()
        taskl = []
        for l in lines:
            task = Task(l.strip('\n'))
            taskl.append(task)
        return taskl
    
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
                conn, addr = self.server.accept()
                try:
                    peer = conn.getpeername()[0]
                    data = conn.recv(self.psize)
                    length = int(data.decode('utf8').strip('\0'))
                    logger.log("INFO", "CollThread_run()", "ready to receive "+str(length)+" bytes")
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
                        logger.log("INFO", "CollThread_run()", "receive "+str(len(temp))+" bytes")
                    logger.log("INFO", "CollThread_run()", "totally receive "+str(len(data))+" bytes")    
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
            pass
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
        self.queuetime = time()
        self.starttime = None
        self.endtime = None
        self.desc = desc
        self.ret = None
        tempkey = self.desc+str(self.queuetime)
        self.key = zlib.adler32(bytes(tempkey, 'utf8')) & 0xffffffff

        
class Executor():
    def __init__(self, tlist):
        self.queue = []
        self.fqueue = []
        self.smap = {}
        self.readyqueue = queue.Queue()
        self.fmap = {} #key-file, value-task
        self.tlist = tlist
        for t in tlist:
            self.readyqueue.put(t, True, None)
    
    def run(self):
        global logger
        global mountpoint
        logger.log('INFO', 'Executor_run', 'executor started')
        while True:
            if self.readyqueue.empty():
                break
            task = self.readyqueue.get(True, None)
            logger.log('INFO', 'Executor_run', 'running task: '+task.desc)
            task.starttime = time()
            ret = os.system(task.desc)
            task.endtime = time()
            task.ret = ret
            self.smap[task.desc+" "+str(task.key)] = 0
            #self.fqueue.append(task)
            logger.log('INFO', 'Executor_run', 'finishing task: '+task.desc)
        logger.log('INFO', 'Executor_run', 'all tasks finished')


if __name__ == '__main__':
    if len(argv) != 4:
        print(('usage: %s <mountpoint> <amfs.conf> <localip>' % argv[0]))
        exit(1)
        
    global logger
    logger = Logger("/tmp/amfs-fuse.log")
    global mountpoint
    mountpoint = argv[1]
    global localip
    localip = argv[3]

    global parentip
    parentip = ''

    global misc
    misc = Misc()

    global shuffleself
    shuffleself = 0
    
    global slist
    slist = []
    fd = open(argv[2], 'r')
    while True:
        line = fd.readline()
        if not line:
            break
        ip, port = line.strip('\n').split(':')
        slist.append(ip)
    logger.log("INFO", "main", "Metadata Server List: "+str(slist))
    
    global amfora
    amfora=Amfora()

    global tcpqueue
    tcpqueue = queue.Queue()

    tcpserver = TCPserver('TCPserver', 55000)
    while not tcpserver.is_alive():
        tcpserver.start()

    workerl = []    
    for i in range(2):
        tcpworker = TCPworker('TCPworker'+str(i))
        while not tcpworker.is_alive():
            tcpworker.start()
        workerl.append(tcpworker)    
    #tcpworker1 = TCPworker('TCPworker1')
    #while not tcpworker1.is_alive():
    #    tcpworker1.start()

    interfaceserver = Interfaceserver('Interfaceserver', 55002)
    while not interfaceserver.is_alive():
        interfaceserver.start()
        
    #alltcpserver = ALLTCPserver('ALLTCPserver', 55001, ramdisk)
    #while not alltcpserver.is_alive():
    #    alltcpserver.start()

    fuse = FUSE(amfora, mountpoint, foreground=True, big_writes=True, direct_io=True)

