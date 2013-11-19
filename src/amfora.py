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
        if apath not in self.data:
            ip = self.meta[apath]['location']
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
            if rpacket.meta[m]['location'] not in gdict:
                gdict[rpacket.meta[m]['location']] = []
            gdict[rpacket.meta[m]['location']].append(m)
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
            if nmeta[m]['location'] not in ndict:
                ndict[nmeta[m]['location']] = []
            ndict[nmeta[m]['location']].append(m)

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
            if meta[k]['location'] not in fdict:
                fdict[meta[k]['location']] = []
            fdict[meta[k]['location']].append([meta[k]['key'], os.path.join(dirname, k[len(apath)+1:])])   
        
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
        if sum(rpacket.meta.values()) != 0:
            logger.log("ERROR", "EXECUTE", "execution failed "+str(rpacket.meta)+"\n"+str(rpacket.data))
            return rpacket
        else:
            logger.log("INFO", "EXECUTE", "execution finished "+str(rpacket.meta))
            return rpacket


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
        #self.cdata[path]=b'' 
        self.cdata[path]=bytearray()
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

        if path in self.data:
            return bytes(self.data[path][offset:offset + size])
        elif path in self.cdata:
            return bytes(self.cdata[path][offset:offset+size])
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
                self.data[path] = rpacket.data[path]
                return bytes(self.data[path][offset:offset + size])

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
            ip = self.cmeta[new]['location']
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
        self.cmeta[new]['location'] = localip
        
        if old in self.meta:
            self.meta.pop(old)

        ip = misc.findserver(old)
        if ip != localip:
            tcpclient = TCPClient()
            packet = Packet(old, "RMMETA", None, None, None, [ip], None)
            ret = tcpclient.sendpacket(packet)
    
        self.release(new, 0)
    
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
        logger.log("INFO", "truncate", path+", "+str(length))

        if path in self.cdata:
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
        global localip
        global misc
        logger.log("INFO", "UNLINK", path)
        #unlink is a two step procedure, first, we need to find the metadata of this file then remove the meta
        #second, clear the actual data 
        tcpclient = TCPClient()
        dst = None
        ip = misc.findserver(path)
        if ip == localip:
            dst = self.meta[path]['location']
            self.meta.pop(path)
        else:
            packet = Packet(path, "UNLINK", {}, {}, 0, [ip], None)
            ret = tcpclient.sendpacket(packet)
            if not ret.meta:
                logger.log("ERROR", "UNLINK", "unlink "+path+" failed")
                raise FuseOSError(ENOENT)
            else:
                dst = ret.meta[path]['location']
        if path in self.meta:
            self.meta.pop(path)

        if not dst:
            logger.log("ERROR", "UNLINK", "unlink "+path+" failed")
            raise FuseOSError(ENOENT)
        else:
            if dst == localip:
                self.data.pop(path)
            else:
                packet = Packet(path, "REMOVE", {}, {}, 0, [dst], None)
                ret = tcpclient.sendpacket(packet)
                if ret.ret != 0:
                    logger.log("ERROR", "UNLINK", "remove "+path+" failed")
            if path in self.data:
                self.data.pop(path)

    def utimens(self, path, times=None):
        global logger
        logger.log("INFO", "utimens", path)
        pass

    def write(self, path, data, offset, fh):
        global logger
        global misc
        logger.log("INFO", "write", path+", length: "+str(len(data))+", offset: "+str(offset))
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

        if path in self.cmeta:
            self.data[path] = self.cdata[path]
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
        #elif path in self.data:
        #     self.data.pop(path)
        else:
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

        if path in self.data:
            tempdict = defaultdict(bytes)
            tempdict[path] = self.data[path]
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
            
    def local_updatelocation(self, path, meta):
        global logger
        logger.log("INFO", "local_updatelocation", path+" location: "+meta['location'])
        
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
                sleep(1)
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
            self.one_sided_sendpacket(op, 55000)
        
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
            logger.log("INFO", "TCPclient_sendallpacket()", "finished executing: "+str(len(packet.misc))+" tasks")
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
            logger.log("ERROR", "TCPworker.sendpacket()", "Socket Exception: "+str(msg))
        except Exception as msg:
            logger.log("ERROR", "TCPworker.sendpacket()", "Other Exception: "+str(msg))
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
                rmeta = dict()
                for i in range(num_files):
                    k = packet.misc.pop()
                    v = packet.meta.pop(k)
                    amfora.meta[k] = v
                    amfora.meta[k]['location'] = localip
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
                tcpclient.one_sided_sendpacket(rpacket, 55001)    
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
            logger.log("INFO", "Interfaceserver_sendpacket()", "ready to send "+str(length)+" bytes")

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
                logger.log("INFO", "Interfaceserver_sendpacket()", "send "+str(sent_iter)+" bytes")
            logger.log("INFO", "Interfaceserver_sendpacket()", "totally send "+str(sent)+" bytes")    
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
            for i in range(math.ceil(len(vlist)/2)):
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
        self.emap = {}
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
            p = subprocess.Popen(task.desc, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            task.endtime = time()
            task.ret = p.returncode
            self.smap[task.desc+" "+str(task.key)] = task.ret
            if task.ret != 0:
                self.emap[task.desc+" "+str(task.key)] = stderr
            #self.fqueue.append(task)
            logger.log('INFO', 'Executor_run', 'finishing task: '+task.desc)
        logger.log('INFO', 'Executor_run', 'all tasks finished')


if __name__ == '__main__':
    if len(argv) != 4:
        print(('usage: %s <mountpoint> <amfs.conf> <localip>' % argv[0]))
        exit(1)
        
    global logger
    logger = Logger("/tmp/amfora-fuse.log")
    global mountpoint
    mountpoint = argv[1]
    global localip
    localip = argv[3]

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
        tcpworker = TCPworker('TCPworker'+str(i))
        while not tcpworker.is_alive():
            tcpworker.start()
        workerl.append(tcpworker)    

    interfaceserver = Interfaceserver('Interfaceserver', 55002)
    while not interfaceserver.is_alive():
        interfaceserver.start()


    fuse = FUSE(amfora, mountpoint, foreground=True, big_writes=True, direct_io=True)

