#!/usr/bin/env python3
##!/home/zhaozhang/workplace/python/bin/python3.3
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
import hashlib
import subprocess
import codecs

###methods that have not been implemented are removexattr, setxattr###
class Logger():
    def __init__(self, logfile):
        self.fd = open(logfile, "w")

    def log(self, info, function, message):
        self.fd.write("%s: %s %s %s\n" % (str(datetime.datetime.now()), info, function, message))
        self.fd.flush()
        
if not hasattr(__builtins__, 'bytes'):
    bytes = str
class RAMdisk(LoggingMixIn, Operations):
    def __init__(self):
        self.files = {}
        self.cache = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2, location=[])
        self.wcache = defaultdict(bytes)
        self.wmeta = {}
        self.waiting = {}
        self.shuffledict = defaultdict(bytes)
        
    #below are collective interface
    def multicast(self, path, algo):
        global logger
        logger.log("INFO", "multicast", path+", "+algo)
        if path not in self.wmeta:
            logger.log("ERROR", "multicast", path+" is not a local file")
            return 1
        else:
            global slist
            mode = self.wmeta[path]['st_mode']
            odict = defaultdict(bytes)
            md5 = hashlib.md5()
            md5.update(path.encode())
            key = md5.hexdigest()
            odict[key] = self.data[key]
            alltcpclient = ALLTCPclient()
            ret = alltcpclient.multicast(path, mode, slist, algo, odict)
            return ret

    def allgather(self, path, algo):
        global logger
        logger.log("INFO", "allgather", path+", "+algo)
        if not S_ISDIR(self.files[path]['st_mode']):
            logger.log("ERROR", "allgather", path+" is not a direcotry")
            return 1
        else:
            global slist
            odict = defaultdict(bytes)
            for x in self.wmeta:
                if x != '/' and path == os.path.dirname(x):
                    md5 = hashlib.md5()
                    md5.update(path.encode())
                    key = md5.hexdigest()
                    odict[key]=self.data[key]
            ret, retdict = self.gather(path, algo)
            odict.update(retdict)
            logger.log("INFO", "allgather", "allgather has "+str(len(odict.keys()))+" files")
            alltcpclient = ALLTCPclient()
            ret = alltcpclient.multicast(path, 33188, slist, algo, odict)
            logger.log("INFO", "allgather", "allgather on "+path+" finished")
            return ret
        
    def allgather_old(self, path, algo):
        global logger
        logger.log("INFO", "allgather", path+", "+algo)
        if not S_ISDIR(self.files[path]['st_mode']):
            logger.log("ERROR", "allgather", path+" is not a direcotry")
            return 1
        else:
            global slist
            alltcpclient = ALLTCPclient()
            metadict=alltcpclient.sendall(path, "READDIR", slist)
            for x in self.files:
                if x != '/' and path == os.path.dirname(x):
                    metadict[x]=self.files[x]
                    
            allgatherthread = Allgatherthread(path, metadict, algo)
            while not allgatherthread.is_alive():
                allgatherthread.start()
                
            alltcpclient = ALLTCPclient()
            ret = alltcpclient.allgather(path, slist, algo, metadict)

            while allgatherthread.is_alive():
                sleep(0.1)
                logger.log("INFO", "allgather", "waiting for allgatherthread to finish")
            logger.log("INFO", "allgather", "allgather finished")
            return 0

    def gather(self, path, algo):
        global logger
        logger.log("INFO", "gather", path+", "+algo)
        if not S_ISDIR(self.files[path]['st_mode']):
            logger.log("ERROR", "gather", path+" is not a direcotry")
            return 1, None
        else:
            global slist
            #self.readdir(path, 0)
            #iplist = self.files[path]['location']
            alltcpclient = ALLTCPclient()
            #retmdict, retdict = alltcpclient.gather(path, iplist, algo)
            #retmdict, retdict = alltcpclient.gather(path, slist, algo)
            retdict = alltcpclient.gather(path, slist, algo)
            #self.files.update(retmdict)
            logger.log("INFO", "gather", "updating self.data")
            self.data.update(retdict)
            for k in retdict.keys():
                print(len(retdict[k]))
            return 0, retdict
        
    def scatter(self, path, algo):
        global logger
        logger.log("INFO", "scatter", path+", "+algo)
        if not S_ISDIR(self.files[path]['st_mode']):
            logger.log("ERROR", "scatter", path+" is not a direcotry")
            return 1
        else:
            global slist
            mdict = dict()
            ddict = defaultdict(bytes)
            for x in ramdisk.wmeta:
                if path == os.path.dirname(x):
                    mdict[x] = ramdisk.wmeta[x]
                    md5 = hashlib.md5()
                    md5.update(x.encode())
                    key = md5.hexdigest()
                    ddict[key] = ramdisk.wcache[key]
            #logger.log("INFO", "scatter", str(mdict))
            #logger.log("INFO", "scatter", str(ddict))
            alltcpclient = ALLTCPclient()
            ret = alltcpclient.scatter(path, slist, algo, mdict, ddict)
            return ret

    def shuffle(self, path, algo, dst):
        global logger
        #global shufflemap
        global shuffleserver
        logger.log("INFO", "shuffle", "shuffle "+path+" to "+dst)
        if not S_ISDIR(self.files[path]['st_mode']):
            logger.log("ERROR", "shuffle", path+" is not a direcotry")
            return 1
        else:
            global slist
            alltcpclient = ALLTCPclient()
            metadict=alltcpclient.sendall(path, "READDIR", slist)
            for x in self.files:
                if path == os.path.dirname(x):
                    metadict[x] = self.files[x]

            shuffleserver = Shuffleserver('shuffleserver', 55004)
            while not shuffleserver.is_alive():
                shuffleserver.start()
                

            alltcpclient = ALLTCPclient()
            data = alltcpclient.shuffle(path, dst, slist, metadict, algo)

            shuffler = Shuffler('shuffler', 'localhost', path, dst, metadict, algo)
            #shuffler.setDaemon(True)
            while not shuffler.is_alive():
                shuffler.start()

            alltcpclient = ALLTCPclient()
            data = alltcpclient.shuffle_launch(path, dst, slist, metadict, algo)
            
            #wait for shuffle to finish
            while shuffleserver.is_alive() or shuffler.is_alive():
                sleep(1)

            logger.log('INFO', 'shuffle', 'shuffle finsihed')
            return data

    def load(self, src, dst):
        global logger
        logger.log("INFO", "load", "loading files from "+src+" to "+dst)
        if not os.path.isdir(src):
            logger.log("INFO", "load", src+" is not a valid directory")
            return 1
        elif dst not in self.files:
            logger.log("INFO", "load", dst+" is not a valid directory")
            return 1
        else:
            global slist
            global localip 
            basename = os.path.basename(src)
            dirname = os.path.join(dst, basename)
            logger.log("INFO", "RAMDISK_LOAD", "creating dir: "+dirname)
            if dirname not in self.wmeta:
                self.mkdir(dirname, self.files['/']['st_mode'])

            flist = os.listdir(src)
            flist.sort()
            logger.log("INFO", "load", "loading "+str(len(flist))+" files from "+src+" to "+dst)
            #flist.reverse()
            
            targetlist = list(slist)
            targetlist.remove(localip)
            portion = 1.0/(len(slist))
            f_fcount = portion*len(flist)
            fcount = int(portion*len(flist))
            if float(fcount) < f_fcount:
                fcount = fcount+1
            pl = []
            for i in range(fcount):
                f = flist.pop(0)
                pl.append(f)
            logger.log("INFO", "RAMDISK_LOAD", "locally process files: "+str(pl))

            loader = Loader('loader', src, dst, pl)
            while not loader.is_alive():
                loader.start()
                    
            alltcpclient = ALLTCPclient()
            algo='mst'
            ret = alltcpclient.load(src, targetlist, flist, dst, algo)
            return ret

    def dump(self, src, dst):
        global logger
        logger.log("INFO", "dump", "dumping files from "+src+" to "+dst)
        if not os.path.isdir(dst):
            logger.log("INFO", "dump", dst+" is not a valid directory")
            return 1
        elif src not in self.files:
            logger.log("INFO", "dump", src+" is not a valid directory")
            return 1
        else:
            global slist
            global localip 
            basename = os.path.basename(src)
            dirname = os.path.join(dst, basename)
            logger.log("INFO", "RAMDISK_DUMP", "creating dir: "+dirname)
            if not os.path.isdir(dirname):
                os.makedirs(dirname)

            fdict = {}
            flist = self.readdir(src, 0)
            flist.remove('.')
            flist.remove('..')
            #might revmove directories once implmented
            
            for f in flist:
                fn = os.path.join(src, f)
                if fn in self.cache:
                    remoteip = self.cache[fn]['location']
                else:
                    remoteip = self.files[fn]['location']
                if remoteip not in fdict:
                    fdict[remoteip] = []
                fdict[remoteip].append(fn)

            targetlist = list(slist)
            targetlist.remove(localip)
            pl = fdict.pop(localip)
            logger.log("INFO", "RAMDISK_LOAD", "locally process files: "+str(pl))

            dumper = Dumper('dumper', src, dst, pl)
            while not dumper.is_alive():
                dumper.start()
                    
            alltcpclient = ALLTCPclient()
            algo='mst'
            ret = alltcpclient.dump(src, targetlist, fdict, dst, algo)

            while dumper.is_alive():
                logger.log("INFO", "ALLtcpserver_DUMP", "waiting for local processing")
                sleep(0.1)
            return ret

    def execute(self):
        global logger
        global slist
        #read task from file
        tlist = []
        fd = open('/tmp/task.txt', 'r')
        while True:
            line = fd.readline()
            if not line:
                break
            task = Task(line.strip('\n'))
            ret = tlist.append(task)
        fd.close()
        #self.queue.reverse()
        logger.log("INFO", "execute", "loaded "+str(len(tlist))+" tasks")

        execlist = []
        portion = 1.0/(len(slist))
        f_num = portion*len(tlist)
        t_num = int(portion*len(tlist))
        if float(t_num) < f_num:
            t_num = t_num+1

        for i in range(t_num):
            if len(tlist) > 0:
                t = tlist.pop(0)
                execlist.append(t)
        
        executor=Executor(execlist)
        while not executor.is_alive():
            executor.start()
            
        alltcpclient = ALLTCPclient()
        ret = alltcpclient.dispatch(slist, tlist)
        while executor.is_alive():
            sleep(0.1)
            logger.log("INFO", "execute", "waiting for local task execution")
        logger.log("INFO", "execute", "all local tasks finished")
        #self.readyqueue = queue.Queue(self.queue)
        return 0
    
    #below are POSIX interface
    def chmod(self, path, mode):
        global logger
        logger.log("INFO", "chmod", path+", "+str(mode))
        tcpclient = TCPclient()
        ret=tcpclient.sendmsg(path, "CHMOD#"+str(mode))
        return 0

    def chown(self, path, uid, gid):
        global logger
        logger.log("INFO", "chown", path+", "+str(uid)+", "+str(gid))
        tcpclient = TCPclient()
        ret=tcpclient.sendmsg(path, "CHOWN#"+str(uid)+"#"+str(gid))

    def create(self, path, mode):
        global logger
        global localip
        logger.log("INFO", "create", path+", "+str(mode))
        self.wmeta[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), location=localip)
        md5=hashlib.md5()
        md5.update(path.encode())
        key = md5.hexdigest()
        self.wcache[key] = b''
        #self.wcache[key] = bytearray(b'')
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        global logger
        logger.log("INFO", "getattr", path)
        if path in self.wmeta:
            logger.log("INFO", "getattr", " wmeta cache hit")
            return self.wmeta[path]
        elif path in self.cache:
            logger.log("INFO", "getattr", " cache hit")
            return self.cache.pop(path)
        elif path in self.files:
            logger.log("INFO", "getattr", " files hit")
            return self.files[path]
        else:
            logger.log("INFO", "getattr", " cache miss")
            tcpclient = TCPclient()
            data=tcpclient.sendmsg(path, "GETATTR")
            ret = pickle.loads(data)
            if not ret:
                raise FuseOSError(ENOENT)
            else:
                return ret

    def getxattr(self, path, name, position=0):
        global logger
        logger.log("INFO", "getxattr", path+", "+name)
        #retrieve metadata from remote server 
        tcpclient = TCPclient()
        data=tcpclient.sendmsg(path, "GETXATTR")
        attrs = pickle.loads(data)
        
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        global logger
        logger.log("INFO", "listxattr", path)
        #retrieve metadata from remote server
        tcpclient = TCPclient()
        data=tcpclient.sendmsg(path, "GETXATTR")
        attrs = pickle.loads(data)
        return list(attrs.keys())

    def mkdir(self, path, mode):
        global logger
        logger.log("INFO", "mkdir", path+", "+str(mode))
        global slist
        alltcpclient = ALLTCPclient()
        data = alltcpclient.sendall(path, "MKDIR#"+str(mode), slist)
        global ramdisk
        ramdisk.local_mkdir(path, mode)

    def open(self, path, flags):
        global logger
        logger.log("INFO", "open", path+", "+str(flags))
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        global logger
        logger.log("INFO", "read", path+", "+str(size)+", "+str(offset))
        md5=hashlib.md5()
        md5.update(path.encode())
        key = md5.hexdigest()
        #retrieve data from remote server
        if key in self.wcache:
            return self.wcache[key][offset:offset + size]
        if key not in self.data:
            tcpclient = TCPclient()
            data = tcpclient.sendmsg(path, "READ")
            meta = pickle.loads(data)
            location = meta['location']
            dsize = meta['st_size']
            ret = tcpclient.retrievefile(key, "COPY", location, dsize)
            self.data[key] = ret
        return self.data[key][offset:offset + size]

    def readdir(self, path, fh):
        global logger
        logger.log("INFO", "readdir", path)
        global slist
        ilist = ['.', '..']
        alltcpclient = ALLTCPclient()
        metadict=alltcpclient.sendall(path, "READDIR", slist)
        #print("readdir metadict:"+str(metadict))
        self.cache.update(metadict)

        for x in metadict.keys():
            iname = os.path.basename(x)
            ilist.append(iname)
        for x in self.files:
            if x != '/' and path == os.path.dirname(x):
                iname=x[len(path):]
                if iname[0] == '/':
                    iname = iname[1:]
                if iname not in ilist:
                    ilist.append(iname)
                self.cache[x] = self.files[x]
        #update location list
        for x in self.cache.keys():
            if self.cache[x]['location'] not in self.files[path]['location']:
                self.files[path]['location'].append(self.cache[x]['location'])
        #print("readdir: ilist:"+str(ilist))
        return ilist
        
    def readlink(self, path):
        global logger
        logger.log("INFO", "readlink", path)
        tcpclient = TCPclient()
        data = tcpclient.sendmsg(path, "READLINK#")
        ret = data.decode("utf8")
        return ret

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
 
        tcpclient = TCPclient()
        if old not in self.files:
            data = tcpclient.sendmsg(old, "READ")
            meta = pickle.loads(data)
        else:
            meta = self.files[old]
        if S_ISDIR(meta['st_mode']):
            print("ERROR: renaming directory is not supported for the moment")
        else:
            location = meta['location']
            ret = tcpclient.renamefile(old, new, location)
            ret = tcpclient.insertmeta(new, meta)
            ret = tcpclient.sendmsg(old, "UNLINK")

    def rmdir(self, path):
        global logger
        logger.log("INFO", "rmdir", path)
        global slist
        alltcpclient = ALLTCPclient()
        data = alltcpclient.sendall(path, "RMDIR", slist)
        global ramdisk
        ramdisk.local_rmdir(path)

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
        tcpclient = TCPclient()
        data=tcpclient.sendmsg(target, "SYMLINK#"+source)

    def truncate(self, path, length, fh=None):
        global logger
        logger.log("INFO", "truncate", path+", "+str(length))
        md5=hashlib.md5()
        md5.update(path.encode())
        key = md5.hexdigest()
        self.data[key] = self.data[key][:length]
        if path in self.files:
            self.files[path]['st_size'] = length
        else:
            tcpclient = TCPclient()
            data=tcpclient.sendmsg(path, "TRUNCATE#"+str(length))

    def unlink(self, path):
        global logger
        logger.log("INFO", "unlink", path)
        tcpclient = TCPclient()
        data=tcpclient.sendmsg(path, "UNLINK")
        self.files[path] = pickle.loads(data)
        location = self.files[path]['location']
        tcpclient.deletefile(path, "DELETE", location)
        if path in self.files:
            self.files.pop(path)
        
    def utimens(self, path, times=None):
        global logger
        logger.log("INFO", "utimens", path)
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        global logger
        logger.log("INFO", "write", path+", length: "+str(len(data))+", offset: "+str(offset))
        md5=hashlib.md5()
        md5.update(path.encode())
        key = md5.hexdigest()
        if path in self.wmeta:
            self.wcache[key] = self.wcache[key][:offset] + data
            #self.wcache[key] = self.wcache[key].extend(data)
            self.wmeta[path]['st_size'] = self.wmeta[path]['st_size'] + len(data)
        elif offset > 0:
            logger.log("INFO", "write_append", path+", length: "+str(len(data))+", offset: "+str(offset))
            if key in self.data:
                self.data.pop(key)
            #append data to where the data is
            tcpclient = TCPclient()
            ret=tcpclient.appenddata(path, offset, data)
            return len(data)

        #self.data[key] = self.data[key][:offset] + data
        return len(data)

    def release(self, path, fh):
        global logger
        #global executor
        logger.log("INFO", "release", path)
        md5=hashlib.md5()
        md5.update(path.encode())
        key = md5.hexdigest()
        #update metadata on a remote server
        tcpclient = TCPclient()
        if key in self.wcache:
            self.data[key] = self.wcache[key]
            ret = tcpclient.insertmeta(path, self.wmeta[path])
        elif key not in self.wcache:
            if key not in self.data:
                ret=tcpclient.remoterelease(path)
        #elif key in executor.waiting:
        #    return 0
        return 0

    def local_chmod(self, path, mode):
        global logger
        logger.log("INFO", "local_chmod", path+", "+str(mode))
        #redirect metadata operation to remote server
        self.files[path]['st_mode'] &= 0o770000
        self.files[path]['st_mode'] |= mode
        return 0

    def local_chown(self, path, uid, gid):
        global logger
        logger.log("INFO", "local_chown", path+", "+str(uid)+", "+str(gid))
        #redirect metadata operation to remote server
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid


    def local_create(self, path, mode, ip):
        global logger
        logger.log("INFO", "local_create", path+", "+str(mode)+", "+str(ip))
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), location=ip)

        self.fd += 1
        return self.fd

    def local_getxattr(self, path, name, position=0):
        global logger
        logger.log("INFO", "local_getxattr", path+", "+str(name))
        #retrieve metadata from remote server 
        attrs = self.files[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            logger.log("ERROR", "local_getxattr", name+" is not an attribute of "+path)
            return ''       # Should return ENOATTR

    def local_listxattr(self, path):
        global logger
        logger.log("INFO", "local_listxattr", path)
        #retrieve metadata from remote server
        attrs = self.files[path].get('attrs', {})
        return list(attrs.keys())

        
    def local_mkdir(self, path, mode):
        global logger
        logger.log("INFO", "local_mkdir", path+", "+str(mode))
        parent = os.path.dirname(path)
        #update metadata on remote server
        if parent not in self.files:
            logger.log("ERROR", "local_mkdir", parent+" does not exist")
            raise FuseOSError(ENOENT)
        else:
            nlink=self.files[parent]['st_nlink']
            self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=nlink+1, st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time(), location=[])
            self.files[parent]['st_nlink'] += 1

    def local_readdir(self, path, fh):
        global logger
        logger.log("INFO", "local_readdir", path)
        metadict = {}
            
        ilist=[]
        for x in self.files:
            if x != '/' and path == os.path.dirname(x) and not S_ISDIR(self.files[x]['st_mode']):
                iname=x[len(path):]
                if iname[0] == '/':
                    iname = iname[1:]
                ilist.append(iname)
                metadict[x] = self.files[x]
        return metadict

    def local_readlink(self, path):
        global logger
        logger.log("INFO", "local_readlink", path)
        md5=hashlib.md5()
        md5.update(path.encode())
        key = md5.hexdigest()
        return self.data[key]

    def local_removexattr(self, path, name):
        global logger
        logger.log("INFO", "local_removeattr", path+", "+name)
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def local_rename(self, old, new):
        global logger
        logger.log("INFO", "local_rename", "old: "+old+" new: "+new)
        md5=hashlib.md5()
        md5.update(old.encode())
        oldkey = md5.hexdigest()
        md5=hashlib.md5()
        md5.update(new.encode())
        newkey = md5.hexdigest()
        self.data[newkey] = self.data.pop(oldkey)
        return 0

    def local_insert(self, path, meta):
        global logger
        logger.log("INFO", "local_insert", path)
        self.files[path] = meta
        #broad file location to waiters
        if path in self.waiting:
            tcpclient = TCPclient()
            for i in self.waiting[path]:
                if i != ip:
                    tcpclient.updatelocation(path, i, meta)
            self.waiting.pop(path)
        return 0

    def local_rmdir(self, path):
        global logger
        logger.log("INFO", "local_rmdir", path)
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def local_setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def local_symlink(self, target, source, ip):
        global logger
        logger.log("INFO", "local_symlink", "target: "+target+" source: "+source)
        #update metadata on a remote server
        self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source), location=ip)

        md5=hashlib.md5()
        md5.update(target.encode())
        key = md5.hexdigest()
        self.data[key] = source

    def local_truncate(self, path, length, fh=None):
        global logger
        logger.log("INFO", "local_truncate", path+", "+str(length))
        self.files[path]['st_size'] = length

    def local_unlink(self, path):
        global logger
        logger.log("INFO", "local_unlink", path)
        self.files.pop(path)

    def local_delete(self, path):
        global logger
        logger.log("INFO", "local_delete", path)
        md5=hashlib.md5()
        md5.update(path.encode())
        key = md5.hexdigest()
        self.data.pop(key)
        
    def local_utimens(self, path, times=None):
        global logger
        logger.log("INFO", "local_utimens", path)
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def local_append(self, path, offset, data):
        global logger
        logger.log("INFO", "local_append", path+", "+str(offset)+", "+str(len(data)))
        md5=hashlib.md5()
        md5.update(path.encode())
        key = md5.hexdigest()
        self.wcache[key] = self.wcache[key][:offset] + data
        self.wmeta[path]['st_size'] = self.wmeta[path]['st_size'] + len(data)
        return len(data)

    def local_getattr(self, path, remoteip):
        global logger
        logger.log("INFO", "local_getattr", path)
        ret = None
        if path in self.files:
            ret = ramdisk.files[path]
        #else:
        #    if path not in ramdisk.waiting:
        #        logger.log("INFO", "local_getattr", path+" does not exist, append "+remoteip+" to self.waiting")
        #        ramdisk.waiting[path] = []
        #        ramdisk.waiting[path].append(remoteip)
        return ret

    def local_release(self, path):
        global logger
        logger.log("INFO", "local_release", path)
        md5=hashlib.md5()
        md5.update(path.encode())
        key = md5.hexdigest()
        self.data[key] = self.wcache[key]
        tcpclient = TCPclient()
        tcpclient.insertmeta(path, self.wmeta[path])

    def local_updatelocation(self, path, meta):
        global logger
        #global executor
        logger.log("INFO", "local_updatelocation", path+" location: "+meta['location'])
        self.files[path] = meta
        #if path in executor.fmap:
        #    task = executor.fmap.pop(path)
        #    executor.readyqueue.put(task, True, None)
        return 0

    def local_shuffle_transfer(self, path, data, remoteip):
        global logger
        global slist
        logger.log("INFO", "local_shuffle_transfer", "receved "+str(len(data))+" bytes from address: "+remoteip+" dir: "+path)
        self.shuffledict[remoteip] = data

        if len(self.shuffledict.keys()) == len(slist):
            logger.log("INFO", "local_shuffle_transfer", "all data received, now put data in a file")
            index = slist.index(localip)
            fname = path+'/'+str(index)+'.txt'
            md5=hashlib.md5()
            md5.update(fname.encode())
            key = md5.hexdigest()
            
            #self.wcache[key] = b''
            #self.create(fname, 33188)
            #for k in self.shuffledict:
            #    logger.log("INFO", "local_shuffle_transfer", "Processing record from "+k)
            #    btext = b''
            #    for line in self.shuffledict[k]:
            #            text=line+'\n'
            #            btext = btext+text.encode('utf8')
            #    self.wcache[key] = self.wcache[key]+btext
            #    logger.log("INFO", "local_shuffle_transfer", "Processed record from "+k)

            temp = bytearray()
            self.create(fname, 33188)
            for k in self.shuffledict:
                logger.log("INFO", "local_shuffle_transfer", "Processing record from "+k)
                #btext = bytearray()
                #for line in self.shuffledict[k]:
                #        text=line+'\n'
                #        btext.extend(text.encode('utf8'))
                #temp.extend(btext)
                temp.extend(self.shuffledict[k])
                logger.log("INFO", "local_shuffle_transfer", "Processed record from "+k)
            self.wcache[key]=bytes(temp)
            self.wmeta[fname]['st_size'] = len(self.wcache[key])
            self.local_release(fname)
            self.shuffledict.clear()
            logger.log("INFO", "local_shuffle_transfer", "Local shuffle transfer finished")
            return 1
        else:
            return 0
            
class TCPclient():
    def init(self, ip):
        global logger
        logger.log("INFO", "TCPclient_init", "connet to "+ip)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        port = 55000
        connected = 0
        while connected == 0:
            try:
                sock.connect((ip, port))
            except socket.error:
                logger.log("ERROR", "TCPclient_init", "connect "+ip+" failed, try again")
                sleep(1)
                continue
            else:
                logger.log("INFO", "TCPclient_init", "connet to "+ip+" succeeded")
                connected = 1
        return sock

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

    def sendmsg(self, filename, msg):
        global logger
        global localip
        global ramdisk
        logger.log("INFO", "TCPclient_sendmsg", filename+", "+msg)
        size=1024
        global slist
        md5 = hashlib.md5()
        md5.update(filename.encode())
        key = md5.hexdigest()
        value = int(key, 16)
        ip = slist[value%len(slist)]
        try:
            #if msg == 'GETATTR' and ip == localip:
            #    ret = ramdisk.local_getattr(filename, localip)
            #    return pickle.dumps(ret)
            s = self.init(ip)            
            s.send(bytes(filename+'#'+msg, "utf8"))
            ret = s.recv(size)
            s.close()
        except socket.error as msg:
            logger.log("ERROR", "TCPclient_sendmsg", "Socket Exception: "+str(msg))
        except Exception as msg:
            logger.log("ERROR", "TCPclient_sendmsg", "Otehr Exception: "+str(msg))
        finally:
            return ret
        
    def sendall(self, path, msg):
        global logger
        logger.log("INFO", "TCPclient_sendall", "broadcast "+path+"#"+msg+" to all servers")
        size = 1024
        retlist = []
        for ip in slist:
            try:
                s = self.init(ip)            
                s.send(bytes(path+'#'+msg, "utf8"))
                ret = s.recv(size)
                s.close()
            except socket.error as msg:
                logger.log("ERROR", "TCPclient_sendall", "Socket Exception: "+str(msg))
            finally:
                if msg == 'READDIR':
                    retlist.extend(pickle.loads(ret))
                elif msg == 'MKDIR':
                    pass
                else:
                    pass
        if msg == 'READDIR':
            return retlist
        elif msg == 'MKDIR':
            return 0
        else:
            return 0

    def retrievefile(self, filename, msg, ip, size):
        global logger
        logger.log("INFO", "TCPclient_retrievefile", "retrieve "+filename+" from "+ip+" size: "+str(size))
        try:
            temp=b''
            ret = b''
            s = self.init(ip)            
            s.send(bytes(filename+'#'+msg, "utf8"))
            data = s.recv(10)
            dsize = data.decode('utf8').strip('\0')
            s.send(bytes('0', 'utf8'))
            tcp_big = TCP_big()
            temp = tcp_big.recv(s, int(dsize))
            #while len(temp) < size:
            #    data = s.recv(dsize)
            #    temp=temp+data
            #    logger.log("INFO", "TCPclient_retrievefile", "received "+str(len(temp))+" bytes")
            
            s.send(bytes('0', 'utf8'))
            ret=pickle.loads(temp)    
        except socket.error as msgg:
            logger.log("ERROR", "TCPclient_retrievefile", "Socket Exception: "+str(msgg))
        except Exception as msgg:
            logger.log("ERROR", "TCPclient_retrievefile", "Other Exception: "+str(msgg))
        finally:
            #s.close()
            return ret

    def deletefile(self, filename, msg, ip):
        global logger
        global localip
        global ramdisk
        logger.log("INFO", "TCPclient_deletefile", "delete "+filename+" at "+ip)
        if ip == localip:
            ramdisk.local_delete(filename)
            return 0
        try:
            dsize=1024
            s = self.init(ip)            
            s.send(bytes(filename+'#DELETE', "utf8"))
            ret=s.recv(dsize)
            s.close()
        except socket.error as msgg:
            logger.log("ERROR", "TCPclient_deletefile", "Socket Exception: "+str(msgg))
        except Exception as msgg:
            logger.log("ERROR", "TCPclient_deletefile", "Other Exception: "+str(msgg))
        finally:
            return ret

    def renamefile(self, old, new, ip):
        global logger
        global localip
        global ramdisk
        logger.log("INFO", "TCPclient_renamefile", "rename "+old+" with "+new)
        if ip == localip:
            ret = ramdisk.local_rename(old, new)
            return ret
        try:
            dsize=1024
            s = self.init(ip)            
            s.send(bytes(old+'#RENAME#'+new, "utf8"))
            ret=s.recv(dsize)
            s.close()
        except socket.error as msgg:
            logger.log("ERROR", "TCPclient_renamefile", "Socket Exception: "+str(msgg))
        except Exception as msgg:
            logger.log("ERROR", "TCPclient_renamefile", "Other Exception: "+str(msgg))
        finally:
            return ret

    def updatelocation(self, path, ip, meta):
        global logger
        logger.log("INFO", "TCPclient_updatelocation", "update file: "+path+"'s location:"+meta['location']+" to "+ip)
        try:
            dsize=1024
            s = self.init(ip)            
            s.send(bytes(path+'#UPDATE#'+str(len(pickle.dumps(meta))), "utf8"))
            s.recv(1)
            s.send(pickle.dumps(meta))
            ret=s.recv(dsize)
            s.close()
        except socket.error as msgg:
            logger.log("ERROR", "TCPclient_updatelocation", "Socket Exception: "+str(msgg))
        except Exception as msgg:
            logger.log("ERROR", "TCPclient_updatelocation", "Other Exception: "+str(msgg))
        finally:
            return ret
        
    def insertmeta(self, path, meta):
        global logger
        global localip
        global ramdisk
        logger.log("INFO", "TCPclient_insertmeta", "insertmetadata of "+path)
        try:
            global slist
            md5 = hashlib.md5()
            md5.update(path.encode())
            key = md5.hexdigest()
            value = int(key, 16)
            ip = slist[value%len(slist)]
            if ip == localip:
                ret = ramdisk.local_insert(path, meta)
                logger.log("INFO", "TCPclient_insertmeta", "insertmetadata of "+path+" suceeded locally")
                return ret
            dsize=1024
            s = self.init(ip)            
            s.send(bytes(path+'#INSERTMETA#'+str(len(pickle.dumps(meta))), "utf8"))
            #print("insertmeta: msg sent")
            ret = s.recv(1)
            s.send(pickle.dumps(meta))
            ret=s.recv(dsize)
            #print("insetmeta: meta sent")
            s.close()
        except socket.error as msgg:
            logger.log("ERROR", "TCPclient_insertmeta", "Socket Exception: "+str(msgg))
        except Exception as msgg:
            logger.log("ERROR", "TCPclient_insertmeta", "Other Exception: "+str(msgg))
        finally:
            logger.log("INFO", "TCPclient_insertmeta", "insertmetadata of "+path+" suceeded")
            return ret

    def appenddata(self, path, offset, data):
        global logger
        logger.log("INFO", "TCPclient_appenddata", "append "+str(len(data))+" bytes to "+path+" at offset: "+str(offset))
        try:
            dsize=1024
            meta=self.sendmsg(path, "GETATTR")
            ret = pickle.loads(meta)
            ip = ret['location']

            s = self.init(ip)
            s.send(bytes(path+'#APPENDDATA#'+str(len(pickle.dumps(data)))+"#"+str(offset), "utf8"))
            s.send(pickle.dumps(data))
            ret=s.recv(dsize)
            s.close()
        except socket.error as msgg:
            logger.log("ERROR", "TCPclient_insertmeta", "Socket Exception: "+str(msgg))
        except Exception as msgg:
            logger.log("ERROR", "TCPclient_insertmeta", "Other Exception: "+str(msgg))
        finally:
            return ret

    def remoterelease(self, path):
        global logger
        logger.log("INFO", "TCPclient_remoterelease", path)
        try:
            dsize=1024
            meta=self.sendmsg(path, "GETATTR")
            ret = pickle.loads(meta)
            if not ret:
                return None
            ip = ret['location']

            s = self.init(ip)
            s.send(bytes(path+'#RELEASE', "utf8"))
            ret=s.recv(dsize)
            s.close()
        except socket.error as msgg:
            logger.log("ERROR", "TCPclient_remoterelease", "Socket Exception: "+str(msgg))
        except Exception as msgg:
            logger.log("ERROR", "TCPclient_remoterelease", "Other Exception: "+str(msgg))
        finally:
            return ret

    def shuffle_transfer(self, path, data, target):
        global logger
        logger.log("INFO", "TCPclient_shuffle_transfer", "transfer "+str(len(data))+" bytes of "+path+" to "+target)
        try:
            dsize=1024
            odata = pickle.dumps(data)
            s = self.init_port(target, 55004)
            s.send(bytes(path+'#SHUFFLE_TRANSFER#'+str(len(odata)), "utf8"))
            ret=s.recv(1)

            length = len(odata)
            tcp_big = TCP_big()
            tcp_big.send(s, odata, length)
            #sent = 0
            #while sent < length:
            #    sent_iter = s.send(odata[sent:])
            #    sent = sent + sent_iter
            #    logger.log("INFO", "TCPclient_shuffle_transfer", "sent "+str(sent)+" bytes")
 
            ret=s.recv(1)
            s.close()
        except socket.error as msgg:
            logger.log("ERROR", "TCPclient_shuffletransfer", "Socket Exception: "+str(msgg))
        except Exception as msgg:
            logger.log("ERROR", "TCPclient_shuffletransfer", "Other Exception: "+str(msgg))
        finally:
            return ret

        
class ALLTCPserver(threading.Thread):
    def __init__(self, workerid, port, ramdisk):
        threading.Thread.__init__(self)
        self.id = workerid
        self.host = ''
        self.port = port
        self.size = 1024
        self.server = None
        self.ramdisk = ramdisk

    def init(self, ip, oport):
        global logger
        logger.log("INFO", "ALLTCPserver_init", "initializing connection to "+ip+": "+str(oport))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connected = 0
        while connected == 0:
            try:
                sock.connect((ip, oport))
            except socket.error:
                logger.log("ERROR", "ALLTCPserver_init", "connect to " +ip+":"+str(oport)+" failed, try again")
                sleep(0.1)
                continue
            finally:
                connected = 1
                logger.log("INFO", "ALLTCPserver_init", "initializing connected "+ip+": "+str(oport))
        return sock

    def open_socket(self):
        global logger
        try:
            logger.log("INFO", "ALLTCPserver_open_socket", "starting server TCP socket")
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.host, self.port))
            self.server.listen(5)
        except socket.error as msg:
            logger.log("ERROR", "ALLTCPserver_open_socket", msg)
            self.server = None
        else:
            logger.log("INFO", "ALLTCPserver_open_socket", "server TCP socket started")

    def run(self):
        global logger
        global ramdisk
        global localip
        global slist
        #global executor
        global shuffleserver
        global parentip
        self.open_socket()
        
        while True:
            conn, addr = self.server.accept()
            peer = conn.getpeername()[0]
            try:
                #data = b''
                #while len(data) == 0:
                data = conn.recv(self.size)
            except socket.error:
                logger.log("ERROR", "ALLTCPserver_run", "socket exception: "+str(socket.error))
                break
            msg = data.decode("utf8").strip()
            logger.log("INFO", "ALLTCPserver_run", "received: "+str(msg))
            el = msg.split('#')
            if el[1] == 'READDIR':
                retlist = []
                retdict = dict()
                path = el[0]
                lsize = int(el[2])
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(self.size)
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                conn.close()

                if len(plist) == 0:
                    metadict = ramdisk.local_readdir(path, 0)
                elif len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    retdict = alltcpclient.sendall(path, 'READDIR', plist)
                    metadict = ramdisk.local_readdir(path, 0)

                retdict.update(metadict)
                sock = self.init(peer, 55002)
                psize = str(len(pickle.dumps(retdict)))
                while len(psize) < 10:
                    psize = psize + '\0'
                sock.send(bytes(psize, "utf8"))
                sock.send(pickle.dumps(retdict))
                sock.close()

            elif el[1] == 'MKDIR':
                path = el[0]
                mode = int(el[2])
                lsize = int(el[3])
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(self.size)
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                conn.close()

                if len(plist) == 0:
                    ramdisk.local_mkdir(path, mode)
                elif len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    recv = alltcpclient.sendall(path, 'MKDIR#'+str(mode), plist)
                    ramdisk.local_mkdir(path, mode)
                sock = self.init(peer, 55002)
                sock.send(bytes(str(1)+"\0\0\0\0\0\0\0\0\0", "utf8"))
                sock.send(bytes(str(0), "utf8"))
                sock.close()
                
            elif el[1] == 'RMDIR':
                path = el[0]
                lsize = int(el[2])
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(self.size)
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                conn.close()

                if len(plist) == 0:
                    ramdisk.local_rmdir(path)
                elif len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    recv = alltcpclient.sendall(path, 'RMDIR', plist)
                    ramdisk.local_rmdir(path)
                sock = self.init(peer, 55002)
                sock.send(bytes(str(1)+"\0\0\0\0\0\0\0\0\0", "utf8"))
                sock.send(bytes(str(0), "utf8"))
                sock.close()

            elif el[1] == 'MULTI':
                path = el[0]
                mode = int(el[2])
                algo = el[3]
                lsize = int(el[4])
                fsize = int(el[5])
                md5 = hashlib.md5()
                md5.update(path.encode())
                key = md5.hexdigest()
                
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(lsize)
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                #bufsize = 1048576
                temp = b''
                tcp_big = TCP_big()
                temp=tcp_big.recv(conn, fsize)
                #while len(temp) < fsize:
                #    if fsize - len(temp) > bufsize:
                #        data = conn.recv(bufsize)
                #    else:
                #        data = conn.recv(fsize-len(temp))
                #    temp = temp + data 
                #ramdisk.data[key] = pickle.loads(temp)
                odict = pickle.loads(temp)
                ramdisk.data.update(odict)
                logger.log("INFO", "ALLTCPserver_run_multicast", "received file: "+path+", "+"num_files:"+str(len(odict.keys())))
                conn.send(bytes(str(0), "utf8"))
                conn.close()

                if not S_ISDIR(ramdisk.files[path]['st_mode']):
                    ramdisk.local_create(path, mode, localip)
                    logger.log("INFO", "ALLTCPserver_run_multicast", path+" is a file, create entry")

                if len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    recv = alltcpclient.multicast(path, mode, plist, algo, odict)

                if algo == 'mst':
                    sock = self.init(peer, 55002)
                    sock.send(bytes(str(0), "utf8"))
                    sock.close()
                    
            #allgather starts here        
            elif el[1] == 'ALLGATHER':
                path = el[0]
                algo = el[2]
                lsize = int(el[3])
                msize = int(el[4])
                
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(self.size)
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))

                temp = b''
                while len(temp) < msize:
                    data = conn.recv(msize)
                    temp = temp + data
                metadict = pickle.loads(temp)
                
                
                allgatherthread = Allgatherthread(path, metadict, algo)
                while not allgatherthread.is_alive():
                    allgatherthread.start()

                if len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    ret = alltcpclient.allgather(path, plist, algo, metadict)
                while allgatherthread.is_alive():
                    sleep(0.1)
                    logger.log("INFO", "ALLtcpserver_allgather", "waiting for allgatherthread to finish")
                logger.log("INFO", "ALLtcpserver_allgather", "allgatherthread finished")    
                conn.send(bytes(str(0), "utf8"))
                
            elif el[1] == 'GATHER':
                path = el[0]
                algo = el[2]
                lsize = int(el[3])
                
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(self.size)
                    temp = temp + data 
                plist = pickle.loads(temp)
                #conn.send(bytes(str(0), "utf8"))
                if algo == 'mst':
                    conn.send(bytes(str(0), "utf8"))
                    conn.close()
                    
                    tempdict = defaultdict(bytes)
                    retdict = defaultdict(bytes)
                    #for f in ramdisk.wmeta:
                    #    if f != '/' and path == os.path.dirname(f):
                    #        md5 = hashlib.md5()
                    #        md5.update(f.encode())
                    #        key = md5.hexdigest()
                    #        tempdict[key] = ramdisk.wcache[key]
                    #for benchmark purpose remove the below segment for real use
                    for f in ramdisk.files:
                        if f != '/' and path == os.path.dirname(f):
                            md5 = hashlib.md5()
                            md5.update(f.encode())
                            key = md5.hexdigest()
                            if key in ramdisk.data:
                                tempdict[key] = ramdisk.data[key]
                            
                    if len(plist) > 0:
                        alltcpclient = ALLTCPclient()
                        retdict = alltcpclient.gather(path, plist, algo)
                    retdict.update(tempdict)

                    try:
                        sock = self.init(peer, 55003)
                        tsize = len(pickle.dumps(retdict))
                        dsize = str(len(pickle.dumps(retdict)))
                        while len(dsize) < 10:
                            dsize = dsize + '\0'
                        sock.send(bytes(dsize, "utf8"))
                        logger.log("INFO", "ALLTCPserver_gather", "sent dsize: "+str(dsize))
                        sock.recv(1)

                        data = pickle.dumps(retdict)
                        tcp_big = TCP_big()
                        tcp_big.send(sock, data, tsize)
                        #sent = 0
                        #while sent < tsize:
                        #    sent_iter = sock.send(data[sent:])
                        #    sent = sent + sent_iter
                        #    logger.log("INFO", "ALLTCPserver_gather", "sent "+str(sent)+" bytes")

                        sock.recv(1)
                        logger.log("INFO", "ALLTCPserver_gather", "sent retdict: "+str(len(retdict.keys())))
                        sock.close()
                    except socket.error as msgg:
                        logger.log("ERROR", "ALLTCPclient_gather", "Exception "+str(msgg))

                elif algo == 'seq':
                    retdict = defaultdict(bytes)
                    for f in ramdisk.wmeta:
                        if f != '/' and path == os.path.dirname(f):
                            md5 = hashlib.md5()
                            md5.update(f.encode())
                            key = md5.hexdigest()
                            retdict[key] = ramdisk.wcache[key]
                    tsize = len(pickle.dumps(retdict))
                    dsize = str(len(pickle.dumps(retdict)))
                    while len(dsize) < 10:
                        dsize = dsize + '\0'
                    conn.send(bytes(dsize, "utf8"))
                    logger.log("INFO", "ALLTCPserver_gather", "sent dsize: "+str(dsize))
                    conn.recv(1)
                    
                    data = pickle.dumps(retdict)
                    tcp_big = TCP_big()
                    tcp_big.send(conn, data, tsize)
                    #sent = 0
                    #while sent < tsize:
                    #    sent_iter = conn.send(data[sent:])
                    #    sent = sent + sent_iter
                    #    logger.log("INFO", "ALLTCPserver_gather", "sent "+str(sent)+" bytes")
                    conn.recv(1)
                    logger.log("INFO", "ALLTCPserver_gather", "sent retdict: "+str(len(retdict.keys())))
                    #conn.close()

            elif el[1] == 'SCATTER':
                path = el[0]
                algo = el[2]
                lsize = int(el[3])
                msize = int(el[4])
                dsize = int(el[5])
                mdict = dict()
                ddict = defaultdict(bytes)
                
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(lsize)
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))


                temp = b''
                tcp_big = TCP_big()
                temp = tcp_big.recv(conn, msize)
                #while len(temp) < msize:
                #    data = conn.recv(msize)
                #    temp = temp + data 
                mdict = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))

                temp = b''
                #while len(temp) < dsize:
                #    data = conn.recv(dsize)
                #    temp = temp + data
                temp = tcp_big.recv(conn, dsize)
                ddict = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))

                conn.close()

                #pop the files this node wants to keep
                portion = 1.0/(len(plist)+1)
                pathlist = list(mdict.keys())
                fcount = int(portion*len(pathlist))
                for i in range(fcount):
                    ok = pathlist.pop()
                    ramdisk.files[ok] = mdict.pop(ok)
                    ramdisk.files[ok]['location'] = localip
                    logger.log("INFO", "ALLTCPserver_run_scatter", "insert metadata of : "+ok)
                    md5 = hashlib.md5()
                    md5.update(ok.encode())
                    hk = md5.hexdigest()
                    ramdisk.data[hk] = ddict.pop(hk)
                    logger.log("INFO", "ALLTCPserver_run_scatter", "insert data of : "+ok)
                    tcpclient = TCPclient()
                    tcpclient.insertmeta(ok, ramdisk.files[ok])

                ret = 0
                if len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    ret = alltcpclient.scatter(path, plist, algo, mdict, ddict)

                if algo == 'mst':
                    sock = self.init(peer, 55002)
                    sock.send(bytes(str(ret), 'utf8'))
                    sock.close()

            elif el[1] == 'LOAD':
                src = el[0]
                dst = el[2]
                algo = el[3]
                lsize = int(el[4])
                outlsize = int(el[5])
                
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(lsize-len(temp))
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))


                temp = b''
                while len(temp) < outlsize:
                    data = conn.recv(outlsize-len(temp))
                    temp = temp + data 
                outl = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))

                conn.close()

                #pop the files this node wants to keep
                pathlist = list(outl)
                if len(plist) > 0:
                    portion = 1.0/(len(plist)+1)
                    f_fcount = portion*len(pathlist)
                    fcount = int(portion*len(pathlist))
                    if float(fcount) < f_fcount:
                        fcount = fcount+1
                    pl = []
                    for i in range(fcount):
                        f = pathlist.pop()
                        pl.append(f)
                    logger.log("INFO", "ALLtcpsever_LOAD", "locally process files: "+str(pl))
                else:
                    pl=list(pathlist)
                    logger.log("INFO", "ALLtcpsever_LOAD", "locally process files: "+str(pl))
                    
                loader = Loader('loader', src, dst, pl)
                while not loader.is_alive():
                    loader.start()

                ret = 0
                if len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    ret = alltcpclient.load(src, plist, pathlist, dst, algo)

                if algo == 'mst':
                    while loader.is_alive():
                        logger.log("INFO", "ALLtcpserver_LOAD", "waiting for local processing")
                        sleep(0.1)
                    sock = self.init(peer, 55002)
                    sock.send(bytes(str(ret), 'utf8'))
                    sock.close()


            elif el[1] == 'DUMP':
                src = el[0]
                dst = el[2]
                algo = el[3]
                lsize = int(el[4])
                outdsize = int(el[5])
                
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(lsize-len(temp))
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))


                temp = b''
                while len(temp) < outdsize:
                    data = conn.recv(outdsize-len(temp))
                    temp = temp + data 
                outdict = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))

                conn.close()

                #pop the files this node wants to keep
                pl = outdict.pop(localip)
                logger.log("INFO", "ALLtcpsever_DUMP", "locally process files: "+str(pl))

                dumper = Dumper('loader', src, dst, pl)
                while not dumper.is_alive():
                    dumper.start()

                ret = 0
                if len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    ret = alltcpclient.dump(src, plist, outdict, dst, algo)

                if algo == 'mst':
                    while dumper.is_alive():
                        logger.log("INFO", "ALLtcpserver_DUMP", "waiting for local processing")
                        sleep(0.1)
                    sock = self.init(peer, 55002)
                    sock.send(bytes(str(ret), 'utf8'))
                    sock.close()

            elif el[1] == 'SHUFFLE':
                path = el[0]
                dst = el[2]
                algo = el[3]
                lsize = int(el[4])
                dsize = int(el[5])
                
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(lsize)
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < dsize:
                    data = conn.recv(dsize)
                    temp = temp + data 
                metadict = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
                #initilize parentip
                parentip = peer

                shuffleserver = Shuffleserver('shuffleserver', 55004)
                while not shuffleserver.is_alive():
                    shuffleserver.start()

                #shuffler = Shuffler('shuffler', peer, path, dst, metadict, algo)
                #while not shuffler.is_alive():
                #    shuffler.start()
                    
                if len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    recv = alltcpclient.shuffle(path, dst, plist, metadict, algo)

                #while shuffleserver.is_alive() or shuffler.is_alive():
                #    sleep(1)
                #    logger.log("INFO", "ALLTCPserver_SHUFFLE", "waiting for local processing to finish")
                logger.log("INFO", "ALLTCPserver_SHUFFLE", "Shuffleserver am ready to send ack to "+parentip)
                sock = self.init(peer, 55002)
                sock.send(bytes('0', 'utf8'))
                sock.close()

            elif el[1] == 'SHUFFLE_LAUNCH':
                path = el[0]
                dst = el[2]
                algo = el[3]
                lsize = int(el[4])
                dsize = int(el[5])
                
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(lsize)
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < dsize:
                    data = conn.recv(dsize)
                    temp = temp + data 
                metadict = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
                #initilize parentip
                parentip = peer

                #shuffleserver = Shuffleserver('shuffleserver', 55004)
                #while not shuffleserver.is_alive():
                #    shuffleserver.start()

                shuffler = Shuffler('shuffler', peer, path, dst, metadict, algo)
                while not shuffler.is_alive():
                    shuffler.start()
                    
                if len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    recv = alltcpclient.shuffle_launch(path, dst, plist, metadict, algo)

                while shuffler.is_alive():
                    sleep(1)
                    logger.log("INFO", "ALLTCPserver_SHUFFLE_launch", "waiting for local processing to finish")
                logger.log("INFO", "ALLTCPserver_SHUFFLE_launch", "Shuffle completed, send ack to "+parentip)
                sock = self.init(peer, 55002)
                sock.send(bytes('0', 'utf8'))
                sock.close()

            elif el[1] == 'DISPATCH':
                lsize = int(el[2])
                tsize = int(el[3])
                
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(lsize-len(temp))
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < tsize:
                    data = conn.recv(tsize-len(temp))
                    temp = temp + data 
                tlist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                conn.close()

                logger.log("INFO", "ALLTCPserver_run_dispatch", "plist: "+str(plist)+"  "+"tlist: "+str(tlist))
                execlist = []
                if len(plist) > 0:
                    portion = 1.0/(len(plist)+1)
                    f_t_num = portion*len(tlist)
                    t_num = int(portion*len(tlist))
                    if float(t_num) < f_t_num:
                        t_num = t_num+1

                    for i in range(t_num):
                        if len(tlist) > 0:
                            task = tlist.pop()
                            execlist.append(task)
                    logger.log("INFO", "ALLTCPserver_run_dispatch", "push "+str(len(execlist))+" task into queue task")
                else:
                    execlist = list(tlist)
                    logger.log("INFO", "ALLTCPserver_run_dispatch", "push "+str(len(execlist))+" task into queue task")
                    
                if len(plist) == 0:
                    #while len(tlist) > 0:
                    #    task = tlist.pop()
                    #    execlist.append(task)
                        #executor.push(task)
                    #    logger.log("INFO", "ALLTCPserver_run_dispatch", "push task into queue task:"+task.desc)
                    executor=Executor(execlist)
                    while not executor.is_alive():
                        executor.start()    
                elif len(plist) > 0:
                    executor=Executor(execlist)
                    while not executor.is_alive():
                        executor.start()    
                    alltcpclient = ALLTCPclient()
                    recv = alltcpclient.dispatch(plist, tlist)

                #while len(executor.queue) > 0:
                #    t = executor.queue.pop()
                #    executor.readyqueue.put(t, True, None)
                while executor.is_alive():
                    sleep(0.1)
                    logger.log("INFO", "ALLTCpserver_run_dispatch", "waiting for local tasks to finish")
                logger.log("INFO", "ALLTCpserver_run_dispatch", "local tasks to done")
                sock = self.init(peer, 55007)
                sock.send(bytes(str(0), "utf8"))
                sock.close()

            elif el[1] == 'WAIT':
                lsize = int(el[2])
                
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(lsize-len(temp))
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                conn.close()

                retdict = {}
                if len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    retdict = alltcpclient.wait(plist)

                values = list(executor.smap.values())
                while sum(values) != 0:
                    sleep(1)
                    values = list(executor.smap.values())
                retdict.update(executor.smap)
                logger.log('INFO', 'ALLTCPserver_WAIT', 'retdict: '+str(retdict.values()))
                sock = self.init(peer, 55008)
                dsize = str(len(pickle.dumps(retdict)))
                while len(dsize) < 10:
                    dsize = dsize + "\0"
                sock.send(bytes(dsize, "utf8"))
                sock.recv(1)
                sock.send(pickle.dumps(retdict))
                sock.recv(1)
                sock.close()

            elif el[1] == 'STATE':
                lsize = int(el[2])
                
                conn.send(bytes(str(0), "utf8"))
                temp = b''
                while len(temp) < lsize:
                    data = conn.recv(lsize-len(temp))
                    temp = temp + data 
                plist = pickle.loads(temp)
                conn.send(bytes(str(0), "utf8"))
                conn.close()

                retq = []
                if len(plist) > 0:
                    alltcpclient = ALLTCPclient()
                    retq = alltcpclient.state(plist)

                for t in executor.fqueue:
                    retq.append(t)
                sock = self.init(peer, 55009)
                qsize = str(len(pickle.dumps(retq)))
                while len(qsize) < 10:
                    qsize = qsize + "\0"
                sock.send(bytes(qsize, "utf8"))
                sock.recv(1)
                sock.send(pickle.dumps(retq))
                sock.recv(1)
                sock.close()


class ALLTCPclient():
    def init(self, ip):
        global logger
        logger.log("INFO", "ALLTCPclient_init", "connect to "+ip)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        port = 55001
        connected = 0
        while connected == 0:
            try:
                sock.connect((ip, port))
            except socket.error:
                logger.log("ERROR", "ALLTCPclient_init", "connection to "+ip+":"+str(port)+" failed")
                sleep(1)
                continue
            else:
                connected = 1
        return sock
    
    def init_server(self):
        global logger
        logger.log("INFO", "ALLTCPclient_init_server", "initializing server to receive acks")
        ip = ''
        port = 55002
        started = 0
        while started == 0:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((ip, port))
                sock.listen(5)
            except socket.error as msg:
                logger.log("ERROR", "ALLTCPclient_init_server", str(msg))
                sleep(1)
                continue
            else:
                started = 1
                logger.log("INFO", "ALLTCPclient_init_server", "server started")
        return sock

    def init_executor_server(self, port):
        global logger
        logger.log("INFO", "ALLTCPclient_init_executor_server", "initializing server to receive acks")
        ip = ''
        started = 0
        while started == 0:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((ip, port))
                sock.listen(5)
            except socket.error as msg:
                logger.log("ERROR", "ALLTCPclient_init_executor_server", str(msg))
                sleep(1)
                continue
            else:
                started = 1
        return sock

    def sendall(self, path, msg, slist):
        global logger
        logger.log("INFO", "ALLTCPclient_sendall", "sending "+msg+" to all servers: "+str(len(slist)))

        global localip
        global ramdisk
        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 8192
        retlist = []
        retdict = dict()
        while len(targetlist) > 0:
            templist = []
            for i in range(int(len(targetlist)/2)+1):
                ip = targetlist.pop()
                templist.append(ip)
            plist.append(templist)
        logger.log("INFO", "ALLTCPclient_sendall", "list partition: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0
        #start ALLTCPreceiver here waiting for len(plist) acks
        if len(pmap.keys()) > 0:
            server=self.init_server()
            sendallthread = Sendallthread(server, pmap, retdict, msg)
            if not sendallthread.is_alive():
                sendallthread.start()

        
            for l in plist:
                ip = l[0]
                l.remove(ip)
                try:
                    s=self.init(ip)
                    s.send(bytes(path+'#'+msg+'#'+str(len(pickle.dumps(l))), "utf8"))
                    ret = s.recv(size)
                    s.send(pickle.dumps(l))
                    ret = s.recv(size)
                    s.close()
                except socket.error as msg:
                    logger.log("ERROR", "ALLTCPclient_sendall", msg)
                else:
                    logger.log("INFO", "ALLTCPclient_sendall", "send "+msg+" to "+ip+" list: "+str(l))

            while sendallthread.is_alive():
                sleep(0.1)
                logger.log("INFO", "ALLTCPclient_sendall", "waiting for sendallthread to finish")
            logger.log("INFO", "ALLTCPclient_sendall", "sendallthread finsihed")

        if msg == 'READDIR':
            return retdict
        else:
            return 0




    def multicast(self, path, mode, slist, algo, odict):
        global logger
        logger.log("INFO", "ALLTCPclient_multicast", "multicast "+path+" to "+str(len(slist))+" servers")
        global localip
        global ramdisk

        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
            
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 1024
        retdict = dict()

        #md5 = hashlib.md5()
        #md5.update(path.encode())
        #key = md5.hexdigest()

        if algo == 'seq':
            while len(targetlist) > 0:
                templist = []
                for i in range(1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)
        elif algo == 'mst':
            while len(targetlist) > 0:
                templist = []
                for i in range(int(len(targetlist)/2)+1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)            
        logger.log("INFO", "ALLTCPclient_multicast", "list partition: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0 #initialize the map to track each child's return status

        if algo == 'mst':
            if len(pmap.keys()) > 0:
                server=self.init_server()
                multicastthread = Multicastthread(server, pmap)
                if not multicastthread.is_alive():
                    multicastthread.start()

                #odict = defaultdict(bytes)
                #odict[key] = ramdisk.data[key]
                data = pickle.dumps(odict)
                #data = pickle.dumps(ramdisk.data[key])
                for l in plist:
                    ip = l[0]
                    l.remove(ip)
                    try:

                        s=self.init(ip)
                        s.send(bytes(path+'#MULTI#'+str(mode)+'#'+algo+'#'+str(len(pickle.dumps(l)))+'#'+str(len(data)), "utf8"))
                        ret = s.recv(size)
                        s.send(pickle.dumps(l))
                        ret = s.recv(size)

                        logger.log("INFO", "ALLTCPclient_multicast_mst", "trasnferring the data ")
                        length = len(data)
                        tcp_big = TCP_big()
                        tcp_big.send(s, data, length)
                        #sent = 0
                        #bufsize = 1048576
                        #while sent < length:
                        #    if length - sent > bufsize:
                        #        sent_iter = s.send(data[sent:sent+bufsize])
                        #    else:
                        #        sent_iter = s.send(data[sent:])
                        #    sent = sent+sent_iter
                        #    logger.log("INFO", "ALLTCPclient_multicast_mst", "sent "+str(sent)+" bytes")
                        ret = s.recv(size)
                        s.close()
                    except socket.error as msg:
                        logger.log("ERROR", "ALLTCPclient_multicast_mst", msg)
                    else:
                        logger.log("INFO", "ALLTCPclient_multicast_mst", "multicasting "+path+" to "+str(len(slist))+" servers")

                while multicastthread.is_alive():
                    logger.log("INFO", "ALLTCPclient_multicast_mst", "waiting for multicastthread to finish")
                    sleep(0.1)
                logger.log("INFO", "ALLTCPclient_multicast_mst", "multicastthread finished, retdict has "+str(sum(list(pmap.values())))+" records")
        elif algo == 'seq':
            if len(pmap.keys()) > 0:
                data = pickle.dumps(odict)
                for l in plist:
                    ip = l[0]
                    l.remove(ip)
                    try:
                        s=self.init(ip)
                        s.send(bytes(path+'#MULTI#'+str(mode)+'#'+algo+'#'+str(len(pickle.dumps(l)))+'#'+str(len(data)), "utf8"))
                        ret = s.recv(size)
                        s.send(pickle.dumps(l))
                        ret = s.recv(size)
                        length = len(data)
                        tcp_big = TCP_big()
                        tcp_big.send(s, data, length)
                        #sent = 0
                        #while sent < length:
                        #    sent_iter = s.send(data[sent:])
                        #    sent = sent+sent_iter
                        #    logger.log("INFO", "ALLTCPclient_multicast_seq", "sent "+str(sent)+" bytes")
                        ret = s.recv(size)
                        s.close()
                    except socket.error as msg:
                        logger.log("ERROR", "ALLTCPclient_multicast_seq", msg)
                    else:
                        logger.log("INFO", "ALLTCPclient_multicast_seq", "multicasting "+path+" to "+str(len(slist))+" servers")
                
            return 0
        return 0
    
    #allgather starts here
    def allgather(self, path, slist, algo, flist):
        global logger
        logger.log("INFO", "ALLTCPclient_allgather", "allgather "+path+" on "+str(len(slist))+" servers")
        global localip
        global ramdisk

        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
            
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 1024
        retdict = dict()

        if algo == 'seq':
            while len(targetlist) > 0:
                templist = []
                for i in range(1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)
        elif algo == 'mst':
            while len(targetlist) > 0:
                templist = []
                for i in range(int(len(targetlist)/2)+1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)            
        logger.log("INFO", "ALLTCPclient_allgather", "list gather: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0 #initialize the map to track each child's return status

        if algo == 'mst':
            if len(pmap.keys()) > 0:
                for l in plist:
                    ip = l[0]
                    l.remove(ip)
                    try:
                        s=self.init(ip)
                        fdata = pickle.dumps(flist)
                        s.send(bytes(path+'#ALLGATHER#'+algo+'#'+str(len(pickle.dumps(l)))+'#'+str(len(fdata)), "utf8"))
                        ret = s.recv(size)
                        s.send(pickle.dumps(l))
                        ret = s.recv(size)
                        s.send(fdata)
                        ret = s.recv(size)
                        s.close()
                    except socket.error as msg:
                        logger.log("ERROR", "ALLTCPclient_allgather", msg)
                    else:
                        logger.log("INFO", "ALLTCPclient_allgather", "allgathering "+path+" on "+str(len(l))+" servers")
                logger.log("INFO", "ALLTCPclient_allgather", "allgathering finished, pmap has "+str(sum(list(pmap.values())))+" records")
                return 0
            return 0
        return 0

    def gather(self, path, slist, algo):
        global logger
        logger.log("INFO", "ALLTCPclient_gather", "gather "+path+" from "+str(len(slist))+" servers")
        global localip
        global ramdisk

        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
            
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 1024
        retdict = dict()

        if algo == 'seq':
            while len(targetlist) > 0:
                templist = []
                for i in range(1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)
        elif algo == 'mst':
            while len(targetlist) > 0:
                templist = []
                for i in range(int(len(targetlist)/2)+1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)            
        logger.log("INFO", "ALLTCPclient_gather", "list gather: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0 #initialize the map to track each child's return status

        retdict = defaultdict(bytes)
        #start server first then put in background
        if algo == 'mst':
            if len(pmap.keys()) > 0:
                server=self.init_executor_server(55003)
                gatherthread = Gatherthread(server, retdict, pmap)
                if not gatherthread.is_alive():
                    gatherthread.start()
                
                for l in plist:
                    ip = l[0]
                    l.remove(ip)
                    try:
                        s=self.init(ip)
                        s.send(bytes(path+'#GATHER#'+algo+'#'+str(len(pickle.dumps(l))), "utf8"))
                        ret = s.recv(size)
                        s.send(pickle.dumps(l))
                        ret = s.recv(size)
                        s.close()
                    except socket.error as msg:
                        logger.log("ERROR", "ALLTCPclient_gather", msg)
                    else:
                        logger.log("INFO", "ALLTCPclient_gather", "gatheringing "+path+" from "+str(len(l))+" servers")
                            
                while gatherthread.is_alive():
                    logger.log("INFO", "ALLTCPclient_gather", "waiting for gatherthread to finish")
                    sleep(0.1)
                logger.log("INFO", "ALLTCPclient_gather", "gatherthread finished, retdcit has "+str(len(retdict.keys()))+" records")
        elif algo == 'seq':
            for l in plist:
                ip = l[0]
                try:
                    s=self.init(ip)
                    s.send(bytes(path+'#GATHER#'+algo+'#'+str(len(pickle.dumps(l))), "utf8"))
                    ret = s.recv(size)
                    s.send(pickle.dumps(l))
                    ret = s.recv(10)
                    dsize = int(ret.decode("utf8").strip('\0'))
                    logger.log("INFO", "Gather_seq", "recv size: "+str(dsize)+" for gather from "+ip)
                    s.send(bytes('0', "utf8"))
                
                    temp=b''
                    tcp_big = TCP_big()
                    temp = tcp_big.recv(s, dize)
                    #buffersize = 1048576
                    #while len(temp) < dsize:
                    #    if len(temp)+buffersize < dsize:
                    #        data = s.recv(buffersize)
                    #    else:
                    #        data = s.recv(dsize - len(temp))
                    #    logger.log("INFO", "Gather_seq", "received "+str(len(data))+" bytes")
                    #    temp = temp + data                
                    retdict.update(pickle.loads(temp))
                    s.send(bytes('0', "utf8"))
                    logger.log("INFO", "Gather_seq", "recv dict: "+str(len(retdict.keys()))+" for gather from "+ip)
                    logger.log("INFO", "Gather_seq", "recv ack for gather from "+ip)
                    
                    #s.close()
                except socket.error as msg:
                    logger.log("ERROR", "ALLTCPclient_gather", msg)
                except Exception as msg:
                    logger.log("ERROR", "ALLTCPclient_gather", "other exception: "+str(msg))
                else:
                    logger.log("INFO", "ALLTCPclient_gather", "gatheringing "+path+" from "+str(len(l))+" servers")
            logger.log("INFO", "ALLTCPclient_gather", "gatherthread finished, retdcit has "+str(len(retdict.keys()))+" records")
        return retdict
        

    def scatter(self, path, slist, algo, mdict, ddict):
        global logger
        logger.log("INFO", "ALLTCPclient_scatter", "scatter "+path+" to "+str(len(slist))+" servers")
        global localip
        global ramdisk
 
        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
            
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 1024
        retdict = dict()

                
        if algo == 'seq':
            while len(targetlist) > 0:
                templist = []
                for i in range(1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)
        elif algo == 'mst':
            while len(targetlist) > 0:
                templist = []
                for i in range(int(len(targetlist)/2)+1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)            
        logger.log("INFO", "ALLTCPclient_scatter", "list scatter: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0 #initialize the map to track each child's return status

        if algo == 'mst':
            if len(pmap.keys()) > 0:
                server = self.init_server()
                scatterthread = Scatterthread(server, pmap)
                if not scatterthread.is_alive():
                    scatterthread.start()
                pathlist = list(mdict.keys())
                num_file = len(pathlist)
                for l in plist:
                    outmdict = dict()
                    outdict = defaultdict(bytes)
                    portion = float(len(l))/len(slist)
                    fcount = int(num_file*portion)
                    for i in range(fcount):
                        ok = pathlist.pop()
                        outmdict[ok] = mdict.pop(ok)
                        md5 = hashlib.md5()
                        md5.update(ok.encode())
                        hk = md5.hexdigest()
                        outdict[hk] = ddict.pop(hk)
                
                    ip = l[0]
                    l.remove(ip)
                    try:
                        s=self.init(ip)
                        mdata = pickle.dumps(outmdict)
                        data = pickle.dumps(outdict)
                        s.send(bytes(path+'#SCATTER#'+algo+'#'+str(len(pickle.dumps(l)))+'#'+str(len(mdata))+'#'+str(len(data)), "utf8"))
                        ret = s.recv(size)
                        s.send(pickle.dumps(l))
                        ret = s.recv(size)
                        mlength = len(mdata)
                        tcp_big = TCP_big()
                        tcp_big.send(s, mdata, mlength)
                        #s.send(pickle.dumps(outmdict))
                        ret = s.recv(size)

                        length = len(data)
                        tcp_big.send(s, data, length)
                        #sent = 0
                        #while sent < length:
                        #    sent_iter = s.send(data[sent:])
                        #    sent = sent+sent_iter
                        #    logger.log("INFO", "ALLTCPclient_gather", "sent "+str(sent)+" bytes")
                        ret = s.recv(size)
                        s.close()
                    except socket.error as msg:
                        logger.log("ERROR", "ALLTCPclient_scatter", msg)
                    else:
                        logger.log("INFO", "ALLTCPclient_scatter", "scattering "+str(len(outdict.keys()))+" files to "+str(len(l)+1)+" servers")
                while scatterthread.is_alive():
                    logger.log("INFO", "ALLTCPclient_scatter_mst", "waiting for scaterthread to finish")
                    sleep(0.1)
                logger.log("INFO", "ALLTCPclient_scatter_mst", "scatterthread finished, retdcit has "+str(sum(list(pmap.values())))+" records")
            return 0
        elif algo == 'seq':
            if len(pmap.keys()) > 0:
                tlist = list(slist)
                for l in plist:
                    outmdict = dict()
                    outdict = defaultdict(bytes)
                    portion = float(len(l))/len(tlist)
                    pathlist = list(mdict.keys())
                    fcount = int(len(pathlist)*portion)
                    for i in range(fcount):
                        ok = pathlist.pop()
                        outmdict[ok] = mdict.pop(ok)
                        md5 = hashlib.md5()
                        md5.update(ok.encode())
                        hk = md5.hexdigest()
                        outdict[hk] = ddict.pop(hk)
                
                    ip = l[0]
                    tlist.remove(ip)
                    l.remove(ip)
                    try:
                        s=self.init(ip)
                        mdata = pickle.dumps(outmdict)
                        data = pickle.dumps(outdict)
                        s.send(bytes(path+'#SCATTER#'+algo+'#'+str(len(pickle.dumps(l)))+'#'+str(len(mdata))+'#'+str(len(data)), "utf8"))
                        ret = s.recv(size)
                        s.send(pickle.dumps(l))
                        ret = s.recv(size)

                        mlength = len(mdata)
                        tcp_big = TCP_big()
                        tcp_big.send(s, mdata, mlength)
                        ret = s.recv(size)

                        length = len(data)
                        tcp_big.send(s, data, length)
                        ret = s.recv(size)
                        s.close()
                    except socket.error as msg:
                        logger.log("ERROR", "ALLTCPclient_scatter_seq", msg)
                    else:
                        logger.log("INFO", "ALLTCPclient_scatter_seq", "scattering "+str(fcount)+" files to "+str(len(slist))+" servers")
            return 0
        else:
            return 0        

    def shuffle(self, path, dst, slist, metadict, algo):
        global logger
        logger.log("INFO", "ALLTCPclient_shuffle", "shuffle "+path+" to "+dst+" on "+str(len(slist))+" nodes")
        global localip
        global ramdisk
        #global shufflemap

        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
            
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 1024
        retdict = dict()

        md5 = hashlib.md5()
        md5.update(path.encode())
        key = md5.hexdigest()

        while len(targetlist) > 0:
            templist = []
            for i in range(int(len(targetlist)/2)+1):
                ip = targetlist.pop()
                templist.append(ip)
            plist.append(templist)            
        logger.log("INFO", "ALLTCPclient_shuffle", "list partition: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0 #initialize the map to track each child's return status

        server=self.init_server()
        shufflethread = Shufflethread(server, pmap)
        if not shufflethread.is_alive():
            shufflethread.start()

        for l in plist:
            ip = l[0]
            l.remove(ip)
            try:
                s=self.init(ip)
                s.send(bytes(path+'#SHUFFLE#'+dst+'#'+algo+'#'+str(len(pickle.dumps(l)))+'#'+str(len(pickle.dumps(metadict))), "utf8"))
                ret = s.recv(size)
                s.send(pickle.dumps(l))
                ret = s.recv(size)
                s.send(pickle.dumps(metadict))
                ret = s.recv(size)
                s.close()
            except socket.error as msg:
                logger.log("ERROR", "ALLTCPclient_shuffle", msg)
            else:
                logger.log("INFO", "ALLTCPclient_shuffle", "shuffle "+path+" to "+dst+" on "+str(len(slist))+" nodes")
                
        while shufflethread.is_alive():
            logger.log("INFO", "ALLTCPclient_shuffle_mst", "waiting for shufflethread to finish")
            sleep(0.1)
        logger.log("INFO", "ALLTCPclient_shuffle_mst", "shufflethread finished, retdcit has "+str(sum(list(pmap.values())))+" records")
        #if len(pmap.keys()) > 0 and algo == 'mst':
        #
        #    while True:
        #        summ=0
        #        for k in pmap:
        #            summ = summ + pmap[k]
        #        if summ == len(pmap):
        #            break
        #        try:
        #            conn, addr = server.accept()
        #            peer = conn.getpeername()[0]
        #            ret = conn.recv(size)
        #            rv = ret.decode('utf8').strip('\0')
        #            logger.log("INFO", "ALLTCPclient_shuffle", "recv ack for shuffle from "+str(conn.getpeername()))
        #            pmap[peer] = 1
        #        except socket.error as msgg:
        #            logger.log("ERROR", "ALLTCPclient_shuffle", "socket exception: "+str(msgg))
        #        except Exception as msgg:
        #            logger.log("ERROR", "ALLTCPclient_shuffle", "other exception: "+str(msgg))
        #        else:
        #            conn.close()
        #    server.close()
        #    return 0
        #else:
        #    return 0

    def shuffle_launch(self, path, dst, slist, metadict, algo):
        global logger
        logger.log("INFO", "ALLTCPclient_shuffle_launch", "shuffle "+path+" to "+dst+" on "+str(len(slist))+" nodes")
        global localip
        global ramdisk
        #global shufflemap

        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
            
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 1024
        retdict = dict()

        md5 = hashlib.md5()
        md5.update(path.encode())
        key = md5.hexdigest()

        while len(targetlist) > 0:
            templist = []
            for i in range(int(len(targetlist)/2)+1):
                ip = targetlist.pop()
                templist.append(ip)
            plist.append(templist)            
        logger.log("INFO", "ALLTCPclient_shuffle_launch", "list partition: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0 #initialize the map to track each child's return status

        for l in plist:
            ip = l[0]
            l.remove(ip)
            try:
                s=self.init(ip)
                s.send(bytes(path+'#SHUFFLE_LAUNCH#'+dst+'#'+algo+'#'+str(len(pickle.dumps(l)))+'#'+str(len(pickle.dumps(metadict))), "utf8"))
                ret = s.recv(size)
                s.send(pickle.dumps(l))
                ret = s.recv(size)
                s.send(pickle.dumps(metadict))
                ret = s.recv(size)
                s.close()
            except socket.error as msg:
                logger.log("ERROR", "ALLTCPclient_shuffle_launch", msg)
            else:
                logger.log("INFO", "ALLTCPclient_shuffle_launch", "shuffle "+path+" to "+dst+" on "+str(len(slist))+" nodes")

        if len(pmap.keys()) > 0 and algo == 'mst':
            server=self.init_server()
            while True:
                summ=0
                for k in pmap:
                    summ = summ + pmap[k]
                if summ == len(pmap):
                    break
                try:
                    conn, addr = server.accept()
                    peer = conn.getpeername()[0]
                    ret = conn.recv(size)
                    rv = ret.decode('utf8').strip('\0')
                    logger.log("INFO", "ALLTCPclient_shuffle_launch", "recv ack for shuffle_launch from "+str(conn.getpeername()))
                    pmap[peer] = 1
                except socket.error as msgg:
                    logger.log("ERROR", "ALLTCPclient_shuffle_launch", "socket exception: "+str(msgg))
                except Exception as msgg:
                    logger.log("ERROR", "ALLTCPclient_shuffle_launch", "other exception: "+str(msgg))
                else:
                    conn.close()
            server.close()
            return 0
        else:
            return 0


    def dispatch(self, slist, tlist):
        global logger
        logger.log("INFO", "ALLTCPclient_dispatch", "dispatch "+str(len(tlist))+" tasks to "+str(len(slist))+" servers")
        global localip
        #global executor

        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
            
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 1024
        retdict = dict()
        num_task = len(tlist)
        length = len(targetlist)
        
        while len(targetlist) > 0:
            templist = []
            for i in range(int(len(targetlist)/2)+1):
                ip = targetlist.pop()
                templist.append(ip)
            plist.append(templist)            
        logger.log("INFO", "ALLTCPclient_dispatch", "list partition: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0 #initialize the map to track each child's return status

        for l in plist:
            temp_tlist = []
            portion = float(len(l))/(2*len(l)-1)
            num_task = len(tlist)
            f_num = num_task*portion
            t_num = int(num_task*portion)
            if float(t_num) < f_num:
                t_num = t_num+1
            print("num_task: "+str(num_task)+"   t_num: "+str(t_num)+"    portion: "+str(portion))
            for i in range(t_num):
                if len(tlist) > 0:
                    t = tlist.pop()
                    temp_tlist.append(t)
                
            ip = l[0]
            l.remove(ip)
            try:
                s=self.init(ip)
                s.send(bytes('#DISPATCH#'+str(len(pickle.dumps(l)))+'#'+str(len(pickle.dumps(temp_tlist))), "utf8"))
                ret = s.recv(size)
                s.send(pickle.dumps(l))
                ret = s.recv(size)
                s.send(pickle.dumps(temp_tlist))
                ret = s.recv(size)
                s.close()
            except socket.error as msg:
                logger.log("ERROR", "ALLTCPclient_dispatch", msg)
            else:
                logger.log("INFO", "ALLTCPclient_dispatch", "dispatching "+str(len(temp_tlist))+" tasks to "+ip)

        if len(pmap.keys()) > 0:
            server=self.init_executor_server(55007)
            while True:
                summ=0
                for k in pmap:
                    summ = summ + pmap[k]
                if summ == len(pmap):
                    break
                try:
                    conn, addr = server.accept()
                    peer = conn.getpeername()[0]
                    ret = conn.recv(size)
                    msize = int(ret.decode("utf8").strip('\0'))
                    logger.log("INFO", "ALLTCPclient_dispatch", "recv ack for dispatch from "+str(conn.getpeername()))
                    pmap[peer] = 1
                except socket.error as msgg:
                    logger.log("ERROR", "ALLTCPclient_dispatch", "socket exception: "+str(msgg))
                except Exception as msgg:
                    logger.log("ERROR", "ALLTCPclient_dispatch", "other exception: "+str(msgg))
                else:
                    conn.close()
            server.close()
            logger.log("INFO", "ALLTCPclient_dispatch", "now the executor queue has "+str(len(tlist))+" tasks")
            return msize
        else:
            return 0

    def wait(self, slist):
        global logger
        logger.log("INFO", "ALLTCPclient_wait", "wait for "+str(len(slist))+" servers to finish")
        global localip
        global executor

        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
            
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 1024
        retdict = dict()

        while len(targetlist) > 0:
            templist = []
            for i in range(int(len(targetlist)/2)+1):
                ip = targetlist.pop()
                templist.append(ip)
            plist.append(templist)            
        logger.log("INFO", "ALLTCPclient_wait", "list partition: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0 #initialize the map to track each child's return status

        for l in plist:
            ip = l[0]
            l.remove(ip)
            try:
                s=self.init(ip)
                s.send(bytes('#WAIT#'+str(len(pickle.dumps(l))), "utf8"))
                ret = s.recv(size)
                s.send(pickle.dumps(l))
                ret = s.recv(size)
                s.close()
            except socket.error as msg:
                logger.log("ERROR", "ALLTCPclient_wait", msg)
            else:
                logger.log("INFO", "ALLTCPclient_wait", "waiting for"+str(len(slist))+" servers to finish")

        if len(pmap.keys()) > 0:
            server=self.init_executor_server(55008)
            while True:
                summ=0
                for k in pmap:
                    summ = summ + pmap[k]
                if summ == len(pmap):
                    break
                try:
                    conn, addr = server.accept()
                    peer = conn.getpeername()[0]
                    ret = conn.recv(10)
                    dsize = int(ret.decode("utf8").strip('\0'))
                    conn.send(bytes('0', 'utf8'))
                    temp = b''
                    while len(temp) < dsize:
                        data = conn.recv(dsize-len(temp))
                        temp = temp + data 
                    tempdict = pickle.loads(temp)
                    retdict.update(tempdict)
                    conn.send(bytes('0', 'utf8'))
                    logger.log("INFO", "ALLTCPclient_wait", "recv ack for wait from "+str(conn.getpeername()))
                    pmap[peer] = 1
                except socket.error as msgg:
                    logger.log("ERROR", "ALLTCPclient_wait", "socket exception: "+str(msgg))
                except Exception as msgg:
                    logger.log("ERROR", "ALLTCPclient_wait", "other exception: "+str(msgg))
                else:
                    conn.close()
            server.close()
            logger.log("INFO", "ALLTCPclient_wait", "wait finsihed")
            return retdict
        else:
            return retdict

    def state(self, slist):
        global logger
        logger.log("INFO", "ALLTCPclient_state", "wait for "+str(len(slist))+" servers to finish")
        global localip
        global executor

        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
            
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 1024
        retq = []

        while len(targetlist) > 0:
            templist = []
            for i in range(int(len(targetlist)/2)+1):
                ip = targetlist.pop()
                templist.append(ip)
            plist.append(templist)            
        logger.log("INFO", "ALLTCPclient_state", "list partition: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0 #initialize the map to track each child's return status

        for l in plist:
            ip = l[0]
            l.remove(ip)
            try:
                s=self.init(ip)
                s.send(bytes('#STATE#'+str(len(pickle.dumps(l))), "utf8"))
                ret = s.recv(size)
                s.send(pickle.dumps(l))
                ret = s.recv(size)
                s.close()
            except socket.error as msg:
                logger.log("ERROR", "ALLTCPclient_state", msg)
            else:
                logger.log("INFO", "ALLTCPclient_state", "waiting for"+str(len(slist))+" servers to finish")

        if len(pmap.keys()) > 0:
            server=self.init_executor_server(55009)
            while True:
                summ=0
                for k in pmap:
                    summ = summ + pmap[k]
                if summ == len(pmap):
                    break
                try:
                    conn, addr = server.accept()
                    peer = conn.getpeername()[0]
                    ret = conn.recv(10)
                    qsize = int(ret.decode("utf8").strip('\0'))
                    conn.send(bytes('0', 'utf8'))
                    temp = b''
                    while len(temp) < qsize:
                        data = conn.recv(qsize-len(temp))
                        temp = temp + data 
                    tempq = pickle.loads(temp)
                    retq.extend(tempq)
                    conn.send(bytes('0', 'utf8'))
                    logger.log("INFO", "ALLTCPclient_state", "recv ack for wait from "+str(conn.getpeername()))
                    pmap[peer] = 1
                except socket.error as msgg:
                    logger.log("ERROR", "ALLTCPclient_state", "socket exception: "+str(msgg))
                except Exception as msgg:
                    logger.log("ERROR", "ALLTCPclient_state", "other exception: "+str(msgg))
                else:
                    conn.close()
            server.close()
            logger.log("INFO", "ALLTCPclient_state", "wait finsihed")
            return retq
        else:
            return retq

    def load(self, src, slist, flist, dst, algo):
        global logger
        logger.log("INFO", "ALLTCPclient_load", "load "+str(len(flist))+" files from "+src+" to "+dst+" on "+str(len(slist))+" servers")
        logger.log("INFO", "ALLTCPclient_load", "load flist: "+str(flist))
        global localip
        global ramdisk
        
        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
            
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 1024
        retdict = dict()

                
        if algo == 'seq':
            while len(targetlist) > 0:
                templist = []
                for i in range(1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)
        elif algo == 'mst':
            while len(targetlist) > 0:
                templist = []
                for i in range(int(len(targetlist)/2)+1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)            
        logger.log("INFO", "ALLTCPclient_load", "list load: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0 #initialize the map to track each child's return status

        fnum = int(len(flist))
        for l in plist:
            outl = []
            portion = float(len(l))/(2*len(l)-1)
            fnum = len(flist)
            f_fcount = fnum*portion
            fcount = int(fnum*portion)
            if float(fcount) < f_fcount:
                fcount = fcount+1
            for i in range(fcount):
                if len(flist) > 0:
                    of = flist.pop()
                    outl.append(of)
                
            ip = l[0]
            l.remove(ip)
            try:
                s=self.init(ip)
                s.send(bytes(src+'#LOAD#'+dst+'#'+algo+'#'+str(len(pickle.dumps(l)))+'#'+str(len(pickle.dumps(outl))), "utf8"))
                ret = s.recv(size)
                s.send(pickle.dumps(l))
                ret = s.recv(size)
                s.send(pickle.dumps(outl))
                ret = s.recv(size)
                s.close()
            except socket.error as msg:
                logger.log("ERROR", "ALLTCPclient_load", msg)
            else:
                logger.log("INFO", "ALLTCPclient_load", "load "+str(len(flist))+" files from "+src+" to "+dst+" on "+str(len(slist))+" servers")

        
        if len(pmap.keys()) > 0 and algo == 'mst':
            server=self.init_server()
            while True:
                summ=0
                for k in pmap:
                    summ = summ + pmap[k]
                if summ == len(pmap):
                    break
                try:
                    conn, addr = server.accept()
                    peer = conn.getpeername()[0]
                    ret = conn.recv(size)
                    rv = ret.decode('utf8').strip('\0')
                    logger.log("INFO", "ALLTCPclient_load", "recv ack for load from "+str(conn.getpeername()))
                    pmap[peer] = 1
                except socket.error as msgg:
                    logger.log("ERROR", "ALLTCPclient_load", "socket exception: "+str(msgg))
                except Exception as msgg:
                    logger.log("ERROR", "ALLTCPclient_load", "other exception: "+str(msgg))
                else:
                    conn.close()
            server.close()
            return rv
        else:
            return 0

    def dump(self, src, slist, fdict, dst, algo):
        global logger
        logger.log("INFO", "ALLTCPclient_dump", "dump "+str(len(fdict.keys()))+" files from "+src+" to "+dst+" on "+str(len(slist))+" servers")
        logger.log("INFO", "ALLTCPclient_dump", "dump fdict: "+str(fdict))
        global localip
        global ramdisk
        
        targetlist = list(slist)
        if localip in targetlist:
            targetlist.remove(localip)
            
        plist = []
        pmap = dict() #ip as key, value:1-returned, 0-pending
        size = 1024
        retdict = dict()
                
        if algo == 'seq':
            while len(targetlist) > 0:
                templist = []
                for i in range(1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)
        elif algo == 'mst':
            while len(targetlist) > 0:
                templist = []
                for i in range(int(len(targetlist)/2)+1):
                    ip = targetlist.pop()
                    templist.append(ip)
                plist.append(templist)            
        logger.log("INFO", "ALLTCPclient_dump", "dump list: "+str(plist))
        for l in plist:
            pmap[l[0]] = 0 #initialize the map to track each child's return status

        for l in plist:
            outdict = dict()
            for el in l:
                outdict[el]=fdict.pop(el)
                
            ip = l[0]
            l.remove(ip)
            try:
                s=self.init(ip)
                s.send(bytes(src+'#DUMP#'+dst+'#'+algo+'#'+str(len(pickle.dumps(l)))+'#'+str(len(pickle.dumps(outdict))), "utf8"))
                ret = s.recv(size)
                s.send(pickle.dumps(l))
                ret = s.recv(size)
                s.send(pickle.dumps(outdict))
                ret = s.recv(size)
                s.close()
            except socket.error as msg:
                logger.log("ERROR", "ALLTCPclient_dump", msg)
            else:
                logger.log("INFO", "ALLTCPclient_dump", "load "+str(len(outdict.keys()))+" files from "+src+" to "+dst+" on "+str(len(slist))+" servers")

        
        if len(pmap.keys()) > 0 and algo == 'mst':
            server=self.init_server()
            while True:
                summ=0
                for k in pmap:
                    summ = summ + pmap[k]
                if summ == len(pmap):
                    break
                try:
                    conn, addr = server.accept()
                    peer = conn.getpeername()[0]
                    ret = conn.recv(size)
                    rv = ret.decode('utf8').strip('\0')
                    logger.log("INFO", "ALLTCPclient_dump", "recv ack for dump from "+str(conn.getpeername()))
                    pmap[peer] = 1
                except socket.error as msgg:
                    logger.log("ERROR", "ALLTCPclient_dump", "socket exception: "+str(msgg))
                except Exception as msgg:
                    logger.log("ERROR", "ALLTCPclient_dump", "other exception: "+str(msgg))
                else:
                    conn.close()
            server.close()
            return rv
        else:
            return 0

#Scatterthread
class Scatterthread(threading.Thread):
    def __init__(self, server, pmap):
        threading.Thread.__init__(self)
        self.server = server
        self.pmap = pmap
        
    def run(self):
        size = 1024
        global logger
        logger.log("INFO", "Scatterthread_run", "Scatterthread thread started")
        while True:
            summ=0
            for k in self.pmap:
                summ = summ + self.pmap[k]
            if summ == len(self.pmap):
                break
            try:
                conn, addr = self.server.accept()
                peer = conn.getpeername()[0]
                ret = conn.recv(size)
                rv = ret.decode('utf8').strip('\0')
                logger.log("INFO", "Scatterthread_run", "recv ack for scatter from "+str(conn.getpeername()))
                self.pmap[peer] = 1
            except socket.error as msgg:
                logger.log("ERROR", "Scatterthread_run", "socket exception: "+str(msgg))
            except Exception as msgg:
                logger.log("ERROR", "Scatterthread_run", "other exception: "+str(msgg))
            else:
                conn.close()
        self.server.close()
        logger.log("INFO", "Scatterthread_run", "Scatterthread thread finished")

#senallthread
class Sendallthread(threading.Thread):
    def __init__(self, server, pmap, retdict, msg):
        threading.Thread.__init__(self)
        self.server = server
        self.pmap = pmap
        self.retdict = retdict
        self.msg = msg
        
    def run(self):
        size = 1024
        global logger
        logger.log("INFO", "Sendallthread_run", "multicastthread thread started")

        while True:
            summ=0
            for k in self.pmap:
                summ = summ + self.pmap[k]
            if summ == len(self.pmap):
                break
            conn, addr = self.server.accept()
            try:
                peer = conn.getpeername()[0]
                ret = conn.recv(10)
                msize = int(ret.decode("utf8").strip('\0'))

                if self.msg.split('#')[0] == 'READDIR':
                    temp = b''
                    while len(temp) < msize:
                        data = conn.recv(size)
                        temp = temp+data                
                    self.retdict.update(pickle.loads(temp))
                    logger.log("INFO", "Sendallthread", "recv ack for READDIR from "+str(conn.getpeername()))
                elif self.msg.split('#')[0] == 'MKDIR':
                    data = conn.recv(msize)
                    logger.log("INFO", "Sendallthread", "recv ack for MKDIR from "+str(conn.getpeername()))
                elif self.msg.split('#')[0] == 'RMDIR':
                    data = conn.recv(msize)
                    logger.log("INFO", "Sendallthread", "recv ack for RKDIR from "+str(conn.getpeername()))
                else:
                    data = conn.recv(msize)
                    logger.log("INFO", "Sendallthread", "recv ack for SHUFFLE from "+str(conn.getpeername()))
                self.pmap[peer] = 1
            except socket.error as msgg:
                logger.log("ERROR", "Sendallthread", "socket exception: "+str(msgg))
            except Exception as msgg:
                logger.log("ERROR", "Sendallthread", "other exception: "+str(msgg))
        self.server.close()
        logger.log("INFO", "Sendallthread_run", "multicastthread thread finished")


#Multicastthread
class Multicastthread(threading.Thread):
    def __init__(self, server, pmap):
        threading.Thread.__init__(self)
        self.server = server
        self.pmap = pmap
        
    def run(self):
        size = 1024
        global logger
        logger.log("INFO", "Multicastthread_run", "multicastthread thread started")
        while True:
            summ=0
            for k in self.pmap:
                summ = summ + self.pmap[k]
            if summ == len(self.pmap):
                break
            try:
                conn, addr = self.server.accept()
                peer = conn.getpeername()[0]
                ret = conn.recv(size)
                msize = int(ret.decode("utf8").strip('\0'))
                logger.log("INFO", "Multicastthread_run", "recv ack for multicast from "+str(conn.getpeername()))
                self.pmap[peer] = 1
            except socket.error as msgg:
                logger.log("ERROR", "Multicastthread_run", "socket exception: "+str(msgg))
            except Exception as msgg:
                logger.log("ERROR", "Multicastthread_run", "other exception: "+str(msgg))
            else:
                conn.close()
        self.server.close()
        logger.log("INFO", "Multicastthread_run", "multicastthread thread finished")

class Shufflethread(threading.Thread):
    def __init__(self, server, pmap):
        threading.Thread.__init__(self)
        self.server = server
        self.pmap = pmap
        
    def run(self):
        size = 1024
        global logger
        logger.log("INFO", "Shufflethread_run", "Shufflethread thread started")
        while True:
            summ=0
            for k in self.pmap:
                summ = summ + self.pmap[k]
            if summ == len(self.pmap):
                break
            try:
                conn, addr = self.server.accept()
                peer = conn.getpeername()[0]
                ret = conn.recv(size)
                msize = int(ret.decode("utf8").strip('\0'))
                logger.log("INFO", "Shufflethread_run", "recv ack for shuffle from "+str(conn.getpeername()))
                self.pmap[peer] = 1
            except socket.error as msgg:
                logger.log("ERROR", "Shufflethread_run", "socket exception: "+str(msgg))
            except Exception as msgg:
                logger.log("ERROR", "Shufflethread_run", "other exception: "+str(msgg))
            else:
                conn.close()
        self.server.close()
        logger.log("INFO", "Shufflethread_run", "Shufflethread thread finished")

class Allgatherthread(threading.Thread):
    def __init__(self, path, metadict, algo):
        threading.Thread.__init__(self)
        self.path = path
        self.metadict = metadict
        self.algo = algo
        
    def run(self):
        global logger
        global ramdisk
        global localip
        logger.log("INFO", "Allgatherthread_run", "allgatherthread thread started")
        flist = []
        for fname in self.metadict:
            flist.append(fname)

        logger.log("INFO", "Allgatherthread_run", "begin flist: "+str(flist))
        length = len(flist)
        for i in range(length):
            f = flist.pop(0)
            if self.metadict[f]['location'] != localip:
                logger.log("INFO", "Shuffler_run", "appending: "+f)
                flist.append(f)
            elif self.metadict[f]['location'] == localip:
                break
        logger.log("INFO", "Allgatherthread_run", "targelist: "+str(flist))            
                
        for f in flist:
            location = self.metadict[f]['location']
            dsize = self.metadict[f]['st_size']
            md5=hashlib.md5()
            md5.update(f.encode())
            key = md5.hexdigest()
            tcpclient = TCPclient()
            ret = tcpclient.retrievefile(key, "COPY", location, dsize)
            ramdisk.data[key] = ret
                
        logger.log("INFO", "Allgatherthread_run", "allgather finished")
        
class Gatherthread(threading.Thread):
    def __init__(self, server, retdict, pmap):
        threading.Thread.__init__(self)
        self.server = server
        self.retdict = retdict
        self.pmap = pmap
        
    def run(self):
        global logger
        logger.log("INFO", "Gatherthread_run", "gatherthread thread started")

        while len(self.pmap.keys()) != sum(list(self.pmap.values())):
            try:
                conn, addr = self.server.accept()
                peer = conn.getpeername()[0]
                ret = conn.recv(10)
                dsize = int(ret.decode("utf8").strip('\0'))
                logger.log("INFO", "Gatherthread_run", "recv size: "+str(dsize)+" for gather from "+peer)
                conn.send(bytes('0', "utf8"))
                
                temp=b''
                tcp_big = TCP_big()
                temp=tcp_big.recv(conn, dsize)
                #buffersize = 1048576
                #while len(temp) < dsize:
                #    if len(temp)+buffersize < dsize:
                #        data = conn.recv(buffersize)
                #    else:
                #        data = conn.recv(dsize - len(temp))
                #    #data = conn.recv(dsize - len(temp))
                #    logger.log("INFO", "Gatherthread_run", "received "+str(len(data))+" bytes")
                #    temp = temp + data                
                self.retdict.update(pickle.loads(temp))
                conn.send(bytes('0', "utf8"))
                logger.log("INFO", "Gatherthread_run", "recv dict: "+str(len(self.retdict.keys()))+" for gather from "+peer)
                logger.log("INFO", "Gatherthread_run", "recv ack for gather from "+peer)
                
            except socket.error as msgg:
                logger.log("ERROR", "Gatherthread_run", "socket exception: "+str(msgg))
            except Exception as msgg:
                logger.log("ERROR", "Gatherthread_run", "other exception: "+str(msgg))
            else:
                self.pmap[peer] = 1
                #conn.close()
        self.server.close()
        
        logger.log("INFO", "Gatherthread_run", "gatherthread thread finished")

class Dumper(threading.Thread):
    def __init__(self, workerid, src, dst, flist):
        threading.Thread.__init__(self)
        self.id = workerid
        self.src = src #source dir
        self.dst = dst #destinaion dir
        self.flist = flist
        
    def run(self):
        global ramdisk
        global logger
        logger.log("INFO", "Dumper_run", "dumper thread started")
        #for each file in flist, create it
        for f in self.flist:
            logger.log("INFO", "dumper_run", "dump "+f+" from "+self.src+" to "+self.dst)
            srcf = os.path.join(self.src, f)
            basename = os.path.basename(self.src)
            if self.dst == '/':
                dstf = f
            else:
                dstf = self.dst+f

            fd = open(dstf, 'wb')
            md5=hashlib.md5()
            md5.update(srcf.encode())
            key = md5.hexdigest()

            fd.write(ramdisk.wcache[key])
            fd.close()
        logger.log("INFO", "Dumper_run", "dumperer thread finished")


class Loader(threading.Thread):
    def __init__(self, workerid, src, dst, flist):
        threading.Thread.__init__(self)
        self.id = workerid
        self.src = src #source dir
        self.dst = dst #destinaion dir
        self.flist = flist
        
    def run(self):
        global ramdisk
        global logger
        logger.log("INFO", "Loader_run", "loader thread started")
        #for each file in flist, create it
        for f in self.flist:
            logger.log("INFO", "loader_run", "load "+f+" from "+self.src+" to "+self.dst)
            srcf = os.path.join(self.src, f)
            basename = os.path.basename(self.src)
            if self.dst == '/':
                dstf = self.dst+basename+"/"+f
            else:
                dstf = self.dst+"/"+basename+"/"+f
            ramdisk.create(dstf, 33188)
            fd = open(srcf, 'rb')
            md5=hashlib.md5()
            md5.update(dstf.encode())
            key = md5.hexdigest()

            ramdisk.wcache[key] = fd.read()
            fd.close()
            ramdisk.wmeta[dstf]['st_size'] = len(ramdisk.wcache[key])
            ramdisk.local_release(dstf)
        logger.log("INFO", "Loader_run", "loader thread finished")

class Shuffleserver(threading.Thread):
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
            logger.log("INFO", "Shuffleserver_opensocket", "Open server socket")
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.host, self.port))
            self.server.listen(5)
        except socket.error as msg:
            logger.log("ERROR", "Shuffleserver_opensocket", msg)
            self.server = None

    def run(self):
        global logger
        global ramdisk
        #global executor
        global localip
        self.open_socket()
        
        while True:
            conn, addr = self.server.accept()
            try:
                data = conn.recv(self.size)
            except socket.error:
                logger.log("ERROR", "Shuffleserver_run", "socket exception when receiving message "+str(socket.error))
                break

            msg = data.decode("utf8").strip()
            logger.log("INFO", "Shuffleserver_run", "received: "+str(msg))
            el = msg.split('#')
            if el[1] == 'SHUFFLE_TRANSFER':
                global parentip
                #global shufflemap
                path = el[0]
                dsize = int(el[2])
                remoteip, remoteport = conn.getpeername()
                conn.send(bytes('0', 'utf8'))
                temp = b''
                tcp_big = TCP_big()
                temp = tcp_big.recv(conn, dsize)
                #while len(temp) < dsize:
                #    data = conn.recv(self.size)
                #    temp = temp + data 
                kvl = pickle.loads(temp)                

                ret = ramdisk.local_shuffle_transfer(path, kvl, remoteip)
                conn.send(bytes(str(0), "utf8"))
                #conn.close()
                if ret == 1:
                    logger.log('INFO', 'Shuffleserver_run', 'my local shuffle transfer are done')
                    break
        self.server.close()
        logger.log('INFO', 'Shuffleserver_run', 'shuffle server closed')


class Shuffler(threading.Thread):
    def __init__(self, workerid, parent, src, dst, metadict, algo):
        threading.Thread.__init__(self)
        self.id = workerid
        self.parent = parent #ip address to send ack
        self.src = src #source dir
        self.dst = dst #destinaion dir
        self.metadict = metadict
        self.algo = algo
        self.host = ''
        self.port = 50012
        self.server = None
        self.kvdict = dict()
        
    def run(self):
        global logger
        global localip
        global slist
        global ramdisk
        #for s in slist:
        #    self.kvdict[s] = []
        for s in slist:
            self.kvdict[s] = bytearray()
        logger.log("INFO", "Shuffler_run", "shuffler thread starts")

        path = []
        for k in self.metadict:
           if self.metadict[k]['location'] == localip:
                path.append(k)
                
        logger.log("INFO", "Shuffler_run", "files: "+str(path)+" are local")
        for p in path:
            md5 = hashlib.md5()
            md5.update(p.encode())
            key = md5.hexdigest()
            bdata = ramdisk.wcache[key]
            lines = bdata.split(b'\n')
            logger.log("INFO", "Shuffler_run", "lines: "+str(len(lines)))

            for line in lines[:len(lines)-1]:
                k, v = line.split(b'\t')
                #md5 = hashlib.md5()
                #md5.update(k)
                #rk = md5.hexdigest()
                #value = int(rk, 16)
                #use simple hash below
                #for test purpose only, switch back to normal
                #value = hash(int(k))
                value = hash(k)
                ip = slist[value%len(slist)]
                self.kvdict[ip].extend(line+b'\n')

        s = ''
        for k in self.kvdict:
            s = s+"key: "+k+" len: "+str(len(self.kvdict[k]))+", "
        logger.log("INFO", "Shuffler_run", "kvdict: "+s)

        tlist = list(slist)
        logger.log("INFO", "Shuffler_run", "begin targelist: "+str(tlist))
        length = len(tlist)
        for i in range(length):
            t = tlist.pop(0)
            if t != localip:
                logger.log("INFO", "Shuffler_run", "appending: "+t)
                tlist.append(t)
            elif t == localip:
                break
        logger.log("INFO", "Shuffler_run", "targelist: "+str(tlist))

        ramdisk.shuffledict[localip] = self.kvdict[localip]
        tcpclient = TCPclient()
        for target in tlist:
            ret = tcpclient.shuffle_transfer(self.dst, self.kvdict[target], target)
        logger.log("INFO", "Shuffler_run", "shuffler finished")
        #fd = codecs.open("/dev/shm/data", "w", encoding="utf8")
        #fd.write(data)
        #fd.close()
    
class TCPserver(threading.Thread):
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
        global ramdisk
        global tcpqueue
        #global executor
        global localip
        self.open_socket()
        
        while True:
            conn, addr = self.server.accept()
            try:
                data = conn.recv(self.size)
            except socket.error:
                logger.log("ERROR", "TCPserver_run", "socket exception when receiving message "+str(socket.error))
                break

            msg = data.decode("utf8").strip()
            logger.log("INFO", "TCPserver_run", "received: "+str(msg))
            tcpqueue.put([conn, msg], True, None)

class TCPworker(threading.Thread):
    def __init__(self, workerid):
        threading.Thread.__init__(self)
        self.id = workerid

    def run(self):
        global logger
        global tcpqueue
        global locaip
        global ramdisk
        
        while True:
            conn, msg = tcpqueue.get(True, None)
            el = msg.split('#')
            if el[1] == 'CREATE':
                filename = el[0]
                mode = int(el[2])
                remoteip, remoteport = conn.getpeername()
                ret = ramdisk.local_create(filename, mode, remoteip)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'RELEASE':
                filename = el[0]
                ret = ramdisk.local_release(filename)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'READ':
                filename = el[0]
                ret = ramdisk.files[filename]
                conn.send(pickle.dumps(ret))
                conn.close()
            elif el[1] == 'COPY':
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
                #sent = 0
                #while sent < length:
                #    sent_iter = conn.send(data[sent:])
                #    sent = sent + sent_iter
                #    logger.log("INFO", "TCPserver_run", "sent "+str(sent)+" bytes")
                #conn.send(data)
                conn.recv(1)
                conn.close()
            elif el[1] == 'GETATTR':
                filename = el[0]
                remoteip, remoteport = conn.getpeername()
                ret = ramdisk.local_getattr(filename, remoteip)
                conn.send(pickle.dumps(ret))
                conn.close()
            elif el[1] == 'GETXATTR':
                filename = el[0]
                ret = None
                if filename in ramdisk.files:
                    ret = ramdisk.files[filename].get('attrs', {})
                conn.send(pickle.dumps(ret))
            elif el[1] == 'CHMOD':
                filename = el[0]
                mode = int(el[2])
                ret = ramdisk.local_chmod(filename, mode)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'CHOWN':
                filename = el[0]
                uid = int(el[2])
                gid = int(el[3])
                ret = ramdisk.local_chown(filename, uid, gid)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'TRUNCATE':
                filename = el[0]
                length = int(el[2])
                ramdisk.local_truncate(filename, length)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
            elif el[1] == 'READDIR':
                path = el[0]
                ret = ramdisk.local_readdir(path, 0)
                conn.send(pickle.dumps(ret))
                conn.close()
            elif el[1] == 'MKDIR':
                path = el[0]
                mode = int(el[2])
                ramdisk.local_mkdir(path, mode)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
            elif el[1] == 'UNLINK':
                path = el[0]
                ret = ramdisk.files[path]
                conn.send(pickle.dumps(ret))
                conn.close()
                ramdisk.local_unlink(path)
            elif el[1] == 'DELETE':
                path = el[0]
                ramdisk.local_delete(path)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
            elif el[1] == 'SYMLINK':
                path = el[0]
                source = el[2]
                remoteip, remoteport = conn.getpeername()
                ramdisk.local_symlink(path, source, remoteip)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
            elif el[1] == 'READLINK':
                path = el[0]
                data = ramdisk.local_readlink(path)
                conn.send(bytes(data, "utf8"))
                conn.close()
            elif el[1] == 'RENAME':
                old = el[0]
                new = el[2]
                data = ramdisk.local_rename(old, new)
                conn.send(bytes(str(data), "utf8"))
                conn.close()
            elif el[1] == 'INSERTMETA':
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
            elif el[1] == 'APPENDDATA':
                path = el[0]
                msize = int(el[2])
                offset = int(el[3])
                data = conn.recv(msize)
                content = pickle.loads(data)
                data = ramdisk.local_append(path, offset, content)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
            elif el[1] == 'UPDATE':
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
            self.server.bind((self.host, self.port))
            self.server.listen(5)
        except socket.error as msg:
            logger.log("ERROR", "Interfaceserver_opensocket", msg)
            self.server = None

    def run(self):
        global logger
        global ramdisk
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
                ret = ramdisk.multicast(path, algo)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'GATHER':
                path = el[0]
                algo = el[2]
                ret, retdict = ramdisk.gather(path, algo)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'ALLGATHER':
                path = el[0]
                algo = el[2]
                ret = ramdisk.allgather(path, algo)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'SCATTER':
                path = el[0]
                algo = el[2]
                ret = ramdisk.scatter(path, algo)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'SHUFFLE':
                path = el[0]
                algo = el[2]
                dst = el[3]
                ret = ramdisk.shuffle(path, algo, dst)
                conn.send(bytes(str(0), "utf8"))
                conn.close()
            elif el[1] == 'QUEUE':
                desc = el[0]
                task = Task(desc)
                ret = executor.push(task)
                conn.send(bytes(str(ret), "utf8"))
                conn.close()
            elif el[1] == 'EXECUTE':
                ret = ramdisk.execute()
                #ret = executor.execute()
                #ret = executor.wait()
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

class Task():
    def __init__(self, desc):
        self.queuetime = time()
        self.starttime = None
        self.endtime = None
        self.desc = desc
        self.ret = None
        tempkey = self.desc+str(self.queuetime)
        md5 = hashlib.md5()
        md5.update(tempkey.encode())
        self.key = md5.hexdigest()
        
class Executor(threading.Thread):
    def __init__(self, tlist):
        threading.Thread.__init__(self)
        self.queue = []
        self.fqueue = []
        self.smap = {}
        self.readyqueue = queue.Queue()
        self.fmap = {} #key-file, value-task
        self.tlist = tlist
        for t in tlist:
            self.readyqueue.put(t, True, None)
    #def push(self, task):
    #    self.queue.append(task)
    #    self.smap[task.key] = -1
    #    return 0

    #def execute(self):
    #    global logger
    #    global slist
        #read task from file
    #    fd = open('/tmp/task.txt', 'r')
    #    while True:
    #        line = fd.readline()
    #        if not line:
    #            break
    #        task = Task(line.strip('\n'))
    #        ret = self.push(task)
    #    fd.close()
    #    #self.queue.reverse()
    #    logger.log("INFO", "Executor", "loaded "+str(len(self.queue))+" tasks")
        
    #    alltcpclient = ALLTCPclient()
    #    alltcpclient.dispatch(slist, self.queue)
    #    for i in range(len(self.queue)):
    #        t = self.queue.pop()
    #        self.readyqueue.put(t, True, None)
    #    #self.readyqueue = queue.Queue(self.queue)
    #    return 0

    #def wait(self):
    #    global logger
    #    alltcpclient = ALLTCPclient()
    #    retdict = alltcpclient.wait(slist)
    #    logger.log('INFO', 'executor_wait', 'retdict: '+str(retdict))
    #    self.smap.update(retdict)
    #    while True:
    #        values = list(self.smap.values())
    #        if sum(values) == 0:
    #            break
    #        else:
    #            sleep(1)
    #            logger.log('INFO', 'executor_wait', 'sum: '+str(values))
    #    return 0

    #def state(self):
    #    alltcpclient = ALLTCPclient()
    #    retq = alltcpclient.state(slist)
    #    for t in self.fqueue:
    #        retq.append(t)
    #    return retq
    
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
            #code below implements asynchronous file access
            #retl = subprocess.getstatusoutput(task.desc)
            #ret = retl[0]
            #msg = retl[1]
            #if ret == 256:
            #    logger.log('INFO', 'Executor_run', 'execution failed: '+msg)
            #    el = msg.split(':')
            #    if el[2].strip(' ') == "No such file or directory":
            #        filename = el[1].strip(' ').strip('\'')
            #        path = filename[filename.find(mountpoint)+len(mountpoint):]
            #        logger.log('INFO', 'Executor_run', "file: "+path+" does not exist")
            #        self.fmap[path] = task
            #        continue
            task.endtime = time()
            task.ret = ret
            self.smap[task.key] = 0
            self.fqueue.append(task)
            logger.log('INFO', 'Executor_run', 'finishing task: '+task.desc)
        logger.log('INFO', 'Executor_run', 'all tasks finished')

class TCP_big():
    def __init__(self):
        self.bufsize = 1048576
    
    def send(self, sock, data, length):
        global logger
        sent = 0
        while sent < length:
            if length - sent > self.bufsize:
                sent_iter = sock.send(data[sent:sent+self.bufsize])
            else:
                sent_iter = sock.send(data[sent:])
            sent = sent + sent_iter
            logger.log("INFO", "TCP_big_send", "sent "+str(sent)+" bytes")
        logger.log("INFO", "TCP_big_send", "sent finished")
    def recv(self, sock, length):
        data = b''
        rect = 0
        while rect < length:
            if length - rect > self.bufsize:
                temp = sock.recv(self.bufsize)
            else:
                temp = sock.recv(length-rect)
            rect = rect + len(temp)
            data = data + temp
            logger.log("INFO", "TCP_big_recv", "receive "+str(rect)+" bytes")        
        logger.log("INFO", "TCP_big_recv", "recv finished")
        return data

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
    
    #global shufflemap
    #shufflemap = None
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
    
    global ramdisk
    ramdisk=RAMdisk()

    #global executor
    #executor=Executor()
    #while not executor.is_alive():
    #    executor.start()
    global tcpqueue
    tcpqueue = queue.Queue()
    tcpserver = TCPserver('TCPserver', 55000)
    while not tcpserver.is_alive():
        tcpserver.start()

    tcpworker = TCPworker('TCPworker')
    while not tcpworker.is_alive():
        tcpworker.start()

    interfaceserver = Interfaceserver('Interfaceserver', 55010)
    while not interfaceserver.is_alive():
        interfaceserver.start()
        
    alltcpserver = ALLTCPserver('ALLTCPserver', 55001, ramdisk)
    while not alltcpserver.is_alive():
        alltcpserver.start()

    fuse = FUSE(ramdisk, argv[1], foreground=True, big_writes=True, direct_io=True)

