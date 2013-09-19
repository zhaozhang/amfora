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
        pass
    def allgather(self, path, algo):
        pass
    def allgather_old(self, path, algo):
        pass
    def gather(self, path, algo):
        pass
    def scatter(self, path, algo):
        pass
    def shuffle(self, path, algo, dst):
        pass
    def load(self, src, dst):
        pass
    def dump(self, src, dst):
        pass
    def execute(self):
        pass

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
            global slist
            global localip
            md5=hashlib.md5()
            md5.update(path.encode())
            key = md5.hexdigest()
            value = int(key, 16)
            ip = slist[value%len(slist)]
            if ip == localip:
                raise FuseOSError(ENOENT)
            logger.log("INFO", "getattr", " cache miss")
            tcpclient = TCPclient()
            data=tcpclient.sendmsg(path, "GETATTR")
            ret = pickle.loads(data)
            if not ret:
                logger.log("INFO", "getattr", path+" is not on remote host")
                self.create(path, 0o755)
                raise FuseOSError(ENOENT)
            else:
                logger.log("INFO", "getattr", path+" is on remote host")
                return ret

    def getxattr(self, path, name, position=0):
        global logger
        logger.log("INFO", "getxattr", path+", "+name)
        #retrieve metadata from remote server 
        if path in self.files:
            attrs = self.files[path].get('attrs', {})
            try:
                return attrs[name]
            except KeyError:
                return b''
        elif path in self.wmeta:
            attrs = self.wmeta[path].get('attrs', {})
            try:
                return attrs[name]
            except KeyError:
                return b''
        else:
            tcpclient = TCPclient()
            data=tcpclient.sendmsg(path, "GETXATTR")
            attrs = pickle.loads(data)
        
            try:
                return attrs[name]
            except KeyError:
                return b''       # Should return ENOATTR

    def listxattr(self, path):
        global logger
        logger.log("INFO", "listxattr", path)
        if path in self.files:
            attrs = self.files[name].get('attrs', {})
            return attrs.keys()
        elif path in self.wmeta:
            attrs = self.wmeta[name].get('attrs', {})
            return attrs.keys()
        else:
            #retrieve metadata from remote server
            tcpclient = TCPclient()
            data=tcpclient.sendmsg(path, "GETXATTR")
            attrs = pickle.loads(data)
            return attrs.keys()

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
        global slist
        logger.log("INFO", "symlink", "target: "+target+", source:"+source)
        md5=hashlib.md5()
        md5.update(target.encode())
        key = md5.hexdigest()
        value = int(key, 16)
        ip = slist[value%len(slist)]
        if ip == localip:
            local_symlink(target, source, ip)
        else:    
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
        if path not in self.files:
            self.create(path, 0o755)
            self.wmeta[path]['st_atime'] = atime
            self.wmeta[path]['st_mtime'] = mtime
        else:
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
            self.wmeta[path]['st_size'] = self.wmeta[path]['st_size'] + len(data)
        elif offset > 0:
            logger.log("INFO", "write_append", path+", length: "+str(len(data))+", offset: "+str(offset))
            if key in self.data:
                self.data.pop(key)
            #append data to where the data is
            tcpclient = TCPclient()
            ret=tcpclient.appenddata(path, offset, data)
            return len(data)

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
            ret = s.recv(1)
            s.send(pickle.dumps(meta))
            ret=s.recv(dsize)
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
                pass
            elif el[1] == 'ALLGATHER':
                pass
            elif el[1] == 'GATHER':
                pass
            elif el[1] == 'SCATTER':
                pass
            elif el[1] == 'LOAD':
                pass
            elif el[1] == 'DUMP':
                pass
            elif el[1] == 'SHUFFLE':
                pass
            elif el[1] == 'DISPATCH':
                pass
            elif el[1] == 'STATE':
                pass

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
        pass
    def allgather(self, path, slist, algo, flist):
        pass
    def gather(self, path, slist, algo):
        pass
    def scatter(self, path, slist, algo, mdict, ddict):
        pass
    def shuffle(self, path, dst, slist, metadict, algo):
        pass
    def dispatch(self, slist, tlist):
        pass
    def state(self, slist):
        pass
    def load(self, src, slist, flist, dst, algo):
        pass
    def dump(self, src, slist, fdict, dst, algo):
        pass

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

class Misc():
    def __init__(self):
        pass
    def findserver(self, fname):
        global slist
        value = hash(fname)
        return slist[value%(len(slist))]

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

