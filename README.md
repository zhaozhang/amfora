Amfora
======

Amfora is a POSIX-compatible parallel scripting framework that lets users run existing programs in parallel with data stored in RAM on distributed platforms: e.g. clouds, clusters, supercomputers. 

Amphora features:

1. Amphora lets the user select the scripting language they will use as the programming interface. 

2. Amphora can execute LINUX command lines in parallel and cache the data in RAM. 

3. Amphora provides a shared file system across the RAM on multiple nodes. 

4. Amphora lets users collectively manage data movement at large scale.


Author: Zhao Zhang, Daniel S. Katz 

Email: zhaozhang@uchicago.edu， dsk@ci.uchicago.edu

Project Website: https://github.com/zhaozhang/amfora/wiki

Project Hosting Page: https://github.com/zhaozhang/amphora

Project Mailing List: coming soon

#Building
The Amphora framework requires the following sotware and package:

Python 3.1 or later

fuse 2.8.5 or later

fusepy module which provides the python bindings (https://github.com/terencehonles/fusepy). The fusepy module is included in the package.

#Installation
