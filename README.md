AMFORA
======

AMFORA is a POSIX-compatible parallel scripting framework that lets users run existing programs in parallel with data stored in RAM on distributed platforms: e.g. clouds, clusters, supercomputers. 

AMFORA features:

1. AMFORA lets the user select the scripting language they will use as the programming interface

2. AMFORA can execute LINUX command lines in parallel and cache the data in RAM

3. AMFORA provides a shared file system across the RAM on multiple nodes

4. AMFORA lets users collectively manage data movement at large scale

5. AMFORA implements the multi-read single-write consistency

Author: Zhao Zhang, Daniel S. Katz 

Email: zhaozhang@uchicago.edu， dsk@ci.uchicago.edu

Project Website: http://www.amfora-project.org

Project Hosting Page: https://github.com/zhaozhang/amphora

Project Documentation Page: https://github.com/zhaozhang/amfora/wiki

Project Mailing List: coming soon

#Building
The Amphora framework requires the following sotware and package:

Python 3.1 or later

fuse 2.8.5 or later

fusepy module which provides the python bindings (https://github.com/terencehonles/fusepy). The fusepy module is included in the package.

#Installation
Please refer to [Quick Start](https://github.com/zhaozhang/amfora/wiki/Amfora-Quickstart)
