AMFORA
======

AMFORA is a POSIX-compatible parallel scripting framework that lets users run existing programs in parallel with data stored in RAM on distributed platforms: e.g. clouds, clusters, supercomputers. 

AMFORA runs on: IBM Blue Gene/P with ZeptoOS, UChicago CS Cluster with Ubuntu 12.04, AMAZON EC2 with Ubuntu 12.04, Google Compute Engine with Debian-7-wheezy.

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

Project Mailing List: https://groups.google.com/forum/#!forum/amfora-user

#Building
The Amphora framework requires the following sotware and package:

Python 3.1 or later

fuse 2.8.5 or later

fusepy module which provides the python bindings (https://github.com/terencehonles/fusepy). The fusepy module is included in the package.

#Installation
If you want to run AMFORA on a regular cluster with fuse interface, please refer to [Quick Start](https://github.com/zhaozhang/amfora/wiki/Amfora-Quickstart)

If you want to run AMFORA on Amazon EC2, please refer to our [Amazon EC2 Recipe](https://github.com/zhaozhang/amfora/wiki/Amfora-Amazon-EC2-Recipe)

If you want to run AMFORA on Google Compute Engine, please refer to our [Google Compute Engine Recipe](https://github.com/zhaozhang/amfora/wiki/Amfora-Google-Compute-Engine-Recipe)

If you want to run AMFORA on a Raspberry Pi cluster, please follow the [Quick Start](https://github.com/zhaozhang/amfora/wiki/Amfora-Quickstart) 

