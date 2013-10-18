#!/bin/bash

mkdir /tmp/amfora/projdir /tmp/amfora/diffdir /tmp/amfora/statdir /tmp/amfora/corrdir /tmp/amfora/final

cp examples/Montage/m101/template.hdr /tmp/amfora/
src/amc.py multicast /tmp/amfora/template.hdr

cp -r examples/Montage/m101/rawdir /tmp/amfora/
mImgtbl /tmp/amfora/rawdir /tmp/amfora/images-rawdir.tbl
src/amc.py scatter /tmp/amfora/rawdir

for file in `ls /tmp/amfora/rawdir` 
do
   src/amc.py queue "/tmp/amfora/bin/mProjectPP /tmp/amfora/rawdir/${file} /tmp/amfora/projdir/hdu0_${file} /tmp/amfora/template.hdr" 
done
src/amc.py execute

mImgtbl /tmp/amfora/projdir /tmp/amfora/images.tbl

mOverlaps /tmp/amfora/images.tbl /tmp/amfora/diffs.tbl

cat /tmp/amfora/diffs.tbl | awk '{if(NR>2) printf("export PATH=/tmp/amfora/bin:${PATH}; mDiffFit -n /tmp/amfora/projdir/%s /tmp/amfora/projdir/%s /tmp/amfora/diffdir/diff.%0.6d.%0.6d.fits /tmp/amfora/template.hdr > /tmp/amfora/statdir/stats-diff.%0.6d.%0.6d.fits\n", $3, $4, $1, $2, $1, $2)}' > /tmp/amfora-task.txt

src/amc.py execute

for file in `ls /tmp/amfora/statdir/`; do   echo /tmp/amfora/statdir/${file} ;   cat /tmp/amfora/statdir/${file} ;done > /tmp/amfora/stats.tmp

examples/Montage/bin/format-fits.py /tmp/amfora/stats.tmp /tmp/amfora/fits.tbl

mBgModel /tmp/amfora/images.tbl /tmp/amfora/fits.tbl /tmp/amfora/corrections.tbl

examples/Montage/bin/gen-back-task.py /tmp/amfora/images.tbl /tmp/amfora/corrections.tbl > /tmp/amfora-task.txt
src/amc.py execute

mImgtbl /tmp/amfora/corrdir /tmp/amfora/corr-images.tbl

mAdd -n -p /tmp/amfora/corrdir /tmp/amfora/corr-images.tbl /tmp/amfora/template.hdr /tmp/amfora/final/m101_corrected.fits

mJPEG -gray /tmp/amfora/final/m101_corrected.fits 0s max gaussian-log -out /tmp/amfora/final/m101_corrected.jpg
