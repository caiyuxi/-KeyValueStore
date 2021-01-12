#**********************
#*
#* Progam Name: MP1. Membership Protocol.
#*
#* Current file: run.sh
#* About this file: Modified shell script to run tests.
#* 
#***********************
#!/bin/sh
rm -rf grade-dir # Make sure grade-dir is clean before starting
rm -f dbg.*.log

make clean > /dev/null 2>&1
make > /dev/null 2>&1 

echo "CREATE test"
./Application testcases/create.conf > /dev/null 2>&1
cp dbg.log ../../dbg.c.log
echo "DELETE test" 
./Application testcases/delete.conf > /dev/null 2>&1
cp dbg.log ../../dbg.d.log
echo "READ test"
./Application testcases/read.conf > /dev/null 2>&1
cp dbg.log ../../dbg.r.log
echo "UPDATE test"
./Application testcases/update.conf > /dev/null 2>&1
cp dbg.log ../../dbg.u.log

