# KeyValueStore
This is the repo for the Cloud Computing Concept (both part 1 and part 2) offered by UIUC on coursera. 

MP1 contains membership protocol implementation of both SWIM and ALL TO ALL broadcasting. MP2 contains a simple and clean implementation of a key value database implementation that supports all CRUD operations. 

To run the code: 
```
$ make clean
$ make
$ ./Application ./testcases/create.conf
$ ./Application ./testcases/delete.conf
$ ./Application ./testcases/read.conf
$ ./Application ./testcases/update.conf
```
or equivalently
```
$ ./run.sh
```
that will run all four tests described above, the setting of each test is in ./testcases/