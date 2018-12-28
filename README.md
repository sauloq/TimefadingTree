# TimefadingTree
Implementation of a timefading algorithm to mini streams.
As python cannot handler deep recursive methods, as well its multiprocessing library has many flews that raise segfault. The code was not tested against bigger datasets once it was reaching the limit of the python recursive heap.

In order to run the code use python 3 passing the following parameters:

python3 handler.py -d <database> -p <preMinSup> -m <minSup> -s <sampleSize> -b <batchSize> -t <True/False>
ex:

python3 handler.py -d ../T10I4D1000K.data -p 0.005 -m 0.02 -s 10000 -b 50 -t True