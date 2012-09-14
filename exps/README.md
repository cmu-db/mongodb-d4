# MongoDB Benchmark Framework

This framework is able to run different benchmarks using MongoDB. It was originally based 
on my TPC-C benchmark framework that I used in my NoSQL course in spring 2011. It was then
forked by one of my students in the summer of 2011. I then grabbed his changes and modified
it further to support the different types of experiments that we will need for this work.

TLDR:
This code is based on: https://github.com/yanglu/BigBenchmark
which was originally based on: https://github.com/apavlo/py-tpcc


## Dependencies:
+ python-execnet

## Example Usage

1. Create a configuration file for the benchmark that you are going to run.
   For this example, we will use the `blog` benchmark.
   
        ./benchmark.py --print-config blog > blog.config

   Modify the configuration file to change the parameters according to your environment setup.
   
2. Load in the benchmark database into MongoDB. The `--no-execute` option will prevent
   the framework from executing the workload portion of the benchmark, while the `--reset` option
   will clear out the contents of the database if it already exists.

        ./benchmark.py --config=blog.config --no-execute --reset blog
        
3. Now execute the workload driver to perform the experiment. The final throughput results 
   will be printed at the end. Note here that the `--no-load` option will prevent the framework
   from repeating the loading step.
   
        ./benchmark.py --config=blog.config --no-load blog
   
        