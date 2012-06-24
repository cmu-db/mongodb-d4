# mongodb-d4

**D4** is an automated tool for generating distributed document database designs, called
that is tailored to the unique properties of large-scale, Web-based applications 
running on MongoDB. Using a sample workload trace from a either a document-oriented 
or relational database application, **D4** computes a database design that optimizes the
throughput and latency of a document DBMS.

The three design elements that D4 can select are:

+ Sharding Keys
+ Indexes
+ (De)normalization

For More Information: http://database.cs.brown.edu/projects/mongodb/

## Dependencies
+ python-pymongo
+ python-yaml

## Authors
+ Andy Pavlo
+ Emanuel Buzek
+ Christopher Keith

## Acknowledgements
This work is supported (in part) by an [Amazon AWS Research Grant](http://aws.amazon.com/education/). Additional assistance was also provided by [10gen, Inc.](http://10gen.com)