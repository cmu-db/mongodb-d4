# mongodb-d4

**D4** is an automated tool for a generating **d**istributed **d**ocument **d**atabase **d**esigns for applications
running on MongoDB. This tool specifically targets applications running highly concurrent workloads, and thus its
designs are tailored to the unique properties of large-scale, Web-based applications. It can also be used to assist
in porting MySQL-based applications to MongoDB.

Using a sample workload trace from a either a document-oriented or relational database application, **D4** will compute
the best a database design that optimizes the throughput and latency of a document DBMS. The three design elements that
D4 can select are:

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
This work is supported (in part) by an [Amazon AWS Research Grant](http://aws.amazon.com/education/).
Additional
assistance was also provided by [10gen, Inc.](http://10gen.com)