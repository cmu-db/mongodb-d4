# mongodb-d4

**D4** is an automated tool for a generating **d**istributed **d**ocument **d**atabase **d**esigns for applications
running on MongoDB. This tool specifically targets applications running highly concurrent workloads, and thus its
designs are tailored to the unique properties of large-scale, Web-based applications. It can also be used to assist
in porting MySQL-based applications to MongoDB.

Using a sample workload trace from a either a document-oriented or relational database application, **D4** will compute
the best a database design that optimizes the throughput and latency of a document DBMS. The three design elements that
D4 can select for an application are:

+ Sharding Keys
+ Indexes
+ Collection (De)normalization

For More Information: http://database.cs.brown.edu/projects/mongodb/

## Dependencies
+ python-pymongo
+ python-yaml

## Authors
+ [Andy Pavlo](http://www.cs.brown.edu/~pavlo)
+ [Christopher Keith](http://www.linkedin.com/pub/christopher-keith/38/882/81a)
+ [Emanuel Buzek](http://www.linkedin.com/pub/emanuel-buzek/2/655/b04)


## Acknowledgements
This work is supported (in part) by an [Amazon AWS Research Grant](http://aws.amazon.com/education/).
Additional assistance was also provided by [10gen, Inc.](http://10gen.com)