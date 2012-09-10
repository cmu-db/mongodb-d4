# Source Code Hierarchy

The following is a rough outline of the source code for **D4** as of September 2012.

## catalog
This directory contains the *Collection* catalog object. This object contains the metadata about a high-level single
collection in the database. The most important element of this object is the *fields* document. This
contains a list of objects that represent a unique element of the parent collection. Note that each field element
is recursive: it also contains a *fields* element for other fields that are embedded inside of that field.
This catalog information is generated from the sample database provided by the user.
Each *Collection* object is linked to **D4**'s underlying workspace database using MongoKit.

## costmodel
The costmodel directory contains the code that will estimate the performance cost of executing a sample workload on
a given database for a particular design. The *CostModel* class is the main object to instantiate and
use to calculate the estimated cost. The best possible design will have a cost of zero, while the worst cost will
have a cost of one.
There are different sub-components used by the costmodel calculator: (1) *disk*, (2) *network*, and (3) *skew*. The
importance of these different components in the final design can be controlled by changing their weighting
parameters in the config file.

+   The *disk* component estimates the amount of disk I/O that the database will perform to execute a workload for
    a given design.
+   The *network* component estimates the number of network messages that will the database will need to send
    between nodes in order to execute queries. Note that these cost should also include the network messages sent
    from the client to the `mongos` process, since we can denormalize collections to generate a smaller number of
    requests.
+   The *skew* component estimates how uniformly the workload is execute amongst the different nodes in the
    cluster. A design that creates heavily skewed workload where all queries go to a single machine (while all
    others are idle) will have a cost of one, while a design that spreads the queries evenly amongst the
    nodes will have a cost of zero.
    
Each of the *AbstractCostComponent* implementations will take in a *State* object that contains all of the data
structures needed to compute their cost estimates. The *State* object contains some rudimentary caching mechanisms
so that we do not need to completely reevaluate the entire workload every time the search algorithm changes an
aspect of the current design.
    
## inputs
This contains code for loading in sample workloads and databases into **D4**'s internal catalog database. There are
currently two different types of inputs that **D4** can ingest:

+   The *mongodb* input converter takes in a `mongosniff` trace file and extract the workload queries.
    The *Reconstructor* will recreate the documents in the original database. This necessary in order to extract
    the schema *Catalog* objects.
    **Important:** The workload traces must come from our customized version of `mongosniff` because the one
    shipped by 10gen does not dump the entire contents of queries.

+   The *mysql* input converter takes in a `mysqldump` file and corresponding a
    [general log](http://dev.mysql.com/doc/refman/5.1/en/query-log.html) trace database. It will first extract the
    schema catalog from MySQL's internal
    [INFORMATION_SCHEMA](http://dev.mysql.com/doc/refman/5.1/en/information-schema.html) tables and populate the
    *Collection* metadata catalog. It will then convert all of the queries from the trace into equivalent MongoDB
    queries. The queries within a transaction will be combined into a single `Session` object, and each of
    the transaction's queries will be converted into a `Operation` object that is nested inside of the `Session`.
    Queries that join multiple tables will be converted into multiple `Operation` (one for each table
    referenced) that are linked together by a unique `query_group` identifier.
    Note also that not all SQL features are supported for this conversion (e.g., functions, aggregates).
    This conversion process is not meant to be completely accurate, but rather is way to approximate the queries
    needed to rewrite a MySQL-based application to use MongoDB instead.
    
## sanitizer
The `Sanitizer` is a standalone program used to anonymize the output produced by `mongosniff`.
It will replace any string value in either the input query or the output result with a salted md5 hash that is
prefixed with the length of the original string.
This is not required by **D4** but is useful for getting sample workloads from customers' production systems.

## search
This directory contains all of the code to execute the search algorithm for finding the best design.
The *InitialDesigner* is a heuristic-based algorithm that selects the different design options for the target
database based on the most frequently accessed attributes in each collection. This provides a quick upper bound
on the solution space.
The *LNSearch* class is **D4**'s large-neighborhood search algorithm implementation that explores the different
solutions for the target database. It will invoke the branch-and-bound implementation (*BBSearch*) in multiple
rounds and use the *CostModel* to guide it to an optimal design.

## util
Utility code for various aspects of the system.
+   *config*: Configuration file schema use by **D4**.
+   *constants*: Global internal configuration parameters.
   
## workload
The workload directory contains the other MongoKit-backed object used in **D4**. The `Session` object contains
an ordered list of `Operations` invoked by a single client thread to complete some action (e.g., a `Session`
could be all of the queries executed to retrieve the data needed to serve a page request). This `operations` list is
sorted by the order that the client executed them (we assume that the client is single-threaded). 
    