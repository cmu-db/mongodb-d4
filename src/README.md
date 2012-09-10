
## Setup

1. Create a default configuration file that you will use for your application:

        d4.py --print-config > application.config
        
2. Edit the settings in this configuration file according to your local environment.


## MongoDB Example

1. Execute [mongosniff](http://www.mongodb.org/display/DOCS/mongosniff) on your application server to collect
   a workload trace of operations executed on the MongoDB server. You can pipe this into a file for later processing.

        mongosniff --source NET lo | gzip --best > sniff.out.gz

2. Load this mongosniff workload trace from into **D4**'s internal catalog

        gunzip -c sniff.out.gz | ./d4.py --config=application.config --reset --no-search
            
   The *--reset* flag will erase all of the metadata that may exist in the catalog database in target MongoDB.
   This does not modify your application's database.
   The *--no-search* flag will cause **D4** to halt the program immediately after processing the workload trace.
        
TODO: Need to discuss how to use an existing MongoDB design in **D4** to check whether there is better configuration.

TODO: Need to discuss how to enable the debug log and where to report issues.
        
## MySQL Example
*To be written*

## Source Code Hierarchy

The following is a rough outline of the source code for **D4** as of September 2012. This may have changed since then.

+ **catalog**  
    This directory contains the *Collection* catalog object. This object contains the metadata about a high-level single
    collection in the database. The most important element of this object is the *fields* document. This
    contains a list of objects that represent a unique element of the parent collection. Note that each field element
    is recursive: it also contains a *fields* element for other fields that are embedded inside of that field.

+ **costmodel**  
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
    
+ parser  
    
    
+ sanitizer  

+ search  

+ util  

+ workload  
