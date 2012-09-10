
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

