# camunda-engine-cassandra

Cassandra Persistence for Camunda (Community Extension)

## Key design aspects

Camunda utilises database persistence for its core engine functionality in a very specific pattern:
-	Execute business processes in memory until there is some asynchronous step (such as message task or user task)
-	Store the process state in database using a single transaction
-	When a task is triggered - retrieve process instance data 
-	When a task is completed - update process instance data in the database. It is possible that some other thread or node has also executed this task and has already updated the database. To prevent data corruption in this scenario, Camunda employs optimistic locking. Currently optimistic locking is applied on per-entity level, e.g. each entity is locked separately (each task, execution, variable etc.)
 
The use of Cassandra persistence for Camunda promises to provide both greater scalability as well as fault tolerance for large deployments. Camunda support for Cassandra has to consider the key design issues listed below. 

The terminology that's currently in use can be slightly confusing. Cassandra is basically a hash table - it provides fast access to rows by keys. Datastax, the primary vendor supporting Cassandra, refers to the keys as "partition keys" and the rows as "partitions". This terminology is used here for consistency, but it's very useful to understand that a “partition” is actually a database row.  

### Query access and indexing. 

Without specific indexes, data in Cassandra can only be accessed via by partition keys (like a  hashtable). Conventional advice is that the data needs to be organised around queries and stored by the query parameters. This works for really simple use cases, however data models of moderate to high complexity will require additional indexing. The native indexing functionality is very limited in scalability. Datastax does not recommend using native indexing in a few cases, including high cardinality columns (e.g. columns with a lot of distinct values). The community at large seems to agree with this and is busy developing custom indexing solutions. The alternative to custom indexing is duplicating data (for each index create a table storing all data by that index). That works fine for smaller partitions but can quickly get out of hand with larger partitions or more indices.

Second important aspect of Cassandra queries is ordering. Every partition in Cassandra is ordered according to the native ordering of the cell names. There is no other ordering support at all. This means that range queries or queries where the order is important have to either use the ordering of the data as it is stored in the data model or use custom row indexes.

Currently, our Cassandra implementation for Camunda persists all data related to single process instance in one partition (row). The key is the process id. 

We do not envisage support for complex user-facing queries through Cassandra directly. The complex task and history searches will likely be implemented using an external search provider, such as Lucene or ElasticSearch. It is outside of scope for this project.

We do want to support the core operations and basic searches that support most of the common requirements in Camunda. Given the number of search criteria required for even basic operation and the size of the process instance, it is impractical to try to duplicate the data; therefore we need an indexing solution. I have provided an initial implementation of custom indexing that fully supports core process operations and message correlation, however this approach has a number of limitations and I'm actively researching other indexing options. 

It looks like Camunda core operation does not require any ordered or range queries and at this stage we have no plans to develop any ordered indexes. 

### Data integrity in case of concurrent access (locking) 

Cassandra does not provide any locking in conventional sense at all. There is however built-in support for optimistic locking using compare-and-set (CAS) statements ("IF NOT EXISTS" and "IF"). It is important to understand that this locking does not work across multiple partitions. This has very significant implications on indexing because custom indexes use separate tables from the data, so this means that indexing statements have to use separate batches from the main data updates. This in turn breaks the data integrity (see atomicity below).

Optimistic locking is important to Camunda to ensure that the process instances are not concurrently updated by different actors. We have decided to use CAS statements to support locking and design the batch structure around it. There is an alternative option to use an external lock provider such as ZooKeeper. While it is far more flexible, it is also a more complicated solution. As locking can always be externalised later we will first explore how far we can get with pure Cassandra implementation..

### Transaction support (atomicity). 

This is about ensuring that the unit of work is either written to the database in its entirety or not at all. Cassandra provides atomic batches to support this. The batches in general can span multiple tables and partitions, however there are some relevant points to consider:

 - Even though batches in general do not provide any guarantees outside of atomicity, batches that only update 1 partition do guarantee isolation (other client nodes or threads will not be able to see partial updates)

 - Batches that include CAS statements are not able to use client-supplied timestamp. To understand timestamps it helps to think that Cassandra nodes are writing data to a set of separate logs that get sorted and merged based on the timestamp for each statement. (This is not what actually happens, but it helps to understand the end result). Usually the timestamp is generated by the server at the time it receives the request, however it is possible for the client to specify the timestamp as well. This can be very useful when there is a possibility of concurrent updates to data.    

In Camunda, transactions are usually limited to one process. In our implementation we use 2 batches for each process due to the optimistic locking restrictions, one for indexing and one for the main data. The main data batch is applied first. 

There are some issues with this approach:
 - In case of client failure the indexing batch might be lost
 - In case of concurrent access to the same process it is possible that the indexing is applied out of order and earlier index batch overrides a later index batch.

These issues are not acceptable in any production system. They are caused by using optimistic locking and client side indexes together. To address these issues we can either 
 - choose not to use Cassandra optimistic locking and use an external provider instead
 - choose to use Cassandra native indexing and accept the scalability limitations
 - develop our own solution to circumvent this. The usual approach is over-indexing,  e.g. creating indexes before the main data is updated, deleting indexes after the main data is deleted and filtering data on read. There are some issues when the same index value is used by multiple processes, however this can be resolved if we add a timestamp to each index entry. This can get quite complicated, so it is not a very desirable option.
 - there is also a "CUSTOM INDEX" keyword in CQL. It is very new, poorly documented and it is unclear if it allows us to achieve what we want. I'll be doing more digging around it. 

In some Camunda engine scenarios, such as inter-process communications, it is possible to have more than one process involved in a single transaction. It is also possible to have updates to other entities inside the transaction (create a user for instance). While these scenarios are supported functionally, we are not considering data integrity for these scenarios for now.

### Delete-heavy workload.

When something is deleted in Cassandra, it does not delete the relevant cells immediately, it creates tombstones instead. These tombstones can remain in the system for days and interfere with the database performance. The worst scenario is when there are masses (hundreds of thousands or more) of tombstones in a single partition. This can happen when many cells are deleted at the same time or when there is a lot of create/delete operations in the same partition. Just having these tombstones in the database is normally not very problematic (they will get cleaned up eventually), however querying partitions with lots of tombstones will cause performance issues. With large volumes of tombstones, Cassandra will start failing queries and performance of the nodes serving the query will suffer (in some cases drastically). 

Normal Camunda operation involves creating process instances, executing those processes and then deleting them. Various process components are created and deleted during execution. This is very delete-heavy workload. However, a new partition is created for each new process instance and this partition is not accessed after the process has finished. So, there is a natural limit on number of tombstones in a single partition. While it is quite possible to create a process generating large number of tombstones, it is not likely to be an issue in real life. So, we are not doing anything special about tombstones for now.   

## Get started

## Resources

* [Gougle Group / Mailing List](https://groups.google.com/forum/?fromgroups&hl=en#!forum/camunda-bpm-dev)
* [Issue Tracker](https://github.com/camunda/camunda-engine-cassandra/issues)
* [Roadmap](https://github.com/camunda/camunda-engine-cassandra/milestones)

### What does already work?

The core process operations and message correlation work.


[Have a Look at some Unit Tests](https://github.com/camunda/camunda-engine-cassandra/blob/master/src/test/java/org/camunda/bpm/engine/cassandra/ExampleTest.java)

### Running the Process Engine Unit Test Suite

It is possible to run the camunda process engine unit test suite against the Cassandra Persistence Layer:

```bash
mvn clean test -P engine-tests
```

This way you can check the compatibility.

## Maintainer

* Natalia Levine (ContextSpace)

## License

Apache License, Version 2.0
