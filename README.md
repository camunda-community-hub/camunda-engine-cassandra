# camunda-engine-cassandra

Cassandra Persistence for Camunda (Community Extension)

## Get started

## Resources

* [Gougle Group / Mailing List](https://groups.google.com/forum/?fromgroups&hl=en#!forum/camunda-bpm-dev)
* [Issue Tracker](link-to-issue-tracker) _use github unless you got your own_
* [Roadmap](link-to-issue-tracker-filter) _if in terms of tagged issues_
* [Changelog](link-to-changelog) _lets users track progress on what has been happening_
* [Download](link-to-downloadable-archive) _if downloadable_
* [Contributing](link-to-contribute-guide) _if desired, best to put it into a CONTRIBUTE.md file_

## Roadmap

### Current State

Persistence for core Runtime Data Structures and the Repository: 
* Executions
* Variables
* Event Subscriptions
* Process Definitions, Resources, Deployments

[Have a Look at some Unit Tests](https://github.com/camunda/camunda-engine-cassandra/blob/master/src/test/java/org/camunda/bpm/engine/cassandra/ExampleTest.java)

### Goal for the first Release

* "core engine" works, which excludes
    * BPMN User Tasks
    * History
    * Job Executor
    * Complex Queries in General
    
### Running the Process Engine Unit Test Suite

It is possible to run the camunda process engine unit test suite against the Cassandra Persistence Layer:

```bash
mvn clean test -P engine-tests
```

This way you can check the compatibility.

## Maintainer

* Natalia Levine

## License

Apache License, Version 2.0

_(Choose among Apache License, Version 2.0 or The MIT License. Update file LICENSE as well.)_
