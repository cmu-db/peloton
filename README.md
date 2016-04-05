[![Peloton Logo](http://db.cs.cmu.edu/wordpress/wp-content/uploads/2015/11/peloton.jpg)](http://pelotondb.org/)

[![Build Status](http://jenkins.db.cs.cmu.edu:8080/job/Peloton/badge/icon?style=flat)](http://jenkins.db.cs.cmu.edu:8080/job/Peloton/)
[![GitHub license](https://img.shields.io/badge/license-apache-blue.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.org/cmu-db/peloton.svg?branch=master)](https://travis-ci.org/cmu-db/peloton)

## What Is Peloton?

Peloton is an in-memory DBMS designed for real-time analytics. It can handle both fast ACID transactions and complex analytical queries on the same database. 

## What Problem Does Peloton Solve?

The current trend is to use specialized systems that are optimized for only one of these workloads, and thus require an organization to maintain separate copies of the database. This adds additional cost to deploying a database application in terms of both storage and administration overhead. We present a hybrid DBMS architecture that efficiently supports varied workloads on the same database.

## How Does Peloton Accomplish Its Goals?

Our approach differs from previous methods in that we use a single execution engine that is oblivious to the storage layout of data without sacrificing the performance benefits of the specialized systems. This obviates the need to maintain separate copies of the database in multiple independent systems.

For more details, please visit the [Peloton Wiki](https://github.com/cmu-db/peloton/wiki "Peloton Wiki") page.

## Installation

Check out the [installation instructions](https://github.com/cmu-db/peloton/wiki/Installation).

## Development / Contributing

Please look up the [contributing guide](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#development) for details.

## Issues

Before reporting a problem, check out this how to [file an issue](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#file-an-issue) guide.

## Contributors

[https://github.com/cmu-db/peloton/graphs/contributors](https://github.com/cmu-db/peloton/graphs/contributors)

## License

Copyright (c) 2015-16 [CMU Database Group](http://db.cs.cmu.edu/)  
Licensed under the [Apache License](LICENSE).
