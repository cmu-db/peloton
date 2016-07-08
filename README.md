<div align="center">
<a href="http://pelotondb.org/">
<img src="http://db.cs.cmu.edu/wordpress/wp-content/uploads/2016/07/peloton.jpg" alt="Peloton Logo"><br><br>
</a>
</div>
[![Join the chat at https://gitter.im/cmu-db/peloton](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cmu-db/peloton?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Travis Status](https://travis-ci.org/cmu-db/peloton.svg?branch=master)](https://travis-ci.org/cmu-db/peloton)
[![GitHub license](https://img.shields.io/badge/license-apache-blue.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)
[![Jenkins Status](http://jenkins.db.cs.cmu.edu:8080/job/Peloton/badge/icon)](http://jenkins.db.cs.cmu.edu:8080/job/Peloton/)
<!---[![Coverage Status](https://coveralls.io/repos/github/cmu-db/peloton/badge.svg?branch=master)](https://coveralls.io/github/cmu-db/peloton?branch=master)--->

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

We invite you to help us build the future of self-driving DBMSs. This is the best moment to participate, as everyone can make an impact. Please look up the [contributing guide](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#development) for details.

## Issues

Before reporting a problem, check out this how to [file an issue](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#file-an-issue) guide.

## Status

_Technology preview_: currently unsupported, may be functionally incomplete or unsuitable for production use.

## Contributors

See the [contributors page](https://github.com/cmu-db/peloton/graphs/contributors).

## License

Copyright (c) 2015-16 [CMU Database Group](http://db.cs.cmu.edu/)  
Licensed under the [Apache License](LICENSE).
