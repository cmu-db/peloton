<a href="http://pelotondb.org/"><img src="http://db.cs.cmu.edu/wordpress/wp-content/uploads/2016/07/peloton.jpg" alt="Peloton Logo"></a>
-----------------
[![GitHub license](https://img.shields.io/badge/license-apache-green.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)
[![Version](https://img.shields.io/badge/version-0.0.5-red.svg)](http://pelotondb.org/)
[![Travis Status](https://travis-ci.org/cmu-db/peloton.svg?branch=master)](https://travis-ci.org/cmu-db/peloton)
[![Jenkins Status](http://jenkins.db.cs.cmu.edu:8080/job/Peloton/badge/icon)](http://jenkins.db.cs.cmu.edu:8080/job/Peloton/)
[![Coverage Status](https://coveralls.io/repos/github/cmu-db/peloton/badge.svg?branch=master)](https://coveralls.io/github/cmu-db/peloton?branch=master)
[![Join the chat at https://gitter.im/cmu-db/peloton](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cmu-db/peloton?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## What Is Peloton?

* Peloton is a self-driving SQL database management system.
* Integrated artificial intelligence components that enable autonomous optimizations.
* Native support for byte-addressable non-volatile memory (NVM) storage technology.
* Lock-free multi-version concurrency control to support real-time analytics.
* Postgres wire-protocol and JDBC compatible.
* High-performance, lock-free Bw-Tree for indexing.
* 100% Open-Source (Apache Software License v2.0).

## What Problem Does Peloton Solve?

In the last two decades, both researchers and vendors have built advisory tools to assist database administrators in various aspects of system tuning and physical design. Most of this previous work, however, is incomplete because they still require humans to make the final decisions about any changes to the database and are reactionary measures that fix problems after they occur.

What is needed for a truly “self-driving” database management system (DBMS) is a new architecture that is designed for autonomous operation. This is different than earlier attempts because all aspects of the system are controlled by an integrated planning component that not only optimizes the system for the current workload, but also predicts future workload trends so that the system can prepare itself accordingly. With this, the DBMS can support all of the previous tuning techniques without requiring a human to determine the right way and proper time to deploy them. It also enables new optimizations that are important for modern high-performance DBMSs, but which are not possible today because the complexity of managing these systems has surpassed the abilities of human experts.

Peloton is a relational database management system designed for fully autonomous optimization of hybrid workloads. See the [peloton wiki](https://github.com/cmu-db/peloton/wiki "Peloton Wiki") for more information.

## Installation

Check out the [installation instructions](https://github.com/cmu-db/peloton/wiki/Installation).

## Supported Platforms

Peloton is known to work on the following platforms. Please note that it will not compile on 32-bit systems.

* Ubuntu Linux 14.04+ (64-bit)
* Fedora Linux 24+ (64-bit)
* <s>Mac OS X 10.9+ (64-bit)</s>

## Development / Contributing

We invite you to help us build the future of self-driving DBMSs. Please look up the [contributing guide](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#development) for details.

## Issues

Before reporting a problem, check out this how to [file an issue](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#file-an-issue) guide.

## Status

_Technology preview_: currently unsupported, may be functionally incomplete or unsuitable for production use.

## Contributors

See the [people page](https://github.com/cmu-db/peloton/graphs/contributors) for the full listing of contributors.

## License

Copyright (c) 2014-16 [CMU Database Group](http://db.cs.cmu.edu/)  
Licensed under the [Apache License](LICENSE).
