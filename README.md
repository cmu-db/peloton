<a href="http://pelotondb.org/"><img src="http://db.cs.cmu.edu/wordpress/wp-content/uploads/2016/07/peloton.jpg" alt="Peloton Logo"></a>
-----------------
[![GitHub license](https://img.shields.io/badge/license-apache-green.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)
[![Version](https://img.shields.io/badge/version-0.0.5-red.svg?style=flat)](http://pelotondb.org)
[![Travis Status](https://travis-ci.org/cmu-db/peloton.svg?branch=master)](https://travis-ci.org/cmu-db/peloton)
[![Jenkins Status](http://jenkins.db.cs.cmu.edu:8080/job/peloton/job/master/badge/icon)](http://jenkins.db.cs.cmu.edu:8080/job/peloton/)
[![Coverage Status](https://coveralls.io/repos/github/cmu-db/peloton/badge.svg?branch=master)](https://coveralls.io/github/cmu-db/peloton?branch=master)

## What Is Peloton?

* A self-driving SQL database management system.
* Integrated artificial intelligence components that enable autonomous optimization.
* Native support for byte-addressable non-volatile memory (NVM) storage technology.
* Lock-free multi-version concurrency control to support real-time analytics.
* Postgres network-protocol and JDBC compatible.
* High-performance, lock-free Bw-Tree for indexing.
* 100% Open-Source (Apache Software License v2.0).

## What Problem Does Peloton Solve?

During last two decades, researchers and vendors have built advisory tools to assist database administrators in system tuning and physical design. This work is incomplete because they still require the final decisions on changes in the database, and are reactionary measures that fix problems after they occur.

A new architecture is needed for a truly “self-driving” database management system (DBMS) which is designed for autonomous operations. This is different than earlier attempts because all aspects of the system are controlled by an integrated planning component. In addition to optimizing the system for the current workload, it predicts future workload trends which lets the system prepare itself accordingly. This eliminates the requirement of a human to determine the right way, and reduces time taken to deploy the changes, optimizing the DBMS to provide high-performance. Auto-management of these systems has surpassed the abilities of human experts.

Peloton is a relational database management system designed for fully autonomous optimization of hybrid workloads. See the [peloton wiki](https://github.com/cmu-db/peloton/wiki "Peloton Wiki") for more information.

## Installation

Check out the [installation instructions](https://github.com/cmu-db/peloton/wiki/Installation).

## Supported Platforms

Peloton has been tested to work on the following platforms:

* Ubuntu Linux 14.04+ (64-bit) [gcc4, gcc5]
* Fedora Linux 24+ (64-bit) [gcc4, gcc5]
* Mac OS X 10.9+ (64-bit) [XCode v8 Only]

 Please note that it will not compile on 32-bit systems.

## Development / Contributing

We invite you to help us build the future of self-driving DBMSs. Please look up the [contributing guide](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#development) for details.

## Issues

Before reporting a problem, please check how to [file an issue](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#file-an-issue) guide.

## Status

_Technology preview_: currently unsupported, possibly due to incomplete functionality or unsuitability for production use.

## Contributors

See the [people page](https://github.com/cmu-db/peloton/graphs/contributors) for the full listing of contributors.

## License

Copyright (c) 2014-2018 [CMU Database Group](http://db.cs.cmu.edu/)  
Licensed under the [Apache License](LICENSE).
