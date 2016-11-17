<a href="http://pelotondb.org/"><img src="http://db.cs.cmu.edu/wordpress/wp-content/uploads/2016/07/peloton.jpg" alt="Peloton Logo"></a>
-----------------
[![GitHub license](https://img.shields.io/badge/license-apache-green.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)
[![Version](https://img.shields.io/badge/version-0.0.3-red.svg)](http://pelotondb.org/)
[![Travis Status](https://travis-ci.org/cmu-db/peloton.svg?branch=master)](https://travis-ci.org/cmu-db/peloton)
[![Jenkins Status](http://jenkins.db.cs.cmu.edu:8080/job/Peloton/badge/icon)](http://jenkins.db.cs.cmu.edu:8080/job/Peloton/)
[![Coverage Status](https://coveralls.io/repos/github/cmu-db/peloton/badge.svg?branch=master)](https://coveralls.io/github/cmu-db/peloton?branch=master)
[![Join the chat at https://gitter.im/cmu-db/peloton](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cmu-db/peloton?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## What Is Peloton?

* Peloton is a self-driving in-memory relational DBMS for real-time analytics. 
* It contains domain-specific AI for automatically adapting to evolving real-world workloads. 
* It is designed from the ground up to leverage the characteristics of <a href="https://www.engadget.com/2015/07/28/intel-3d-memory-1000-times-faster/">fast non-volatile memory technologies</a>.
* It can handle both fast ACID transactions and complex analytical queries on the same database. 

## What Problem Does Peloton Solve?

The current trend is to manually tune the DBMS configuration for evolving real-world workloads. This approach requires the database administrator to constantly adapt the DBMS based on the current query workload. The adminstrator needs to understand the subtle interactions between the different knobs exposed by the system to do this kind of black-box tuning. Further, it is often the case that several critical parameters used within the DBMS are not exposed as knobs to the administrator.

Peloton is designed to automate some of the critical tasks performed by the database administrator. Using novel physical design algorithms and domain-specific AI, it can automatically and incrementally adapt the storage layout, access methods, and data placement policy employed inside the DBMS in tandem with workload shifts. 

For more details, please visit the [Peloton Wiki](https://github.com/cmu-db/peloton/wiki "Peloton Wiki") page.

## Installation

Check out the [installation instructions](https://github.com/cmu-db/peloton/wiki/Installation).

## Supported Platforms

Peloton is known to work on the following platforms. Please note that it will not compile on 32-bit systems.

* Ubuntu Linux 14.04+ (64-bit)
* <s>Mac OS X 10.9+ (64-bit)</s>

## Development / Contributing

We invite you to help us build the future of self-driving DBMSs. Please look up the [contributing guide](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#development) for details.

## Issues

Before reporting a problem, check out this how to [file an issue](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#file-an-issue) guide.

## Status

_Technology preview_: currently unsupported, may be functionally incomplete or unsuitable for production use.

## Contributors

See the [contributors page](https://github.com/cmu-db/peloton/graphs/contributors).

## License

Copyright (c) 2015-16 [CMU Database Group](http://db.cs.cmu.edu/)  
Licensed under the [Apache License](LICENSE).
