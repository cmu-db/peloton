# libpg_query [![Build Status](https://travis-ci.org/lfittl/libpg_query.svg?branch=master)](https://travis-ci.org/lfittl/libpg_query)

C library for accessing the PostgreSQL parser outside of the server.

This library uses the actual PostgreSQL server source to parse SQL queries and return the internal PostgreSQL parse tree.

Note that this is mostly intended as a base library for https://github.com/lfittl/pg_query (Ruby) and https://github.com/lfittl/pg_query.go (Go).

You can find further background to why a query's parse tree is useful here: https://pganalyze.com/blog/parse-postgresql-queries-in-ruby.html


## Installation

```
git clone git://github.com/lfittl/libpg_query
cd libpg_query
make
```

Due to compiling parts of PostgreSQL, running `make` will take a while. Expect up to 5 minutes.


## Usage

A [full example](https://github.com/lfittl/libpg_query/blob/master/examples/simple.c) that parses a query looks like this:

```
#include <pg_query.h>
#include <stdio.h>

int main() {
  PgQueryParseResult result;

  pg_query_init();

  result = pg_query_parse("SELECT 1");

  printf("%s\n", result.parse_tree);

  pg_query_free_parse_result(result);
}
```

Compile it like this:

```
cc -Ilibpg_query -Llibpg_query -lpg_query example.c
```

This will output:

```
[{"SELECT": {"distinctClause": null, "intoClause": null, "targetList": [{"RESTARGET": {"name": null, "indirection": null, "val": {"A_CONST": {"val": 1, "location": 7}}, "location": 7}}], "fromClause": null, "whereClause": null, "groupClause": null, "havingClause": null, "windowClause": null, "valuesLists": null, "sortClause": null, "limitOffset": null, "limitCount": null, "lockingClause": null, "withClause": null, "op": 0, "all": false, "larg": null, "rarg": null}}]
```


## Versions

For stability, it is recommended you use individual tagged git versions, see CHANGELOG.

Current `master` reflects a PostgreSQL base version of 9.4, with a legacy output format.

New development is happening on `9.5-latest`, which will become `master` in the future.


## Authors

- [Lukas Fittl](mailto:lukas@fittl.com)


## License

Copyright (c) 2015, Lukas Fittl <lukas@fittl.com><br>
libpg_query is licensed under the 3-clause BSD license, see LICENSE file for details.

Query normalization code:<br>
Copyright (c) 2008-2015, PostgreSQL Global Development Group
