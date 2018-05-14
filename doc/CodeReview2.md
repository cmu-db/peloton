# Dictionary Encoding

## Files Added

* [dict_encoded_tile.cpp](https://github.com/suafeng/peloton/blob/master/src/storage/dict_encoded_tile.cpp)
* [dict_encoded_tile.h](https://github.com/suafeng/peloton/blob/master/src/include/storage/dict_encoded_tile.cpp)

### Other Existing Files Modified

Some other files are also modified:

* [runtime_functions_proxy.h](https://github.com/suafeng/peloton/blob/master/src/include/codegen/proxy/runtime_functions_proxy.h)
* [runtime_functions_proxy.cpp](https://github.com/suafeng/peloton/blob/master/src/codegen/proxy/runtime_functions_proxy.cpp)
* [runtime_functions.h](https://github.com/suafeng/peloton/blob/master/src/include/codegen/runtime_functions.h)
* [runtime_functions.cpp](https://github.com/suafeng/peloton/blob/master/src/codegen/runtime_functions.cpp)
* [tile_group.h](https://github.com/suafeng/peloton/blob/master/src/include/codegen/tile_group.h)
* [tile_group.cpp](https://github.com/suafeng/peloton/blob/master/src/codegen/tile_group.cpp)

## Implementation of dictionary encoding

Currently dictionary encoding is only for various length type (varchar and varbinary). When encoding tile, a dictionary will be
created for that tile. Each unique varlen value will be matched with an index. The mapping from index to value is an array. The
index being assigned to varlen value is the its index in this array. This array actually is an array of pointer, each pointer
pointing to a varlen value (all values are stored on heap). This index replaces the varlen value in tile. Then let's talk about
how to query on these encoded data. Originally, varlen values normally are stored as pointer in tile (so called uninlined data),
the execution engine will use this pointer to access these varlen values. In our implementation, since we store index in tile
instead of the pointer to varlen value, we need to interpret this index to the pointer to the varlen value. So index of the value and pointer to the array is used to do some pointer arithmetic and get the pointer to varlen value.

## Testing

* [Encoding Correctness Test](https://github.com/suafeng/peloton/blob/master/test/storage/tile_compression_test.cpp)
* [Select On Encoded Data Correctness Test](https://github.com/suafeng/peloton/blob/master/test/storage/tile_compression_select_test.cpp)

### Encoding Correctness Test

* Tree tuples are inserted to a tile.
* There are five columns, two of them are varchar.
* This tile is encoded and decoded, to see whether we have the same tile before encoding and after decoding.

### Select On Encoded Data Correctness Test

* A table "foo" is created and three tuples are inserted into this table.
* Each tuple consists of an integer and a string.
* All tiles in this table are compressed and query "select * from foo" is executed.
* Check whether we can get the expected results.