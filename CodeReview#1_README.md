## Files Added

* [compressed_tile.cpp](https://github.com/rohit-cmu/compression-peloton/blob/master/src/storage/compressed_tile.cpp)
* [compressed_tile.h](https://github.com/rohit-cmu/compression-peloton/blob/master/src/include/storage/compressed_tile.h)

## Strategy for Compressing

* Once a tile gets full (all slots occupied by tuples), the CompressTile is created.
* We scan each column and sort it.
* We compute the median, min and max, this gives us the minoffset(min-median) and maxoffset(max-median).
* If these offsets can be represented in a smaller data type than the original data type, we compress the entire column.
* The median is chosen as the base, and essentially only the offsets are stored in the column.
* Currently we can compress SMALLINT and INTEGER and BIGINT. TINYINT is already 1 byte so we do not compress it.
* We are yet to add support for decimal values.
* This median is also stored as the metadata to later retrieve the original value

## Testing

* [Compression Correctness Test](https://github.com/rohit-cmu/compression-peloton/blob/master/test/sql/compression_sql_test.cpp)
* [Compression Size Test](https://github.com/rohit-cmu/compression-peloton/blob/master/test/storage/compression_test.cpp)


Compression Correctness Test:
* This test inserts 25 tuples
* Each tuple is of the form (i, i*100) where i belongs to (0,25)
* Since each tile group contains 1 tile and 10 tuples per tile, there are 3 tile groups formed.
* The first 2 tile groups are full and get compressed.
* The third tile group has 5 slots vacant and is still not full and is uncompressed.
* Thus we now have compressed and uncompressed data.
* We now perform a SELECT * on this and expect to correctly recieve the true value of the compressed data and uncompressed data.

Compression Size Test:
* This test inserts 100 tuples
* Each tuple is of the form [Integer, Integer, Decimal, Varchar]
* Thus the tuple length is (4+ 4+ 8+ 8) = 24 bytes.
* We currently support the compression of integers
* The integers get compressed to TINYINT making the tuple sizes now (1+ 1+ 8+ 8) = 18 bytes
* The test checks this decrease in size.




