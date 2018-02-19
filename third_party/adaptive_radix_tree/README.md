This code is originally from [flode](https://github.com/flode/ARTSynchronized), but
has been modified. The modifications include:

1. Fix race condition during child traversal.
2. Store duplicate keys in chunked-array leaf nodes.
