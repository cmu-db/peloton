# Index

This directory contains source file for implementing Peloton's in-memory index, BwTree, and related utilities.

BwTree
======

BwTree is a concurrent lock-free B+Tree index. It was originally proposed by Microsoft Research and then adopted into Peloton as the major in-memory index structure. BwTree features a hardware compare-and-swap based update protocol and software transaction based structural modification protocol and thus provides high throughput OLTP support to the entire system.

A standalone version of BwTree could be downloaded here: https://github.com/wangziqi2013/BwTree

Index Wrapper 
=============
The index wrapper interfaces between BwTree and Peloton by exposing a uniform set of functions to the external world. Future addition of indices could be achieved by providing wrappers with appropriate member functions.

We strive to make index wrapper a mere interfacing component and thus make it carry as little logic as possible. In future development of Peloton please implement index logic either inside the index or inside coprresponding executors.

Index Factory
=============
The index factory is responsible for selecting an index given restrictions on keys. The selection of index type is based on whether the key could be represented in a special compact form and the size of the key. If requirements for the special compact form are satisfied then the index could be made faster and more memory friendly by using the more compact form of keys

Index Key
=========
Index keys are implemented as fixed length C++ objects that is directly used with the index. A proposal for CompactIntsKey could be found here: https://github.com/cmu-db/peloton/issues/434
