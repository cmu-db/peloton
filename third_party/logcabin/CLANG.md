This file describes how to build LogCabin under Clang with libstdc++ (GNU) or
libc++.

Diego used the following as a `Local.sc` file to build with clang 3.5 and
libc++ 3.5.

```python
CXX='clang++-3.5'
CXXFLAGS=['-Werror', '-stdlib=libc++', '-DGTEST_USE_OWN_TR1_TUPLE']
PROTOCXXFLAGS=['-stdlib=libc++']
GTESTCXXFLAGS=['-stdlib=libc++', '-DGTEST_USE_OWN_TR1_TUPLE']
BUILDTYPE='DEBUG'
PROTOINSTALLPATH='/home/ongardie/local/protobuf-2.6.1/clang-3.5-libc++'
LINKFLAGS='-lc++ -L%s/lib -Wl,-rpath,%s/lib' % (PROTOINSTALLPATH,
                                                PROTOINSTALLPATH)
```

To use libstdc++ instead:
```python
CXX='clang++-3.5'
CXXFLAGS=['-Werror']
BUILDTYPE='DEBUG'
PROTOINSTALLPATH='/home/ongardie/local/protobuf-2.6.1/clang-3.5-libstdc++'
LINKFLAGS='-L%s/lib -Wl,-rpath,%s/lib' % (PROTOINSTALLPATH,
                                          PROTOINSTALLPATH)
```

Note that earlier versions of libstdc++ do not seem to work. For example, Diego wasn't
able to even build ```#include <thread>``` using clang++ 3.4 and libstdc++ 4.6
(due to std::chrono::duration issues as in
https://github.com/Andersbakken/rct/issues/17).


The ProtoBuf library was built as follows (using v2.6.1, git hash bba83652):

```shell
./autogen.sh
./configure \
    CC=clang-3.5 \
    CXX=clang++-3.5  \
    CPPFLAGS="-stdlib=libc++ -DGTEST_USE_OWN_TR1_TUPLE" \
    LIBS="-lc++ -lc++abi" \
    --prefix=$HOME/local/protobuf-2.6.1/clang-3.5-libc++
make -j4
make -j4 check
make install
```

The `GTEST_USE_OWN_TR1_TUPLE` flag works around gtest's attempt to include the
`<tr1/tuple>` header.

To build the ProtoBuf library against libstdc++ instead:

```shell
./autogen.sh
./configure \
    CC=clang-3.5 \
    CXX=clang++-3.5  \
    --prefix=$HOME/local/protobuf-2.6.1/clang-3.5-libstdc++
make -j4
make -j4 check
make install
```
