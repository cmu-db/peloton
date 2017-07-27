Proxy System
============

Oh, so you want to call C++ code from codegen'd code? Or, you want to
access C++ data structures from codegen'd code? Well young-blood, this
is precisely what the proxying system was designed to do. In general,
there are three steps to interface with C++ code from JIT'd code:

1. Declare the proxy for the class in a header file.
2. Define the elements of the class.
3. Specify which methods you'd like to access.

The steps are _greatly_ simplified by using several provided macros.

**Example**: To illustrate the process, let's run through an example.
Assume you have a C++ class as follows:

```C++
namespace me {

// Your fancy class
class MyStruct : public BaseStruct {
 public:
  // Initialize
  void Init();

  // A function that does work and returns a 64-bit integer
  uint64_t DoWork();

  // Destruction
  void Destroy();

 private:
  int a;
  int b;
};

}  // namespace me
```

To call functions on `MyStruct`, we'll need to define a proxy. Create
`src/include/codegen/proxy/mystruct_proxy.h` with the following
contents:

```C++
#include "codegen/proxy/proxy.h"

#include "mystruct.h"

PROXY(MyStruct) {
  DECLARE_MEMBER(0, int, a);
  DECLARE_MEMBER(1, int, b);
  DECLARE_TYPE

  DECLARE_METHOD(Init);
  DECLARE_METHOD(DoWork);
  DECLARE_METHOD(Destroy);
};

TYPE_BUILDER(MyStruct, me::MyStruct);

```

The proxy resembles the structure of the C++ class. The `PROXY` macro
defines a new class with the same name, but with `Proxy` as a suffix.
Thus, to interact with `MyStruct`, you use `MyStructProxy`.

We define two accessible member fields, `a` and `b` at slots 0 and 1,
respectively. The ordering of the members must be equivalent to the
layout of the fields in memory. If you do not need access to the fields,
it is simpler to use an opaque byte-array whose size is
`sizeof(MyStruct)`. However, you must define a type if you need to call
instance methods on the class.

Next, we declare proxies to three methods: `Init`, `DoWork`, and
`Destroy`. Given this, to call `MyStruct::Init()` when generating code,
you can simply write: `codegen.Call(MyStructProxy::Init)` given a
`CodeGen` instance.

To link this altogether, you need to link the `Init` function in
`MyStructProxy` to `MyStruct::Init` in C++. Create
`src/codegen/proxy/mystruct_proxy.cpp` with the following contents:

```C++
#include "codegen/proxy/mystruct_proxy.h"

DEFINE_METHOD(me, MyStruct, Init);
DEFINE_METHOD(me, MyStruct, DoWork);
DEFINE_METHOD(me, MyStruct, Destroy);
```

Each `DEFINE_METHOD` function takes three arguments: the namespace and
name of the C++ class, and the name of the C++ method you want to proxy.
With this, the linking of the proxy to the C++ class is complete.
