### Questions

1. Is [codegen.CallFunc](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/table_scan_translator.cpp#L68) a function call inside LLVM?
   * Yes
   
2. What can be seen by the generated code?
    * [RuntimeState](https://github.com/tq5124/peloton-1/blob/codegen/src/include/codegen/runtime_state.h)

3. What cannot be seen by the generated code?
    * [CodeContext](https://github.com/tq5124/peloton-1/blob/codegen/src/include/codegen/code_context.h)

4. What are the LLVM types for these parameters in [SubmitTask](https://github.com/tq5124/peloton-1/blob/codegen/src/include/common/thread_pool.h#L70)?

### Notes

1. [RuntimeState](https://github.com/tq5124/peloton-1/blob/codegen/src/include/codegen/runtime_state.h)
    * This is where you can pass C++ object around to the LLVM world
    * Global state is combined into a struct and passed to the 3 major LLVM function. Operators must initialize any global state in init() and clean up in tearDown()
    * Local state is guaranteed to be allocated once at the start of the plan()
    * Example:
         * [RegisterState](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/compilation_context.cpp#L34): register a global state
         * [GetStateValue](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/compilation_context.cpp#L129): retrieve the state
         * [catalog_ptr](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/table_scan_translator.cpp#L66): use the value

2. Proxy
   1. [CatalogProxy.h](https://github.com/tq5124/peloton-1/blob/codegen/src/include/codegen/catalog_proxy.h) and [CatalogProxy.cpp](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/catalog_proxy.cpp)
      1. Register a pointer to Catalog object using [CatalogProxy::GetType](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/compilation_context.cpp#L34)
      2. Invoke a LLVM call to a Catalog function using [CatalogProxy::_GetTableWithOid::GetFunction](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/table_scan_translator.cpp#L68)
      
3. Thread pool
   * [Init.h](https://github.com/tq5124/peloton-1/blob/codegen/src/include/common/init.h#L19) has a global thread pool, which is initialized [here](https://github.com/tq5124/peloton-1/blob/codegen/src/common/init.cpp#L41) and destroyed [here](https://github.com/tq5124/peloton-1/blob/codegen/src/common/init.cpp#L91)
   * Usage example: [TransactionLevelGCManager](https://github.com/tq5124/peloton-1/blob/codegen/src/include/gc/transaction_level_gc_manager.h#L85) and [LibeventMasterThread](https://github.com/tq5124/peloton-1/blob/codegen/src/wire/libevent_thread.cpp#L60)
