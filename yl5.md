### Questions

1. Is [codegen.CallFunc](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/table_scan_translator.cpp#L68) a function call inside LLVM?

2. What can be seen by the generated code?
    * [RuntimeState](https://github.com/tq5124/peloton-1/blob/codegen/src/include/codegen/runtime_state.h)

3. What cannot be seen by the generated code?
    * [CodeContext](https://github.com/tq5124/peloton-1/blob/codegen/src/include/codegen/code_context.h)

### Notes

1. [RuntimeState](https://github.com/tq5124/peloton-1/blob/codegen/src/include/codegen/runtime_state.h)
    * This is where you can pass C++ object around to the LLVM world
    * Global state is combined into a struct and passed to the 3 major LLVM function. Operators must initialize any global state in init() and clean up in tearDown()
    * Local state is guaranteed to be allocated once at the start of the plan()
    * Example:
         * [RegisterState](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/compilation_context.cpp#L34): register a global state
         * [GetStateValue](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/compilation_context.cpp#L129): retrieve the state
         * [catalog_ptr](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/table_scan_translator.cpp#L66): use the value
