### Questions

1. Is [codegen.CallFunc](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/table_scan_translator.cpp#L68) a function call inside LLVM?

2. What can be seen by the generated code?

3. What cannot be seen by the generated code?
    * [CodeContext](https://github.com/tq5124/peloton-1/blob/codegen/src/include/codegen/code_context.h)

4. Is the [catalog_ptr](https://github.com/tq5124/peloton-1/blob/codegen/src/codegen/table_scan_translator.cpp#L66) created in C++, or inside the generated LLVM?
    * If it is created in C++ ... Its value is compiled into the LLVM? How is the two worlds bridged??
