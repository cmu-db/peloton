//#pragma once
//
//#include "codegen/proxy/proxy.h"
//#include "codegen/proxy/type_builder.h"
//#include "common/internal_types.h"
//#include "codegen/value.h"
//
//namespace peloton {
//namespace codegen {
//
//PROXY(Diff) {
//  DECLARE_MEMBER(0, char[sizeof(peloton::codegen::Delta)], opaque);
//  DECLARE_TYPE;
//};
//
//TYPE_BUILDER(Diff, peloton::codegen::Delta);
//
//}  // namespace codegen
//}  // namespace pelotonv