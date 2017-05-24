//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// schema_proxy.cpp
//
// Identification: src/codegen/schema_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/schema/schema_proxy.h"
#include "catalog/schema.h"

namespace peloton {
namespace codegen {

llvm::Type* SchemaProxy::GetType(CodeGen& codegen) {
  static const std::string kSchemaTypeName = "peloton::catalog::Schema";
  // Check if the data table type has already been registered in the current
  // codegen context
  auto schema_type = codegen.LookupTypeByName(kSchemaTypeName);
  if (schema_type != nullptr) {
    return schema_type;
  }

  // Type isn't cached, create a new one
  auto* byte_array =
      llvm::ArrayType::get(codegen.Int8Type(), sizeof(catalog::Schema));
  schema_type = llvm::StructType::create(codegen.GetContext(), {byte_array},
                                         kSchemaTypeName);
  return schema_type;
}

}  // namespace codegen
}  // namespace peloton
