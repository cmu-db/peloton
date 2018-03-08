//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updater_proxy.h
//
// Identification: src/include/codegen/proxy/updater_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/updater.h"
#include "codegen/proxy/proxy.h"

namespace peloton {
namespace codegen {

PROXY(Updater) {
  /// We don't need access to internal fields, so use an opaque byte array
  DECLARE_MEMBER(0, char[sizeof(Updater)], opaque);
  DECLARE_TYPE;

  /// Proxy Init() and Update() in codegen::Updater
  DECLARE_METHOD(Init);
  DECLARE_METHOD(Prepare);
  DECLARE_METHOD(PreparePK);
  DECLARE_METHOD(GetPool);
  DECLARE_METHOD(Update);
  DECLARE_METHOD(UpdatePK);
  DECLARE_METHOD(TearDown);
};

TYPE_BUILDER(Updater, codegen::Updater);

}  // namespace codegen
}  // namespace peloton
