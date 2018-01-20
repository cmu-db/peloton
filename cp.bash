#!/bin/bash

cp /vagrant/peloton/src/catalog/catalog.cpp ~/peloton/src/catalog/
cp /vagrant/peloton/src/codegen/codegen.cpp ~/peloton/src/codegen/
cp /vagrant/peloton/src/codegen/proxy/string_functions_proxy.cpp ~/peloton/src/codegen/proxy/
cp /vagrant/peloton/src/codegen/type/varchar_type.cpp ~/peloton/src/codegen/type/
cp /vagrant/peloton/src/function/string_functions.cpp ~/peloton/src/function/
cp /vagrant/peloton/src/include/codegen/proxy/string_functions_proxy.h ~/peloton/src/include/codegen/proxy/
cp /vagrant/peloton/src/include/common/internal_types.h ~/peloton/src/include/common/
cp /vagrant/peloton/src/include/function/string_functions.h ~/peloton/src/include/function/

