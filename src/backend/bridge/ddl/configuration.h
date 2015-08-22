//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.h
//
// Identification: src/backend/bridge/ddl/configuration.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <string>
#include <iostream>

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Configuration Manager
//===--------------------------------------------------------------------===//

class ConfigManager {

public:

	typedef enum
	{
		INVALID_TYPE,
		BOOLEAN_TYPE,
		INTEGER_TYPE,
		REAL_TYPE,
		STRING_TYPE,
		ENUM_TYPE
	} config_type;

	typedef struct {
	  // type of parameter
		config_type type;

		std::string value;
	} config_details;


	static std::string GetConfigOption(std::string option_name);

	static void SetConfigOption(std::string option_name, std::string option_value);

  static std::map<std::string, config_details> BuildConfigMap();

};


} // namespace peloton
} // namespace bridge
