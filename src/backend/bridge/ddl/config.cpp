/*
 * config.cpp
 *
 *  Created on: Aug 11, 2015
 *      Author: vivek
 */

#include "backend/bridge/ddl/config.h"

#include <cassert>
#include "postgres.h"
#include "utils/guc_tables.h"

extern struct config_bool ConfigureNamesBool[];
extern struct config_int ConfigureNamesInt[];
extern struct config_real ConfigureNamesReal[];
extern struct config_string ConfigureNamesString[];
extern struct config_enum ConfigureNamesEnum[];

namespace peloton {
namespace bridge {

std::map<std::string, ConfigurationOptions::config_details>
ConfigurationOptions::ConstructConfigurationMap() {

	std::map<std::string, config_details> config_map;

	int count;
	config_details temp;

	/* For bool options */
	count = 0;
	while(ConfigureNamesBool[count].gen.name != NULL) {

		temp.type = BOOLEAN_TYPE;
		auto val = GetConfigOption(ConfigureNamesBool[count].gen.name, false, false);
		temp.value = std::string(val);

		config_map.insert(std::map<std::string, config_details>::value_type(
									ConfigureNamesBool[count].gen.name,	temp));

		count++;
	}
	int bool_count = count;


	/* For int options */
	count = 0;
	while(ConfigureNamesInt[count].gen.name != NULL) {

		temp.type = INTEGER_TYPE;
		auto val = GetConfigOption(ConfigureNamesInt[count].gen.name, false, false);
		temp.value = std::string(val);

		config_map.insert(std::map<std::string, config_details>::value_type(
									ConfigureNamesInt[count].gen.name,	temp));

		count++;
	}
	int int_count = count;


	/* For real options */
	count = 0;
	while(ConfigureNamesReal[count].gen.name != NULL) {

		temp.type = REAL_TYPE;
		auto val = GetConfigOption(ConfigureNamesReal[count].gen.name, false, false);
		temp.value = std::string(val);

		config_map.insert(std::map<std::string, config_details>::value_type(
									ConfigureNamesReal[count].gen.name,	temp));

		count++;
	}
	int real_count = count;


	/* For string options */
	count = 0;
	while(ConfigureNamesString[count].gen.name != NULL) {

		temp.type = STRING_TYPE;
		auto val = GetConfigOption(ConfigureNamesString[count].gen.name, false, false);
		if(val == NULL)
			temp.value = "NULL";
		else
			temp.value = std::string(val);

		config_map.insert(std::map<std::string, config_details>::value_type(
								ConfigureNamesString[count].gen.name,	temp));

		count++;
	}
	int string_count = count;


	/* For enum options */
	count = 0;
	while(ConfigureNamesEnum[count].gen.name != NULL) {

		temp.type = ENUM_TYPE;
		auto val = GetConfigOption(ConfigureNamesEnum[count].gen.name, false, false);
		temp.value = std::string(val);

		config_map.insert(std::map<std::string, config_details>::value_type(
									ConfigureNamesEnum[count].gen.name,	temp));

		count++;
	}
	int enum_count = count;


	std::cout << "trying get-set-get for peloton_mode..." << std::endl;
	std::string pel_opt("peloton_mode");
	std::string pel_val("peloton_mode_2");

	puts(GetConfigurationOption(pel_opt).c_str());
	SetConfigurationOption(pel_opt, pel_val);
	//puts(GetConfigurationOption(pel_opt).c_str());

	/* print the config_map for testing */
/*	int i;
	for(i=0; i<bool_count; i++) {
		puts(ConfigureNamesBool[i].gen.name);
		std::cout << config_map[ConfigureNamesBool[i].gen.name].type << std::endl;
		puts(config_map[ConfigureNamesBool[i].gen.name].value.c_str());
	}

	for(i=0; i<int_count; i++) {
		puts(ConfigureNamesInt[i].gen.name);
		std::cout << config_map[ConfigureNamesInt[i].gen.name].type << std::endl;
		puts(config_map[ConfigureNamesInt[i].gen.name].value.c_str());
	}

	for(i=0; i<real_count; i++) {
		puts(ConfigureNamesReal[i].gen.name);
		std::cout << config_map[ConfigureNamesReal[i].gen.name].type << std::endl;
		puts(config_map[ConfigureNamesReal[i].gen.name].value.c_str());
	}

	for(i=0; i<string_count; i++) {
		puts(ConfigureNamesString[i].gen.name);
		std::cout << config_map[ConfigureNamesString[i].gen.name].type << std::endl;
		puts(config_map[ConfigureNamesString[i].gen.name].value.c_str());
	}

	for(i=0; i<enum_count; i++) {
		puts(ConfigureNamesEnum[i].gen.name);
		std::cout << config_map[ConfigureNamesEnum[i].gen.name].type << std::endl;
		puts(config_map[ConfigureNamesEnum[i].gen.name].value.c_str());
	}
*/

	return config_map;
}


std::string ConfigurationOptions::GetConfigurationOption(std::string option_name) {
	return GetConfigOption(option_name.c_str(), false, false);
}


void ConfigurationOptions::SetConfigurationOption(std::string option_name, std::string option_value) {
	SetConfigOption(option_name.c_str(), option_value.c_str(), PGC_USERSET, PGC_S_USER);
}


} // namespace peloton
} // namespace bridge

