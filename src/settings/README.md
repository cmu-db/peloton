# Settings
There are two major components to Peloton settings:
- SettingsCatalog
- SettingsManager

## SettingsCatalog
All settings are stored in the 'pg_settings' catalog table. pg_settings
contains the following columns to track configuration information:
- name (varchar) : the name of a parameter
- value (varchar) : the current value of a parameter
- value_type (integer) : the type (type::TypeId) of the value
- description (varchar) : a short description of usage
- min_value (varchar) : the minimum legal value
- max_value (varchar) : the maximum legal value
- default_value (varchar) : default value
- is_mutable (boolean) : be true if the parameter is allowed to modify during the system running
- is_persistent (boolean) : be true if the value should be restored from logs/checkpoints when the system restarts

## SettingsManager
This is a programmatic wrapper around SettingsCatalog. It provides
several APIs: `DefineSetting`, `GetValue` and `SetValue` for defining,
retrieving and setting configuration values at runtime.
It also provides utility functions such as `GetInt(id`) and
`SetInt(id, value)` for simplified and typed access to configuration.
SettingsManager keeps a map from name to value which is used during
bootstrap before catalog being initialized.

## SettingId
SettingId is an enum class contains members whose names are the same as
the names of parameters.

## How To Use It
When you want to add a new parameter, define it with macros
`SETTING_(int, bool, string)` in *include/settings/settings.h*
For example:
```
// CONFIG_int(name, description, default_value, is_mutable, is_persistent)
CONFIG_int(port, "Peloton port (default: 15721)", 15721, false, false)
```
The macro defined in *include/settings/settings_macro.h* will be exposed as four types of content on demand:
- a gflags definition
- a gflags declaration
- a member in SettingId 
- a statement of SettingsManager::DefineSetting

To get or set values, use the utility functions of `SettingsManager`.
For example:
`SettingsManager::GetInt(SettingId::port)`
`SettingsManager::SetBool(SettingId::index_tuner, true)`

### TODO
- Support more types, not only INTEGER, BOOLEAN, STRING
- Support validation check, I leave blank in min_value and max_value
temporarily.
