ConfigBaseType = dict[str, str]
ConfigSubType = dict[str, str | ConfigBaseType | list[str]]
ConfigDataType = dict[str, str | dict[str, ConfigSubType]]

# Define a type hint for the config
ConfigType = (
    ConfigBaseType
    | dict[str, str | ConfigSubType]
    | dict[str, str | ConfigDataType]
)
