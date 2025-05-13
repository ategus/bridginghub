from abc import ABC, abstractmethod
from datetime import datetime, timezone
from importlib import import_module
from typing import Callable, Type, cast

from bridging_hub_types import (
    ConfigBaseType,
    ConfigDataType,
)


class DuplicatedModuleException(Exception):
    """
    Exception to be thrown on duplicated entries in the
    BridgingHubModuleRegistry.
    """

    pass


class NoSuchModuleException(Exception):
    """
    Exception to be thrown on missing entries in the
    BridgingHubModuleRegistry.
    """

    pass


class BrokenConfigException(Exception):
    """
    Exception to be thrown on invalid or missing content
    in the config file(s).
    """

    pass


class InputModuleException(Exception):
    """
    Exception to be thrown on errors while reading from input.
    """

    pass


class OutputModuleException(Exception):
    """
    Exception to be thrown on errors while writing to output.
    """

    pass


class FilterModuleException(Exception):
    """
    Exception to be thrown on errors while filtering messages.
    """

    pass


class StorageModuleException(Exception):
    """
    Exception to be thrown on errors while writing to disk.
    """

    pass


class BridgingHubBaseModule(ABC):
    """
    Abstract base class for all action modules.
    """

    # display generic keywords and prevent typos etc.
    KEY_BRIDGE: str = "bridge"
    KEY_CLEANUP: str = "cleanup"
    KEY_DATA: str = "data"
    KEY_INPUT: str = "input"
    KEY_OUTPUT: str = "output"
    KEY_FILTER: str = "filter"
    KEY_STORAGE: str = "storage"

    # these are actions a user can choose from at runtime from the cli
    KEY_ACTION_TYPES: list[str] = [
        KEY_BRIDGE,
        KEY_CLEANUP,
        KEY_INPUT,
        KEY_OUTPUT,
    ]

    # keys refering to module loading
    KEY_ACTION_MODULE_NAME: str = "module_class_name"
    KEY_ACTION_MODULE_PATH: str = "module_path"

    # these are keys used in the _custom_name map and are ment
    # to guarantee the accessibilty between different modulse
    KEY_BH_STATUS_NAME: str = "bHstatus_name"
    KEY_DATA_VALUE_MAP: str = "value_register_map"
    KEY_GEOHASH_NAME: str = "geohash_name"
    KEY_ID_NAME: str = "id_name"
    KEY_LOCATION_NAME: str = "location_name"
    KEY_TIMESTAMP_NAME: str = "timestamp_name"
    KEY_TYPE_NAME: str = "type_name"
    KEY_VALUE_NAME: str = "value_name"
    KEY_UNIT_NAME: str = "unit_name"

    # the default relative path for modules"
    DEFAULT_ACTION_MODULE_PATH: str = "module"

    # The action type this modules belongs to.
    # These are the (possible) candidates:
    # * collect (standard - synchronous input action)
    # * consume (pending - asynchronous input action)
    # * send (standard = synchronous output action)
    # * produce (pending - asynchronous output action)
    # * bridge (standard - connecting in-/output)
    # * filter (pending - as bridge but edit content)
    action_type: str = ""

    def __init__(self) -> None:
        # Store the config details related to the action type.
        self._action_detail: dict[str, str] = {}
        # This will be filled with the KEY_DATA_VALUE_MAP on run init
        # We need to keep this (and the next) dict separate for the
        # modules even if the content MUST be compatible inside of
        # one run, and mostly actually is identical...
        self._data: ConfigDataType = {}
        # For a minimal compatibility between the modules, the basic data
        # structure is defined and filled with defaults. This will be
        # overwritten by the config parameters. Keeping a map of customizable
        # names allows for a translation beween different modules if
        # necessary.
        self._custom_name: ConfigBaseType = {
            BridgingHubBaseModule.KEY_BH_STATUS_NAME: "bHstatus",
            BridgingHubBaseModule.KEY_GEOHASH_NAME: "geohash",
            BridgingHubBaseModule.KEY_ID_NAME: "id",
            BridgingHubBaseModule.KEY_LOCATION_NAME: "location",
            BridgingHubBaseModule.KEY_TIMESTAMP_NAME: "timestamp",
            BridgingHubBaseModule.KEY_TYPE_NAME: "type",
            BridgingHubBaseModule.KEY_VALUE_NAME: "value",
            BridgingHubBaseModule.KEY_UNIT_NAME: "unit",
        }

    @abstractmethod
    def input(self) -> dict[str, dict[str, str]]:
        """
        Abstract input method.

        :rtype: dict
        :return: data set
        :raise BrokenConfigException:
        """
        pass

    @abstractmethod
    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """
        Abstract output method.

        :param message: data set as dict
        :rtype: dict
        :return: data (sub-)set processed
        :raise BrokenConfigException:
        """
        pass

    def configure(self, config: dict) -> None:
        """Configure the module with the specific config. This is intended
        to happen after Object creation.
        :param dict config:
        :raise BrokenConfigException:"""
        # init the _data
        if (
            BridgingHubBaseModule.KEY_DATA in config
            and config[BridgingHubBaseModule.KEY_DATA]
        ):
            self._data = config[BridgingHubBaseModule.KEY_DATA]
            # set the custom names from config
            for k in self._custom_name.keys():
                if k in self._data and self._data[k]:
                    if not isinstance(self._data[k], str):
                        raise BrokenConfigException(
                            f"The 'data:{k}' config parameter MUST be a 'str'"
                        )
                    self._custom_name[k] = str(self._data[k])
        else:
            raise BrokenConfigException(
                "No 'data' section found in the config."
            )
        if self.action_type in config and config[self.action_type]:
            self._action_detail = config[self.action_type]
        else:
            raise BrokenConfigException(
                f"No '{self.action_type}' section found in the config."
            )

    def current_timestamp(self) -> int:
        """Generate a timestamp in nano seconds since Unix Epoch.
        :rtype: int
        :return: nano second timestamp a.U."""
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return int((now - epoch).total_seconds() * 1000000000)


class InputBaseModule(BridgingHubBaseModule):
    """
    Abstract base module for all input modules.
    """

    action_type: str = BridgingHubBaseModule.KEY_INPUT

    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """
        Fake output method in input module.

        :param message: data set as dict
        :rtype: dict
        :return: data (sub-)set processed
        :raise BrokenConfigException:
        """
        raise BrokenConfigException(
            "This output module was configured in input."
        )
        return {}


class OutputBaseModule(BridgingHubBaseModule):
    """
    Abstract base module for all input modules.
    """

    action_type: str = BridgingHubBaseModule.KEY_OUTPUT

    def input(self) -> dict[str, dict[str, str]]:
        """
        Fake input method in output module.

        :rtype: dict
        :return: data set
        :raise BrokenConfigException:
        """
        raise BrokenConfigException(
            "This input module was configured in output."
        )
        return {}


class CollectorBaseModule(InputBaseModule):
    """
    Abstract base module used by modules collecting data.
    """

    def input(self) -> dict[str, dict[str, str]]:
        """
        Default input method for collector modules.

        :rtype: dict
        :return: data set
        :raise BrokenConfigException:
        """
        return self.collect()

    @abstractmethod
    def collect(self) -> dict[str, dict[str, str]]:
        """
        Abstract methods to be implemented to gather data info
        following the config data section and return a dict.

        :rtype: dict
        :return: data set
        :raise BrokenConfigException:
        """
        pass


class ConsumerBaseModule(InputModuleException):
    """
    Abstract base module used by modules optionally connected to
    data streams.
    """

    def __init__(self) -> None:
        self.subscription: list[Callable] = []

    @abstractmethod
    def consume(self) -> None:
        """
        Abstract method used to connect to an external service.
        """
        pass

    def subscribe(self, call: Callable) -> None:
        """
        Any method interested can subscribe here in order to receive
        the data gathered in this input module.
        """
        self.subscription.append(call)

    def on_data(self, data: dict[str, dict[str, str]]) -> None:
        """
        On data input, call this method to pass the data to all
        parties interested.

        :param data: single data element as dict
        :rtype: dict
        """
        for c in self.subscription:
            c(data)


class SenderBaseModule(OutputBaseModule):
    """
    Abstract base sender module used by output modules.
    """

    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """
        Default output method in sender module.

        :param message: data set as dict
        :rtype: dict
        :return: data (sub-)set processed
        :raise BrokenConfigException:
        """
        return self.send(message)

    @abstractmethod
    def send(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """
        Abstract method to be implemented to send data info
        following the config data section and return the
        dict of the data really sent away.

        :param message: data set as dict
        :rtype: dict
        :return: data (sub-)set processed
        :raise BrokenConfigException:
        """
        pass


class FilterBaseModule(BridgingHubBaseModule):
    """
    Abstract base filter module.
    """

    action_type: str = BridgingHubBaseModule.KEY_FILTER

    def input(self) -> dict[str, dict[str, str]]:
        raise BrokenConfigException(
            "This filter module was configured in input."
        )
        return {}

    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        raise BrokenConfigException(
            "This filter module was configured in output."
        )
        return {}

    @abstractmethod
    def filter(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Filter content between storage, in-, and output."""
        pass


class StorageBaseModule(BridgingHubBaseModule):
    """
    Abstract base storage module. There is only one type of storage
    module usable at a time.
    """

    action_type: str = BridgingHubBaseModule.KEY_STORAGE

    def input(self) -> dict[str, dict[str, str]]:
        raise BrokenConfigException(
            "This storage module was configured in input."
        )
        return {}

    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        raise BrokenConfigException(
            "This storage module was configured in output."
        )
        return {}

    @abstractmethod
    def write_cache(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Remember message content between in- and output."""
        pass

    @abstractmethod
    def read_cache(self) -> dict[str, dict[str, str]]:
        """Look up message content between in- and output."""
        pass

    @abstractmethod
    def clean_cache(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Clean up the files remembered between in- and output."""
        pass

    @abstractmethod
    def store(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Remember message content after output."""
        pass


RegistryType = dict[str, dict[str, str | BridgingHubBaseModule]]


class BridgingHubModuleRegistry:
    """
    Registry class for dynamic module access.
    """

    KEY_MOD_PATH = "module_path"
    KEY_MOD_NAME = "module_name"
    KEY_MOD_OBJ = "module_object"

    _registry: RegistryType = {}

    @classmethod
    def register_module(
        cls, segment_name: str, module_class_name: str, module_path: str
    ) -> BridgingHubBaseModule:
        """Register a method in the table for later.

        :param str segment_name: The name of the segment in the chain.
        :param str module_class_name: The name of the class.
        :param str module_path: The path of the python module.
        :rtype: BridgingHubBaseModule
        :return: The registered BridgingHubBaseModule
        :raise NoSuchModuleException: (pass on)"""
        n: str = segment_name + module_class_name
        if n not in cls._registry:
            cls._registry[n] = {
                BridgingHubModuleRegistry.KEY_MOD_PATH: module_path
            }
            cls._registry[n][
                BridgingHubModuleRegistry.KEY_MOD_NAME
            ] = module_class_name
        return cls.load_module(segment_name, module_class_name)

    @classmethod
    def load_module(
        cls, segment_name: str, module_class_name: str
    ) -> BridgingHubBaseModule:
        """Look up and return a new instance of a registered module.

        :param str segment_name: The name of the segment in the chain.
        :param str module_class_name: The name of the module as key.
        :rtype: a subclass of BridgingHubBaseModule
        :return: The class of the module.
        :raise NoSuchModuleException:"""
        n: str = segment_name + module_class_name
        # Test if the module was indeed registered
        if n not in cls._registry or not cls._registry[n]:
            raise NoSuchModuleException(
                f"There is no module registered by the name of \
'{module_class_name}'."
            )
        # Test if the module was loaded before..
        if not (
            BridgingHubModuleRegistry.KEY_MOD_OBJ in cls._registry[n]
            and cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_OBJ]
        ):
            module_path: str = str(
                cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_PATH]
            )
            try:
                pymod = import_module(module_path)
            except ModuleNotFoundError as e:
                raise NoSuchModuleException(
                    f"Module '{module_class_name}' not found. {e}"
                )
            except ImportError as e:
                raise NoSuchModuleException(f"Import error: {e}")
            except SyntaxError as e:
                raise NoSuchModuleException(
                    f"Syntax error in module '{module_class_name}': {e}"
                )
            except Exception as e:
                raise NoSuchModuleException(
                    f"""An unexpected error occurred in '{module_class_name}'\
: {e}"""
                )
            module_class: Type = getattr(pymod, module_class_name)
            if not issubclass(module_class, BridgingHubBaseModule):
                raise NoSuchModuleException(
                    f"An incompatible module ('{module_class}') was \
registered for '{module_path}.'"
                )
            else:
                cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_OBJ] = cast(
                    BridgingHubBaseModule, module_class()
                )
        if isinstance(
            cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_OBJ],
            BridgingHubBaseModule,
        ):
            return cast(
                BridgingHubBaseModule,
                cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_OBJ],
            )
        else:
            t = type(cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_OBJ])
            raise NoSuchModuleException(
                f"Unexpected module type found for {n}: {t}."
            )
