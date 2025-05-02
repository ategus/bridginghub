from abc import ABC, abstractmethod
from datetime import datetime, timezone
from importlib import import_module
from typing import Type


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
    Exception to be thrown on errors while reading input.
    """

    pass


class BridgingHubBaseModule(ABC):
    """
    Abstract base class for all action modules.
    """

    # The action type this modules belongs to.
    # These are the (possible) candidates:
    # * collect (standard - synchronous input action)
    # * consume (pending - asynchronous input action)
    # * send (standard = synchronous output action)
    # * produce (pending - asynchronous output action)
    # * bridge (standard - connecting in-/output)
    # * filter (pending - as bridge but edit content)
    _action_type: str = ""

    # For a minimal compatibility between the modules, the basic data
    # structure is defined and filled with defaults. This will be
    # overwritten by the config parameters. Keeping a map of customizable
    # names allows for a translation beween different modules if necessary.
    _custom_name: dict[str, str] = {
        "id_name": "id",
        "value_name": "value",
        "timestamp_name": "timestamp",
        "geohash_name": "geohash",
        "location_name": "location",
    }

    # This will be filled with the 'value_register_map' on run init and
    # the values on run time.
    _data: dict[str, dict] = {}

    # Store the config details related to the action type.
    _action_detail: dict[str, str] = {}

    @abstractmethod
    def run(self) -> dict:
        pass

    def configure(self, config: dict) -> None:
        """Configure the module with the specific config. This is intended
        to happen after Object creation.
        :param dict config:
        :raise BrokenConfigException:"""
        # set the custom names from config
        # TODO
        for k in self._custom_name.keys():
            if k in config and config[k]:
                self._custom_name = config[k]
        # init the _data from value_register_map for adding values later
        if "data" in config and config["data"]:
            self._data = config["data"]
        else:
            raise BrokenConfigException(
                "No 'data' section found in the config."
            )
        if self._action_type in config and config[self._action_type]:
            self._action_detail = config[self._action_type]
        else:
            raise BrokenConfigException(
                f"No '{self._action_type}' section found in the config."
            )

    def current_timestamp(self) -> int:
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return int((now - epoch).total_seconds() * 1000000000)


class BridgingHubModuleRegistry:
    """
    Registry class for dynamic module access.
    """

    _registry: dict[str, str] = {}

    @classmethod
    def register_module(cls, module_class_name: str, module_path: str):
        """Register a method in the table for later.

        :param str module_class_name: The (unique) name of the class.
        :param str module_path: The path of the python module.
        :raise DuplicatedModuleException:"""
        if module_class_name not in cls._registry:
            cls._registry[module_class_name] = module_path
        elif cls._registry[module_class_name] != module_path:
            raise DuplicatedModuleException(
                f"""This module ({module_class_name}) was already
                registered for '{cls._registry[module_class_name]}'"""
            )

    @classmethod
    def load_module(cls, module_class_name: str) -> BridgingHubBaseModule:
        """Look up and return a new instance of a registered module.

        :param str module_class_name: The name of the module as key.
        :rtype: a subclass of BridgingHubBaseModule
        :return: The class of the module.
        :raise NoSuchModuleException:"""
        if module_class_name not in cls._registry:
            raise NoSuchModuleException(
                f"There is no module by name '{module_class_name}' registered."
            )
        module_path = cls._registry[module_class_name]
        module = import_module(module_path)
        module_class: Type = getattr(module, module_class_name)
        if not issubclass(module_class, BridgingHubBaseModule):
            raise NoSuchModuleException(
                f"An incompatible module was registered for '{module_path}.'"
            )
        return module_class()


class CollectorBaseModule(BridgingHubBaseModule):
    """
    Abstract base collector module used by input modules.
    """

    _action_type: str = "input"

    def run(self):
        return self.collect()

    @abstractmethod
    def collect(self) -> dict[str, dict[str, str]]:
        pass


class SenderBaseModule(BridgingHubBaseModule):
    """
    Abstract base sender module used by output modules.
    """

    _action_type: str = "output"

    def run(self):
        return self.send()

    @abstractmethod
    def send(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        pass


class StorageBaseModule(BridgingHubBaseModule):
    """
    Abstract base storage module. There is only one type of storage modules.
    """

    _action_type: str = "storage"

    def run(self):
        """Not used here."""
        pass

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
