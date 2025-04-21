from abc import ABC, abstractmethod
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


class BridgingHubBaseModule(ABC):
    """
    Abstract base class for all action modules.
    """

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def configure(self, config: dict):
        """Configure the module with the specific config.
        TODO: We might later want to optionally verify it against a schema
        :param dict config:"""
        pass


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
                f"""There was an incompatible module registered for
                '{module_path}'"""
            )
        return module_class()


class CollectorBaseModule(BridgingHubBaseModule):
    """
    Abstract base collector module that can be extended.
    """

    def run(self):
        self.collect()

    @abstractmethod
    def collect(self):
        pass


class SenderBaseModule(BridgingHubBaseModule):
    """
    Abstract base sender module that can be extended.
    """

    def run(self):
        self.send()

    @abstractmethod
    def send(self):
        pass
