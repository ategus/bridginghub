#!/usr/bin/env python3
# using argparse as we want to keep dependencies minimal
import argparse

# import logging
# TODO use logfile on demand..
import os

# import shutil
import sys
from os.path import join
from typing import cast

from bridging_hub_module import (
    BridgingHubBaseModule,
    BridgingHubModuleRegistry,
    BrokenConfigException,
    DuplicatedModuleException,
    InputModuleException,
    NoSuchModuleException,
    StorageBaseModule,
)
from bridging_hub_types import (
    ConfigDataType,
    ConfigSubType,
    ConfigType,
)


class IllegalFileOperation(Exception):
    """Custom Error to be raised on illegal file operations in order
    to treat them uniformly."""

    pass


class ModuleLoaderException(Exception):
    """Marks a problem in connection with configuration and loading of
    modules."""

    pass


class ModulePipeException(Exception):
    """Failed to run the module pipe as requested."""

    pass


def load_config(
    filename, workdir=""
) -> ConfigType | ConfigSubType | ConfigDataType:
    """Load the configuration for actions and data from YAML/JSON file(s).

    :param str filename: The name of the file (mandatory)
    :rtype: dict
    :return: The config from YAML/JSON
    :raise IllegalFileOperation:"""
    j: dict = {}
    if not filename.startswith("/"):
        filename = join(workdir, filename)
    if filename[-5:] == ".json":
        import json

        try:
            with open(filename) as f:
                j = json.load(f)
        except Exception as e:
            raise IllegalFileOperation(
                f"""Could not read the config file {filename} - the \
problem was: {e}"""
            )
    if filename[-5:] == ".yaml" or filename[-4:] == ".yml":
        try:
            import ruamel.yaml as yaml  # type: ignore[import-untyped]
        except ImportError:
            try:
                import yaml  # type: ignore[import-untyped, no-redef]
            except ImportError:
                raise IllegalFileOperation(
                    """Install at least either 'ruamel.yaml' or 'yaml' \
(PyYAML) or use JSON configs."""
                )
        try:
            with open(filename) as f:
                j = yaml.safe_load(f)
        except Exception as e:
            raise IllegalFileOperation(
                f"""Could not read the config file {filename} - \
the problem was: {e}"""
            )
    return j


def load_module(
    config: ConfigType, action_name: str, segment_name: str = "default"
) -> BridgingHubBaseModule:
    """Wrapper to load all modulues uniformely (and no-brain-ly).

    :param config config: The parameter map that was read from file
    :param str action_name: The name/type to register the module with
    :param str segment: The name of the pipe segment
    :rtype: BridgingHubBaseModule
    :return: Return the requested module
    :raise: ModuleLoaderException"""
    try:
        ac = config[action_name]
        assert isinstance(
            ac, dict
        ), f"Expected action_config to \
be a dictionary, but got {type(ac)}"

        mn = ac[BridgingHubBaseModule.KEY_ACTION_MODULE_NAME]
        mp = ac[BridgingHubBaseModule.KEY_ACTION_MODULE_PATH]
        assert isinstance(
            mn, str
        ), f"""Expected module_name to be a string, \
but got {type(mn)}"""
        assert isinstance(
            mp, str
        ), f"""Expected module_path to be a string, \
but got {type(mp)}"""

        m = BridgingHubModuleRegistry.register_module(segment_name, mn, mp)
        if m.action_type != action_name:
            raise BrokenConfigException(
                f"""The config for '{action_name}' holds a module of type \
'{m.action_type}'."""
            )
        m.configure(config)
        return m
    except (
        AssertionError,
        DuplicatedModuleException,
        NoSuchModuleException,
        BrokenConfigException,
    ) as e:
        raise ModuleLoaderException(f"Unable to load module: {e}")


def run_module_pipe(action_name, config) -> bool:
    """Execute the modules as defined by the config file.

    :param str action_name: The name of the action type from the registry
    :param dict config: The parameter map that was read from file
    :rtype: bool
    :return: Report success/failure and leave the rest to the caller
    :raise: ModulePipeException"""
    print(config)
    for pipe_name, pipe_config in config.items():
        print(pipe_config["type"])
    return True


def run_module_pipe_old(action_name, config) -> bool:
    """Execute module using the parameters found in the config.

    :param str action_name: The name of the action type from the registry
    :param dict config: The parameter map that was read from file
    :rtype: bool
    :return: Report success/failure and leave the rest to the caller
    :raise: ModulePipeException"""
    # TODO change the logic here to strictly just following the order set
    # in the config, relying only on 'type' instead of the key name.
    try:
        # TODO testing here..
        # Load the storage module on request by config
        m: BridgingHubBaseModule | None = None
        s: BridgingHubBaseModule | None = None
        ri: dict[str, dict[str, str]] = {}
        rc: dict[str, dict[str, str]] = {}
        ro: dict[str, dict[str, str]] = {}
        re: dict[str, dict[str, str]] = {}
        if (
            BridgingHubBaseModule.KEY_STORAGE in config
            and config[BridgingHubBaseModule.KEY_STORAGE]
            and BridgingHubBaseModule.KEY_ACTION_MODULE_NAME
            in config[BridgingHubBaseModule.KEY_STORAGE]
            and config[BridgingHubBaseModule.KEY_STORAGE][
                BridgingHubBaseModule.KEY_ACTION_MODULE_NAME
            ]
        ):
            s = load_module(
                config, BridgingHubBaseModule.KEY_STORAGE, "default"
            )

        # Load input module on request by action or default to data
        if (
            action_name == BridgingHubBaseModule.KEY_BRIDGE
            or action_name == BridgingHubBaseModule.KEY_INPUT
        ):
            m = load_module(config, BridgingHubBaseModule.KEY_INPUT, "default")
            # get the infos from input
            ri = m.input()
            if "verbose" in config and config["verbose"] == "True":
                if ri:
                    print("Data from input: ", ri)
            if (
                config[BridgingHubBaseModule.KEY_DATA][
                    BridgingHubBaseModule.KEY_DATA_VALUE_MAP
                ].keys()
                != ri.keys()
            ):
                raise InputModuleException(
                    "Every module MUST make sure all configured data points \
do get a timestamp and a value.",
                )
            if isinstance(s, StorageBaseModule):
                # write input to cache and return cached items
                rc = s.write_cache(ri)
        elif action_name == BridgingHubBaseModule.KEY_OUTPUT:
            if isinstance(s, StorageBaseModule):
                # w/o input, just read from cache
                rc = s.read_cache()
            else:
                # w/o input and cache, just load the config
                ri = config[BridgingHubBaseModule.KEY_DATA]
        if "verbose" in config and config["verbose"] == "True":
            if rc:
                print("Data from cache: ", rc)
        if (
            action_name == BridgingHubBaseModule.KEY_BRIDGE
            or action_name == BridgingHubBaseModule.KEY_OUTPUT
        ):
            m = load_module(
                config, BridgingHubBaseModule.KEY_OUTPUT, "default"
            )
            if ri:
                ro = m.output(ri)
            elif rc:
                ro = m.output(rc)
            if isinstance(s, StorageBaseModule):
                # process current data mv timestamp_datakey.json from new
                # to junk or archive based on marker from output..
                re = s.store(ro)
            if "verbose" in config and config["verbose"] == "True":
                if ro:
                    print("Data from output:", ro)
                if re:
                    print("Data from storage:", re)
        elif action_name == BridgingHubBaseModule.KEY_CLEANUP:
            if isinstance(s, StorageBaseModule):
                pass  # cleanup
            else:
                raise BrokenConfigException(
                    "Action 'cleanup' only makes sense if \
'storage' is configured."
                )
        else:  # action_name == BridgingHubBaseModule.KEY_INPUT (no output)
            if ri:
                print("Data from input: ", ri)
            if rc:
                print("Data from cache: ", rc)

        return True
    # TODO: input->prefilter->cache->filter->output->postfilter->store
    except (ModuleLoaderException, InputModuleException) as e:
        raise ModulePipeException(f"Stopped running the module pipe: {e}")
    # TODO how shall we exit eventually? Clean up should be done in the
    # submodule..?!


if __name__ == "__main__":
    """Main function called if the command line was the entry point."""
    parser = argparse.ArgumentParser(
        description="""Modular approach to collect, handle and distribute
            measurement data."""
    )
    parser.add_argument(
        "action",
        help="""Action type: `collect` and `send` are in-/output only actions,
            while `bridge` binds an in- to an output channel (optionally
            storing and filtering the content on the go).
            """,
        choices=BridgingHubBaseModule.KEY_ACTION_TYPES,
    )
    parser.add_argument(
        "-c",
        "--config",
        help="""Central config file (mandatory).""",
        required=True,
    )
    cwd = os.getcwd()
    parser.add_argument(
        "-w",
        "--workdir",
        help="""Top-level directory of the project for relative paths
            (default: `cwd`).""",
        default=cwd,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="""Be more explicit on what happens.""",
    )
    args = parser.parse_args()
    # the action defines the mode to run in
    action_name = args.action
    # args.workdir defaults to `cwd` if not set
    cfg_dir = args.workdir
    if not os.path.isdir(cfg_dir):
        print(f"Please provide a valid WORKDIR: {cfg_dir}")
        sys.exit(1)
    # initialize the config dictionary
    cfg: dict = {}
    # try to load the config context first
    try:
        if args.config:
            # get the config from the location provided by the user
            cfg = cast(ConfigType, load_config(args.config, cfg_dir))
        if "verbose" in cfg and cfg["verbose"]:
            if cfg["verbose"] != "True" or cfg["verbose"] != "False":
                raise BrokenConfigException(
                    "The 'verbose' parameter is reserved, see flag '--verbose'"
                )
        if args.verbose:
            cfg["verbose"] = "True"
        # the rest of the config may either also come from cli, or the
        # single or split config file(s)
        if BridgingHubBaseModule.KEY_INPUT not in cfg:
            raise ModulePipeException(
                "Please configure 'input' in the config file."
            )
        else:
            if isinstance(cfg[BridgingHubBaseModule.KEY_INPUT], str):
                cfg[BridgingHubBaseModule.KEY_INPUT] = cast(
                    ConfigSubType,
                    load_config(
                        cfg[BridgingHubBaseModule.KEY_INPUT],
                        cfg_dir,
                    ),
                )
        if BridgingHubBaseModule.KEY_OUTPUT not in cfg:
            raise ModulePipeException(
                "Please configure 'output' in the config file."
            )
        else:
            if isinstance(cfg[BridgingHubBaseModule.KEY_OUTPUT], str):
                cfg[BridgingHubBaseModule.KEY_OUTPUT] = cast(
                    ConfigSubType,
                    load_config(
                        cfg[BridgingHubBaseModule.KEY_OUTPUT],
                        cfg_dir,
                    ),
                )
        if BridgingHubBaseModule.KEY_STORAGE in cfg:
            if isinstance(cfg[BridgingHubBaseModule.KEY_STORAGE], str):
                cfg[BridgingHubBaseModule.KEY_STORAGE] = cast(
                    ConfigSubType,
                    load_config(
                        cfg[BridgingHubBaseModule.KEY_STORAGE],
                        cfg_dir,
                    ),
                )
        if BridgingHubBaseModule.KEY_FILTER in cfg:
            if isinstance(cfg[BridgingHubBaseModule.KEY_FILTER], str):
                cfg[BridgingHubBaseModule.KEY_FILTER] = cast(
                    ConfigSubType,
                    load_config(
                        cfg[BridgingHubBaseModule.KEY_FILTER],
                        cfg_dir,
                    ),
                )
        if BridgingHubBaseModule.KEY_DATA not in cfg:
            raise ModulePipeException(
                "Please configure 'data' in the config file."
            )
        else:
            if isinstance(cfg[BridgingHubBaseModule.KEY_DATA], str):
                cfg[BridgingHubBaseModule.KEY_DATA] = cast(
                    ConfigDataType,
                    load_config(
                        cfg[BridgingHubBaseModule.KEY_DATA],
                        cfg_dir,
                    ),
                )
        if (
            "_bridgingHub_major_version" in cfg
            and cfg["_bridgingHub_major_version"]
            and float(cfg["_bridgingHub_major_version"]) >= 1
        ):
            run_module_pipe(action_name, cfg)
        else:
            run_module_pipe_old(action_name, cfg)
        sys.exit(0)  # all done here..
    except IllegalFileOperation as e:
        print(f"Could not load the config: {e}")
        sys.exit(2)
    except ModulePipeException as e:
        print(f"The following error occurred: {e}")
        sys.exit(98)
