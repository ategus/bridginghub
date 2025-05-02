#!/usr/bin/env python3
# using argparse as we want to keep dependencies minimal
import argparse

# import logging
# TODO use logfile on demand..
import os

# import shutil
import sys
from os.path import join

from bridging_hub_module import (
    BridgingHubBaseModule,
    BridgingHubModuleRegistry,
    DuplicatedModuleException,
    NoSuchModuleException,
    StorageBaseModule,
)


class IllegalFileOperation(Exception):
    """Custom Error to be raised on illegal file operations in order
    to treat them uniformly."""

    pass


def load_config(filename, workdir="") -> dict:
    """Load the configuration for actions and data from YAML/JSON file(s).

    :param str filename: The name of the file (mandatory)
    :rtype: dict
    :return: The config from YAML/JSON
    :raise IllegalFileOperation:"""
    j = {}
    if not filename.startswith("/"):
        filename = join(workdir, filename)
    if filename[-5:] == ".json":
        import json

        try:
            with open(filename) as f:
                j = json.load(f)
        except Exception as e:
            raise IllegalFileOperation(
                f"""Could not read the config file {filename} - the
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
                    """Install at least either 'ruamel.yaml' or 'yaml'
                    (PyYAML) or use JSON configs."""
                )
        try:
            with open(filename) as f:
                j = yaml.safe_load(f)
        except Exception as e:
            raise IllegalFileOperation(
                f"""Could not read the config file {filename} -
                the problem was: {e}"""
            )
    return j


def load_module(action_name, config) -> BridgingHubBaseModule:
    """Wrapper to load all modulues uniformely (and no-brain-ly).

    :param str action_name: The name/type to register the module with
    :param config config: The parameter map that was read from file
    :rtype: BridgingHubBaseModule
    :return: Return the requested module"""
    BridgingHubModuleRegistry.register_module(
        config[action_name][BridgingHubBaseModule.KEY_ACTION_MODULE_NAME],
        config[action_name][BridgingHubBaseModule.KEY_ACTION_MODULE_PATH],
    )
    m = BridgingHubModuleRegistry.load_module(
        config[action_name][BridgingHubBaseModule.KEY_ACTION_MODULE_NAME]
    )
    m.configure(config)
    return m


def run_module(action_name, config) -> bool:
    """Execute a module using the parameters found in the config.

    :param str action_name: The name of the action type from the registry
    :param dict config: The parameter map that was read from file
    :rtype: bool
    :return: Report success/failure and leave the rest to the caller"""
    try:
        # TODO testing here..
        # Load the storage module on request by config
        m: BridgingHubBaseModule | None = None
        s: BridgingHubBaseModule | None = None
        if (
            BridgingHubBaseModule.KEY_STORAGE in config
            and config[BridgingHubBaseModule.KEY_STORAGE]
            and BridgingHubBaseModule.KEY_ACTION_MODULE_NAME
            in config[BridgingHubBaseModule.KEY_STORAGE]
            and config[BridgingHubBaseModule.KEY_STORAGE][
                BridgingHubBaseModule.KEY_ACTION_MODULE_NAME
            ]
        ):
            s = load_module(BridgingHubBaseModule.KEY_STORAGE, config)
        print(action_name)
        # Load input module on request by action or default to data
        if (
            action_name == BridgingHubBaseModule.KEY_BRIDGE
            or action_name == BridgingHubBaseModule.KEY_INPUT
        ):
            m = load_module(BridgingHubBaseModule.KEY_INPUT, config)
            ri = m.run()
            if isinstance(s, StorageBaseModule):
                rc = s.write_cache(ri)
        else:
            ri = config[BridgingHubBaseModule.KEY_DATA]
            if isinstance(s, StorageBaseModule):
                rc = s.read_cache()
        ro = None
        if (
            action_name == BridgingHubBaseModule.KEY_BRIDGE
            or action_name == BridgingHubBaseModule.KEY_OUTPUT
        ):
            m = load_module(BridgingHubBaseModule.KEY_OUTPUT, config)
            ro = m.run()
        else:
            if ri:
                print("Info from input: ", ri)
            if rc:
                print("Info from cache: ", rc)
        if s:
            if ro:
                print("Info from output:", ro)
            pass

    # TODO:
    # input -> prefilter -> cache -> filter -> output -> postfilter -> store
    except (NoSuchModuleException, DuplicatedModuleException) as e:
        print(e)
        return False
    except AssertionError as e:
        print("Wrong type of module loaded", e)
        return False
    return True
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
    args = parser.parse_args()
    # the action defines the mode to run in
    action_name = args.action
    # args.workdir defaults to `cwd` if not set
    cfg_dir = args.workdir
    if not os.path.isdir(cfg_dir):
        print(f"Please provide a valid WORKDIR: {cfg_dir}")
        sys.exit(1)
    # initialize the config dictionary
    cfg = {}
    # try to load the config context first
    try:
        if args.config:
            # get the config from the location provided by the user
            cfg = load_config(args.config, cfg_dir)
        # the rest of the config may either also come from cli, or the
        # single or split config file(s)
        if isinstance(cfg[BridgingHubBaseModule.KEY_INPUT], str):
            cfg[BridgingHubBaseModule.KEY_INPUT] = load_config(
                cfg[BridgingHubBaseModule.KEY_INPUT], cfg_dir
            )
        if isinstance(cfg[BridgingHubBaseModule.KEY_OUTPUT], str):
            cfg[BridgingHubBaseModule.KEY_OUTPUT] = load_config(
                cfg[BridgingHubBaseModule.KEY_OUTPUT], cfg_dir
            )
        if isinstance(cfg[BridgingHubBaseModule.KEY_DATA], str):
            cfg[BridgingHubBaseModule.KEY_DATA] = load_config(
                cfg[BridgingHubBaseModule.KEY_DATA], cfg_dir
            )
        # TODO duplicate for storage
        # TODO duplicate for filter

        run_module(action_name, cfg)
        sys.exit(0)  # all done here..
    except IllegalFileOperation as e:
        print(e)
        sys.exit(2)
    except SystemExit as e:
        # TODO: after refactoring, raise and catch and exit from here now
        print(f"TODO... {e}")
        sys.exit(98)
    except Exception as e:
        print(f"Last resort error... {e}")
        sys.exit(99)
