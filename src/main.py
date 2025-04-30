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
    BridgingHubModuleRegistry,
    DuplicatedModuleException,
    NoSuchModuleException,
)

KEY_ACTION_TYPES = ["bridge", "collect", "filter", "send"]

KEY_ACTION_MODULE_NAME = "module_class_name"
KEY_ACTION_MODULE_PATH = "module_path"

# the default relative path for modules
DEFAULT_ACTION_MODULE_PATH = "module"


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


def run_module(action_name, config) -> bool:
    """Execute a module using the parameters found in the config.

    :param str action_name: The name of the action type from the registry
    :param dict config: The parameter map that was read from file
    :rtype: bool
    :return: Report success/failure and leave the rest to the caller"""
    try:
        # TODO testing here..
        BridgingHubModuleRegistry.register_module(
            config[action_name][KEY_ACTION_MODULE_NAME],
            config[action_name][KEY_ACTION_MODULE_PATH],
        )
        f = BridgingHubModuleRegistry.load_module(
            config[action_name][KEY_ACTION_MODULE_NAME]
        )
        f.configure(config)
        f.run()
        # TODO input -> filter -> output
    except (NoSuchModuleException, DuplicatedModuleException) as e:
        print(e)
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
        help="""Action type: `collect` and `send` are in-/output actions and
            are mainly for testing purposes, `bridge` binds an in- to an
            output(, and `filter` is like `bridge` but will allow a list
            of manipulations on the go - not implemented yet..).
            """,
        choices=KEY_ACTION_TYPES,
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
        if isinstance(cfg["input"], str):
            cfg["input"] = load_config(cfg["input"], cfg_dir)
        if isinstance(cfg["output"], str):
            cfg["output"] = load_config(cfg["output"], cfg_dir)
        if isinstance(cfg["data"], str):
            cfg["data"] = load_config(cfg["data"], cfg_dir)

        run_module("input", cfg)
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
