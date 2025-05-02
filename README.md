# bridgingHub

## Status

* N O T   R E A D Y   Y E T ! *

## Description

**bridgingHub** connects different data endpoints in a flexible and modular
way.
It is designed to run with minimal dependencies while remaining portable
enough to operate on edge gateways, small VMs, or containers — in short,
any standard Python environment.

Its framework-like, extensible module infrastructure makes it easy to
configure and adapt to a wide range of use cases.


## Setup

### Requirements

#### Minimal Setup

  * python >3.10

#### User Installation

  * python >3.10
  * pip

#### Development

  * git
  * python >=3.10
  * optional(/recommended): venv
  * pip (see [requirements-dev.txt](./requirements-dev.txt))
    * pre-commit
    * black
    * isort
    * flake8
    * mypy
    * types-PyYAML


### Installation

#### For Quick and Brainless (minimal) Use

If you only want to *use* the software:

- Copy the folder structure and contents from [`src/`](./src)
- Optionally link [`src/main.py`](./src/main.py) to somewhere in your
  `PATH`, e.g.:

```bash
ln -s $PWD/src/main.py ~/bin/bridgingHub.py
```

You will then need to create configuration files to suit your setup.
See [Configuration](#configuration) for details.


#### Full Installation

*To be added.*


#### For Development

You may want to isolate your development environment using `venv`,
`virtualenv`, or a similar tool.

Once you've set up your Python environment, install development requirements
and enable Git hooks:

```bash
pip install -r requirements-dev.txt
pre-commit install
```

Then configure the project for your use case — see
[Configuration](#configuration).


### Configuration

Example configurations can be found in the
[`config/`](./config/) directory,
the documentation in [`EXAMPLE.md`](./config/EXAMPLE.md).

You can define a `module_path:` in your config to load custom
modules from your own Python path.


## Testing

*To be added.*


## Modules

You can develop your own modules and place them anywhere in your Python path.
To use them, specify their location using the `module_path:` key in your
configuration file.

This allows you to extend the system with domain-specific logic, integrations,
or transformations — without modifying the core.



## Usage

Run the tool using:

```bash
./src/main.py -c config/example.yml collect
```

Adjust the path and command according to your configuration and modules.


## License

This software is released under the [*GPL 3+*](./LICENSE).

