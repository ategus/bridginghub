# bridgingHub

## Status

The basic functionality is working, but it's

*NOT Production Ready*

## Description

**bridgingHub** connects different data endpoints in a flexible and modular
way.
It is designed to run with minimal dependencies while remaining portable
enough to operate on edge gateways, small VMs, or containers — in short,
any standard Python environment.

Its framework-like, extensible module infrastructure makes it easy to
configure and adapt to a wide range of use cases.

## Overview

The bridge character can be visualized by the folloing chain and its
corresponding module groups/types:

```
                    tmpSTORAGE                                  longSTORAGE
                    #write_cache                                #write_junk
                    #read_cache                                 #write_archive
        preFILTER               mainFILTER          postFILTER
        #jinja                  #jinja              #jinja
        #validate
INPUT                                       OUTPUT
#collect                                    #send
#consume                                    #produce
                                            #cleanup            .cleanup

                     ,--->#>---.                                ,-->#
   ,------->::>-----+----------+---->::>----+------->::>-------'
>-'                                          '-->
```

## Setup

### Requirements

#### Minimal Setup

  * python >=3.10

#### User Installation

  * python >=3.10
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

##### CI/CD

Every major release will have its own branch, so those will always be
*stable* and can be used for CI/CD.


##### Release Packages

We will probably generate at least signed and checksumed release archives.



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


##### Branches

The `main` branch **Must** always be *working*, i.e. new (all) developers
will be able to use and merge the branch w/o first having to get a running
basis.

New code will always go through as 'merge-request'.
Forks and development branches are our friends.


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
./src/main.py -c config/example.yml bridge
```

Adjust the path and command according to your configuration and modules.
**NOTE:** The example config will write to your '/tmp/' folder.


## License

This software is released under the [*GPL 3+*](./LICENSE).

