# Example Config

## Preamble

*tl;dr?* Note at least the title layout!


## Description

In order to keep the usage simple and the learning
curve flat while still offering quite some flexibility,
we decided to keep the documentation as close to
the code as possible.

This example config is meant to explain the options at hand.


## Config Structure

### Sections

#### data (mapping)

The *data* section is the central part and describes
the data points themselfes and their relation to the
other sections.
It is not defining any action by itself but acts as
a mapping hub.

#### input (action)

The *input* section defines information relevant for
collecting data from specified sources, like sensors,
tests, etc.

One input module group/type is *collect*.

It is read in *input* and *bridge* action.


#### output (action)

The *output* section defines information relevant for sending
data further to distant services like data brokers, APIs, etc.

One output module group/type is *send*.

It is read in *output* and *bridge* action.

#### storage (mode)

The *storage* section optionally defines cache and archive,
resp. junk folders to track the action process.


#### filter (mode)

The *filter* section may contain filtering information to
translate the data on the fly at three points during the
transition of the chain.


## Config File Options

### Config File Formats

#### YAML

In order to please the human user, the config can be written in YAML.

As there are currently two main YAML-implementations in Python and we
strive for maximum installation-free compatibility, both are supported.
If the program detects a file suffix of `.yml` or `.yaml`, it will
auto-detect which library is installed and load it.


#### JSON

We always support JSON, so even if no YAML happen to be installed on
your tiny system, just use JSON in your config with a file name ending
in `.json`. 


#### JSON-YAML

Of course, a config written in JSON but put e.g. into a file ending in
`.yml` would also not pose a problem, as YAML includes JSON syntax...


### Config File Organization

### Single File Config

The whole config can be put in one file.

In this case, every subsection is to be put into a separate map inside
the main map under the key named after the subsection.

See [`./file_example.json`](./single_file_example.json) for details.


### Split Directory/Files Config

The whole config can be split into multiple files.

If the config section is a skalar string, it is assumed to
represent the name of the file containing the config for its
resp. section.

The contents of the file will be merged into the main config
map as a separate map under the
key named after the subsection.

See [`./example.json`](./example.json) for details.
