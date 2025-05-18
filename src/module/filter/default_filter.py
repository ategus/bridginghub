# import json
import logging

# from jinja2 import Template, Environment
from jinja2 import Environment

from bridging_hub_module import (
    FilterBaseModule,
    FilterModuleException,
)
from bridging_hub_types import (
    ConfigSubType,
)

# from jinja2.exceptions import (
#    FilterArgumentError,
#    TemplateError,
#    TemplateSyntaxError,
#    UndefinedError,
#    TemplateRuntimeError,
#    TemplateAssertionError,
#    SecurityError,
# )


class DefaultFilter(FilterBaseModule):
    """
    Message content can be filtered and enriched by config values on the go.
    """

    KEY_PREDEFINED: str = "predefined_filter"
    KEY_TEMPLATE: str = "jinja_template"

    # available predefined filters
    KEY_MERGE_DICTS: str = "merge_message_with_config"
    KEY_ADD_DATETIME: str = "add_datetime"

    def __init__(self, name: str = "") -> None:
        """Instanciate the object.

        :param name: obtional name"""
        super().__init__(name)
        #        self._filter_available: list = [
        #            DefaultFilter.KEY_MERGE_DICTS,
        #            DefaultFilter.KEY_ADD_DATETIME
        #        ]
        # prepare filter lists
        # create a Jinja2 environment
        self.env = Environment()

    def configure(self, config: dict) -> None:
        super().configure(config)

        # make filter info more accessible
        self._predefined: ConfigSubType = self._action_detail.pop(
            DefaultFilter.KEY_PREDEFINED
        )
        logging.debug(
            f"  * \
{0 if not self._predefined else len(self._predefined)} predefined \
filters added."
        )
        self._template: ConfigSubType = self._action_detail.pop(
            DefaultFilter.KEY_TEMPLATE
        )
        logging.debug(
            f"  * \
{0 if not self._template else len(self._template)} custom \
jinja templates added."
        )
        # register the custom filter
        self.env.filters["dict_to_items"] = self._dict_to_items_filter

    def _merge_message_with_config_filter(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Custom Filter to merge parameters from data and config.
        the message content always takes precedence over the config.

        :param message: the data set to store
        :rtype: dict[str, dict[str, str]]
        :return: the merged message
        :raise FilterModuleException:"""
        logging.debug(f"bhm._merge_message_with_config_filter({message})")
        m: dict[str, dict[str, str]] = {}
        try:
            for k, v in message.items():
                m[k] = v
                for kk, vv in self._data[DefaultFilter.KEY_DATA_VALUE_MAP][
                    k
                ].items():
                    m[k][kk] = vv
        except Exception as e:
            raise FilterModuleException(
                f"""Could not load the filter data: {e}"""
            )
        return m

    def _add_datetime(
        self,
        message: dict[str, dict[str, str]],
        datetimename: str = "",
        form: str = "",
    ) -> dict[str, dict[str, str]]:
        """Custom Filter to merge parameters from data and config.

        :param message: the data set to store
        :rtype: dict[str, dict[str, str]]
        :return: the merged message
        :raise FilterModuleException:"""
        logging.debug(f"bhm._add_datetime({message},{datetimename},{form})")
        m: dict[str, dict[str, str]] = {}
        dt = self.current_datetimestr(form)
        try:
            for k, v in message.items():
                m[k] = v
                m[k][datetimename] = dt
        except Exception as e:
            FilterModuleException(f"""Could not load the filter data: {e}""")
        return m

    def _to_dict_filter(self, generator):
        """Custom Jinja2 Filter to convert a generator object in jinja
        to a dict."""
        return dict(generator)

    def _dict_to_items_filter(self, dictionary):
        """Custom Jinja2 filter to onvert a dictionary to a list of items."""
        return [{"key": k, "value": v} for k, v in dictionary.items()]

    def filter(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Use predefined or custom filters as requested by the
        config in order to manipulate the data on the fly.

        :param message: the sent data to be filtered
        :rtype: dict[str, dict[str, str]]
        :return: the messages processed
        :raise: FilterModuleException"""
        m: dict[str, dict[str, str]] = message.copy()

        # TODO shall we filter if input is empty? except maybe by config?
        logging.debug("Starting filters.")
        for f in self._predefined:
            fname, fparams = f[:-1].split("(")
            logging.debug(f"Found: {fname}: {m}")
            if fname == DefaultFilter.KEY_MERGE_DICTS:
                m = self._merge_message_with_config_filter(m)
                logging.debug(f"Passed through '{fname}: {m}")
            elif fname == DefaultFilter.KEY_ADD_DATETIME:
                pname, form = fparams.split("=")
                m = self._add_datetime(m, pname, form)
                logging.debug(f"Passed through '{fname}: {m}")

        # TODO process self._template
        return m
