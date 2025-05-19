import json
import logging
from typing import cast

import requests

from bridging_hub_module import SenderBaseModule


class PostRequestSender(SenderBaseModule):
    """
    Post data to a web server and return the result.
    """

    KEY_HOST_URL: str = "host_url"
    KEY_HOST_PORT: str = "host_port"
    KEY_SUCCESS: str = "expected_retval"
    KEY_VERIFY_CERT: str = "verify_certificate"
    KEY_BASIC_USERNAME: str = "basic_username"
    KEY_BASIC_PASSWORD: str = "basic_password"

    # used to select fields and rename them for sending
    KEY_SELECT: str = "select_send_as"

    # currently only json form data is implemneted
    supported_content_type: list[str] = ["application/json"]

    def __init__(self, name: str = "") -> None:
        """Instanciate the object.

        :param name: obtional name"""
        super().__init__(name)
        # register config settings
        self._send_as: dict[str, str] = {}

    def configure(self, config: dict) -> None:
        """Configure the module with the specific config. This is intended
        to happen after Object creation.
        :param dict config:
        :raise BrokenConfigException:"""
        super().configure(config)

        # register with dict type and store in object for quick access
        self._send_as = cast(
            dict[str, str],
            self._action_detail.pop(PostRequestSender.KEY_SELECT),
        )

    def send(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Send data to standard out.

        :param message: data set as dict
        :rtype: dict
        :return: data processed"""
        logging.debug(f"bhm.send({message})")
        m: dict[str, dict[str, str]] = {}
        for k, v in message.items():

            # create a new data map, only containing the selected fields
            nv: dict[str, str] = {}
            for kk, vv in v.items():
                if kk in self._send_as.keys():
                    nv[self._send_as[kk]] = vv
                else:
                    nv[kk] = vv

            # translate into a json data map
            j = json.dumps(nv)

            logging.debug(f"Sending data to host: {j}")
            response = requests.post(
                self._action_detail[PostRequestSender.KEY_HOST_URL],
                data=j,
                auth=(
                    self._action_detail[PostRequestSender.KEY_BASIC_USERNAME],
                    self._action_detail[PostRequestSender.KEY_BASIC_PASSWORD],
                ),
                headers={"Content-Type": "application/json"},
            )

            # mark done, if the request was successful
            if response.status_code == int(
                self._action_detail[PostRequestSender.KEY_SUCCESS]
            ):
                logging.info(
                    f"Data successfully sent to host: {response.status_code}"
                )
                m[k] = v
                m[k][
                    self._custom_name[PostRequestSender.KEY_BH_STATUS_NAME]
                ] = "out"
            else:
                logging.info(
                    f"""Data NOT sent to host: {response.status_code} \
{response.text}"""
                )
        return m
