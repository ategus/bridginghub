import logging
import math
import time
from struct import unpack
from typing import Literal, cast

# from pymodbus import ModbusException
from pymodbus.client import (
    ModbusBaseClient,
    ModbusSerialClient,
    ModbusTcpClient,
)
from pymodbus.exceptions import (
    ConnectionException,
    ModbusException,
    ModbusIOException,
)

from bridging_hub_module import (
    CollectorBaseModule,
    InputModuleException,
)


class ModbusClientException(Exception):
    """
    Exception that indicates a problem setting up the Modbus client.
    """

    pass


class ModbusCollector(CollectorBaseModule):
    """
    Input module that combines info from Stdin with data configuration.
    """

    KEY_MODBUS_MODE = "modbus_mode"
    KEY_MODBUS_RTU_DEVICE = "modbus_rtu_port"
    KEY_MODBUS_RTU_TIMEOUT = "modbus_rtu_timeout"
    KEY_MODBUS_RTU_BAUDRATE = "modbus_rtu_baudrate"
    KEY_MODBUS_RTU_STOPBIT = "modbus_rtu_stopbit"
    KEY_MODBUS_RTU_BYTESIZE = "modbus_rtu_bytesize"
    KEY_MODBUS_RTU_PARITY = "modbus_rtu_parity"
    KEY_MODBUS_TCP_HOST = "modbus_tcp_host"
    KEY_MODBUS_TCP_PORT = "modbus_tcp_port"
    KEY_MODBUS_DEFAULT_DATA_TYPE = "modbus_default_data_type"
    KEY_MODBUS_DEFAULT_BYTE_ORDER = "modbus_default_byte_order"
    KEY_MODBUS_MAX_CONNECTION_RETRIES = "max_connection_retries"

    # Customizable Data Section Config Keys
    KEY_MODBUS_ADDRESS_NAME = "modbus_address_name"
    KEY_MODBUS_COUNT_NAME = "modbus_count_name"
    KEY_MODBUS_DATA_TYPE_NAME = "modbus_data_type_name"
    KEY_MODBUS_BYTE_ORDER_NAME = "modbus_byte_order_name"

    def __init__(self, name: str = "") -> None:
        """Instanciate the object.

        :param name: obtional name"""
        super().__init__(name)
        self._custom_name[ModbusCollector.KEY_MODBUS_ADDRESS_NAME] = (
            "modbus_address"
        )
        self._custom_name[ModbusCollector.KEY_MODBUS_COUNT_NAME] = (
            "modbus_count"
        )
        self._custom_name[ModbusCollector.KEY_MODBUS_DATA_TYPE_NAME] = (
            "modbus_data_type"
        )
        self._custom_name[ModbusCollector.KEY_MODBUS_BYTE_ORDER_NAME] = (
            "modbus_byte_order"
        )

    def _convert_byte_registers_to_value(self, register_array) -> float | int:
        if len(register_array) < 2:
            raise ValueError(
                """At least two register values are required for conversion."""
            )
        # TODO self._custom_name[ModbusCollector.KEY_MODBUS_BYTE_ORDER_NAME]
        byte_order = cast(
            Literal["little", "big"],
            self._action_detail[
                ModbusCollector.KEY_MODBUS_DEFAULT_BYTE_ORDER
            ].lower(),
        )
        # Determine byte order for packing and unpacking
        byte_order_char: str = ">"
        if byte_order == "little":
            byte_order_char = "<"
        elif not byte_order == "big":
            raise ValueError("Unsupported endian type. Use 'little' or 'big'.")

        byte_order_char = "<" if byte_order == "little" else ">"

        # Convert registers to bytes
        hex1 = (int(register_array[0])).to_bytes(2, byteorder=byte_order)
        hex2 = (int(register_array[1])).to_bytes(2, byteorder=byte_order)

        # Combine bytes
        combined_bytes = (
            hex2 + hex1
        )  # TODO Order of hex2 and hex1 may depend on endianness

        # TODO self._custom_name[ModbusCollector.KEY_MODBUS_DATA_TYPE_NAME]
        data_type = cast(
            Literal["float", "int"],
            self._action_detail[
                ModbusCollector.KEY_MODBUS_DEFAULT_DATA_TYPE
            ].lower(),
        )
        if data_type == "float":
            value = unpack(f"{byte_order_char}f", combined_bytes)[0]
            return 0 if math.isnan(value) else value
        elif data_type == "int":
            return unpack(f"{byte_order_char}I", combined_bytes)[0]
        else:
            raise ValueError("Unsupported data type. Use 'float' or 'int'.")

    def _create_modbus_client(self) -> ModbusBaseClient:
        """Create and hand out a modbus client object as defined
        by config.
        :rtype: ModbusBaseClient, either ModbusTcpClient or ModbusSerialClient
        :return: the modbus client to connect to the server
        :raise: ModbusClientException
        """
        logging.debug("bhm._create_modbus_client()")
        client: ModbusBaseClient = None
        try:
            if (
                self._action_detail[ModbusCollector.KEY_MODBUS_MODE].lower()
                == "tcp"
            ):
                tcp_params: dict[str, str] = {}
                if (
                    (
                        ModbusCollector.KEY_MODBUS_TCP_HOST
                        in self._action_detail
                    )
                    and (
                        self._action_detail[
                            ModbusCollector.KEY_MODBUS_TCP_HOST
                        ]
                    )
                    and (
                        isinstance(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_TCP_HOST
                            ],
                            str,
                        )
                    )
                ):
                    tcp_params["host"] = self._action_detail[
                        ModbusCollector.KEY_MODBUS_TCP_HOST
                    ]
                else:
                    raise ValueError(
                        "Invalid ModbusTCP host address in config"
                    )
                if ModbusCollector.KEY_MODBUS_TCP_PORT in self._action_detail:
                    if (
                        (
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_TCP_PORT
                            ]
                        )
                        and (
                            isinstance(
                                self._action_detail[
                                    ModbusCollector.KEY_MODBUS_TCP_PORT
                                ],
                                str,
                            )
                        )
                        or (
                            isinstance(
                                self._action_detail[
                                    ModbusCollector.KEY_MODBUS_TCP_PORT
                                ],
                                int,
                            )
                        )
                    ):
                        tcp_params["port"] = str(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_TCP_PORT
                            ]
                        )
                    else:
                        raise ValueError("Invalid ModbusTCP port in config")
                client = ModbusTcpClient(**tcp_params)
            elif (
                self._action_detail[ModbusCollector.KEY_MODBUS_MODE].lower()
                == "rtu"
            ):
                rtu_params: dict[str, str] = {}
                if (
                    ModbusCollector.KEY_MODBUS_RTU_DEVICE
                    in self._action_detail
                    and self._action_detail[
                        ModbusCollector.KEY_MODBUS_RTU_DEVICE
                    ]
                    and isinstance(
                        self._action_detail[
                            ModbusCollector.KEY_MODBUS_RTU_DEVICE
                        ],
                        str,
                    )
                ):
                    rtu_params["port"] = self._action_detail[
                        ModbusCollector.KEY_MODBUS_RTU_DEVICE
                    ]
                else:
                    raise ValueError("Invalid ModbusRTU device in config")
                if (
                    ModbusCollector.KEY_MODBUS_RTU_TIMEOUT
                    in self._action_detail
                ):
                    if (
                        self._action_detail[
                            ModbusCollector.KEY_MODBUS_RTU_TIMEOUT
                        ]
                    ) and (
                        isinstance(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_TIMEOUT
                            ],
                            str,
                        )
                        or isinstance(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_TIMEOUT
                            ],
                            int,
                        )
                    ):
                        rtu_params["timeout"] = str(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_TIMEOUT
                            ]
                        )
                    else:
                        raise ValueError("Invalid ModbusRTU timeout in config")
                if (
                    ModbusCollector.KEY_MODBUS_RTU_BAUDRATE
                    in self._action_detail
                ):
                    if self._action_detail[
                        ModbusCollector.KEY_MODBUS_RTU_BAUDRATE
                    ] and (
                        isinstance(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_BAUDRATE
                            ],
                            str,
                        )
                        or isinstance(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_BAUDRATE
                            ],
                            int,
                        )
                    ):
                        rtu_params["baudrate"] = str(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_BAUDRATE
                            ]
                        )
                    else:
                        raise ValueError(
                            "Invalid ModbusRTU baudrate in config"
                        )
                if (
                    ModbusCollector.KEY_MODBUS_RTU_STOPBIT
                    in self._action_detail
                ):
                    if self._action_detail[
                        ModbusCollector.KEY_MODBUS_RTU_STOPBIT
                    ] and (
                        isinstance(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_STOPBIT
                            ],
                            str,
                        )
                        or isinstance(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_STOPBIT
                            ],
                            int,
                        )
                    ):
                        rtu_params["stopbit"] = str(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_STOPBIT
                            ]
                        )
                    else:
                        raise ValueError("Invalid ModbusRTU stopbit in config")
                if (
                    ModbusCollector.KEY_MODBUS_RTU_BYTESIZE
                    in self._action_detail
                ):
                    if self._action_detail[
                        ModbusCollector.KEY_MODBUS_RTU_BYTESIZE
                    ] and (
                        isinstance(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_BYTESIZE
                            ],
                            str,
                        )
                        or isinstance(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_BYTESIZE
                            ],
                            int,
                        )
                    ):
                        rtu_params["bytesize"] = str(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_BYTESIZE
                            ]
                        )
                    else:
                        raise ValueError(
                            "Invalid ModbusRTU bytesize in config"
                        )
                if (
                    ModbusCollector.KEY_MODBUS_RTU_PARITY
                    in self._action_detail
                ):
                    if self._action_detail[
                        ModbusCollector.KEY_MODBUS_RTU_PARITY
                    ] and (
                        isinstance(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_PARITY
                            ],
                            str,
                        )
                        or isinstance(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_PARITY
                            ],
                            int,
                        )
                    ):
                        rtu_params["parity"] = str(
                            self._action_detail[
                                ModbusCollector.KEY_MODBUS_RTU_PARITY
                            ]
                        )
                    else:
                        raise ValueError("Invalid ModbusRTU parity in config")
                client = ModbusSerialClient(**rtu_params)
            else:
                raise ValueError(
                    f"""Invalid client type \
'{self._action_detail[ModbusCollector.KEY_MODBUS_MODE]}'. \
Use 'tcp' or 'rtu'."""
                )
            if not client.connect():
                raise ConnectionException(
                    "Failed to initiate a connection to the Modbus device."
                )
        except (ValueError, ConnectionException) as e:
            raise ModbusClientException(f"""Modbus client error: {e}""")

        return client

    def collect(self) -> dict[str, dict[str, str]]:
        """Get the data from Modbus and return it

        :rtype: dict
        :return: mapping of the input info
        :raise InputModuleError:"""
        logging.debug("bhm.collect()")
        # new data result dict with all data to be returned at the end
        r: dict[str, dict[str, str]] = {}
        try:
            tmstmp: str = str(self.current_timestamp())
            # modbus client
            client: ModbusBaseClient = self._create_modbus_client()

            # loop through all the points of interest from the data
            # config section
            dataconf = cast(
                dict[str, dict[str, str]],
                self._data[ModbusCollector.KEY_DATA_VALUE_MAP],
            )
            for k, v in dataconf.items():
                logging.debug(f".scanning {k}")
                # only use 'r' after the run
                timestamp: dict[str, str] = {
                    self._custom_name[
                        ModbusCollector.KEY_TIMESTAMP_NAME
                    ]: tmstmp
                }
                # look up maxretries from config
                if (
                    ModbusCollector.KEY_MODBUS_MAX_CONNECTION_RETRIES
                    in self._action_detail
                    and self._action_detail[
                        ModbusCollector.KEY_MODBUS_MAX_CONNECTION_RETRIES
                    ]
                ):
                    max_retries = int(
                        self._action_detail[
                            ModbusCollector.KEY_MODBUS_MAX_CONNECTION_RETRIES
                        ]
                    )
                else:
                    max_retries = 8  # just something..
                # just try a couple of times if necessary...
                tmpv = None
                for i in range(0, max_retries):
                    logging.debug(f"..round {i}/{max_retries}")
                    logging.debug(
                        f"""...addr:\
{v[self._custom_name[ModbusCollector.KEY_MODBUS_ADDRESS_NAME]]} count: \
{v[self._custom_name[ModbusCollector.KEY_MODBUS_COUNT_NAME]]}"""
                    )
                    try:
                        tmpv = client.read_input_registers(
                            int(
                                v[
                                    self._custom_name[
                                        ModbusCollector.KEY_MODBUS_ADDRESS_NAME
                                    ]
                                ]
                            ),
                            count=int(
                                v[
                                    self._custom_name[
                                        ModbusCollector.KEY_MODBUS_COUNT_NAME
                                    ]
                                ]
                            ),
                        )
                        logging.debug(f"...found registers: {tmpv.registers}")
                        break
                    except ValueError as e:
                        raise ModbusException(
                            f"""Unable to get valid data, check config for \
e.g. '{ModbusCollector.KEY_MODBUS_ADDRESS_NAME}' and \
'{ModbusCollector.KEY_MODBUS_COUNT_NAME}', or for stable connection: {e}"""
                        )
                    except ModbusException:
                        time.sleep(2)
                        continue
                try:
                    if tmpv:
                        r[k] = timestamp
                        r[k][
                            self._custom_name[ModbusCollector.KEY_VALUE_NAME]
                        ] = str(
                            self._convert_byte_registers_to_value(
                                tmpv.registers
                            )
                        )
                #            except (KeyError, ValueError, AttributeError):
                except ValueError:
                    continue

        except (
            ConnectionException,
            ModbusIOException,
            ModbusException,
            ModbusClientException,
        ) as e:
            raise InputModuleException(
                f"An error occurred during reading input: {e}"
            )
        return r
