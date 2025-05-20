import logging
import time
from typing import Dict, Any, List, Optional, cast

try:
    import can  # type: ignore
except ImportError:
    print("Warning: python-can library not found. CanBusCollector will not work.")
    can = None  # type: ignore

from bridging_hub_module import CollectorBaseModule, InputModuleException

logger = logging.getLogger(__name__)

class CanBusCollector(CollectorBaseModule):
    """
    Collector module to read data from a CAN bus using the python-can library.
    """
    KEY_CHANNEL = "channel"
    KEY_BUSTYPE = "bustype"
    KEY_BITRATE = "bitrate"
    KEY_FILTERS = "filters"
    KEY_READ_TIMEOUT = "read_timeout"
    KEY_MAX_MESSAGES = "max_messages_per_collect"
    
    def __init__(self, name: str = "") -> None:
        """Initialize the CAN bus collector.
        
        :param name: optional name
        """
        super().__init__(name)
        self._can_bus: Optional[can.BusABC] = None
        self._channel: Optional[str] = None
        self._bustype: Optional[str] = None
        self._bitrate: Optional[int] = None
        self._can_filters: Optional[List[Dict[str, Any]]] = None
        self._read_timeout: float = 1.0
        self._max_messages: int = 10  # Default max messages per collect() call
    
    def configure(self, config: Dict[str, Any]) -> None:
        """Configure the CAN bus collector from config.
        
        :param config: Configuration dictionary
        """
        super().configure(config)
        
        if self.action_type not in config:
            raise ValueError(
                f"Configuration for module '{self.the_name}' is missing the '{self.action_type}' key."
            )
            
        module_config = config[self.action_type]
        
        # Read configuration parameters
        self._channel = module_config.get(self.KEY_CHANNEL)
        self._bustype = module_config.get(self.KEY_BUSTYPE)
        self._bitrate = module_config.get(self.KEY_BITRATE)
        self._can_filters = module_config.get(self.KEY_FILTERS)
        self._read_timeout = float(module_config.get(self.KEY_READ_TIMEOUT, 1.0))
        self._max_messages = int(module_config.get(self.KEY_MAX_MESSAGES, 10))
        
        if not self._channel or not self._bustype:
            raise ValueError(
                f"CAN module '{self.the_name}' requires '{self.KEY_CHANNEL}' and '{self.KEY_BUSTYPE}' in its configuration."
            )
        
        logging.info(
            f"CAN Collector '{self.the_name}' configured for channel '{self._channel}', "
            f"bustype '{self._bustype}', bitrate '{self._bitrate}', timeout {self._read_timeout}s."
        )
    
    def _initialize_can_bus(self) -> None:
        """Initialize the CAN bus interface."""
        if not can:
            raise RuntimeError("python-can library is not installed. Cannot initialize CAN bus.")
            
        if self._can_bus is not None:
            logging.debug(f"CAN bus for '{self.the_name}' already initialized.")
            return
            
        try:
            bus_kwargs = {
                'channel': self._channel,
                'bustype': self._bustype,
            }
            
            if self._bitrate is not None:
                bus_kwargs['bitrate'] = int(self._bitrate)
                
            if self._can_filters:
                bus_kwargs['can_filters'] = self._can_filters
                
            logging.info(f"Initializing CAN bus for '{self.the_name}': {bus_kwargs}")
            self._can_bus = can.interface.Bus(**bus_kwargs)
            logging.info(f"CAN bus for '{self.the_name}' initialized successfully.")
            
        except Exception as e:
            logging.error(f"Failed to initialize CAN bus for '{self.the_name}': {e}", exc_info=True)
            self._can_bus = None
            raise RuntimeError(f"Failed to initialize CAN bus: {e}") from e
    
    def _shutdown_can_bus(self) -> None:
        """Shut down the CAN bus interface."""
        if self._can_bus:
            try:
                self._can_bus.shutdown()
                logging.info(f"CAN bus for '{self.the_name}' shut down.")
            except Exception as e:
                logging.error(f"Error shutting down CAN bus for '{self.the_name}': {e}", exc_info=True)
            finally:
                self._can_bus = None
    
    def collect(self) -> Dict[str, Dict[str, str]]:
        """Collect messages from the CAN bus.
        
        :rtype: dict
        :return: Mapping of the collected CAN messages
        :raise InputModuleException: If there's an error during collection
        """
        logging.debug(f"{self.the_name}.collect()")
        
        # Result dictionary
        result: Dict[str, Dict[str, str]] = {}
        
        try:
            # Initialize CAN bus if not already done
            if self._can_bus is None:
                self._initialize_can_bus()
                
            if self._can_bus is None:
                raise RuntimeError("CAN bus could not be initialized.")
                
            # Current timestamp for all messages in this collection
            timestamp_str = str(self.current_timestamp())
            
            # Collect up to max_messages
            msg_count = 0
            start_time = time.time()
            
            while msg_count < self._max_messages:
                # Check if we've exceeded timeout
                if time.time() - start_time > self._read_timeout:
                    break
                    
                # Try to receive a message
                message = self._can_bus.recv(timeout=0.1)  # Use a short timeout for the loop
                
                if message:
                    # Create a unique key for this message
                    msg_key = f"can_msg_{msg_count}_{message.arbitration_id:x}"
                    
                    # Base message data with timestamp
                    result[msg_key] = {
                        self._custom_name[self.KEY_TIMESTAMP_NAME]: timestamp_str,
                        "arbitration_id": str(message.arbitration_id),
                        "is_extended_id": str(message.is_extended_id),
                        "is_remote_frame": str(message.is_remote_frame),
                        "is_error_frame": str(message.is_error_frame),
                        "dlc": str(message.dlc),
                    }
                    
                    # Add data bytes as individual values
                    if message.data and len(message.data) > 0:
                        data_str = " ".join([f"{b:02X}" for b in message.data])
                        result[msg_key][self._custom_name[self.KEY_VALUE_NAME]] = data_str
                        
                        # Optionally add individual bytes for easier access
                        for i, b in enumerate(message.data):
                            result[msg_key][f"byte_{i}"] = f"{b:02X}"
                    
                    msg_count += 1
                    logging.debug(f"[{self.the_name}] Received CAN message: {message}")
                else:
                    # No message received, wait a bit
                    time.sleep(0.01)
                    
            if msg_count == 0:
                logging.debug(f"[{self.the_name}] No CAN messages received within timeout.")
                
        except Exception as e:
            logging.error(f"[{self.the_name}] Error during CAN bus operation: {e}", exc_info=True)
            raise InputModuleException(f"An error occurred during reading CAN bus: {e}")
            
        finally:
            # We don't close the connection after each collect() call
            # as it's more efficient to keep it open
            pass
            
        return result
    
    def cleanup(self) -> None:
        """Clean up resources."""
        logging.info(f"Cleaning up CAN bus collector module '{self.the_name}'")
        self._shutdown_can_bus()
        super().cleanup()
    
    def __del__(self):
        """Ensure CAN bus is shut down when object is garbage collected."""
        self._shutdown_can_bus()
