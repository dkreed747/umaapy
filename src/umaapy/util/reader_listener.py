"""
.. module:: reader_listener
   :synopsis: Wraps a callback into a DDS DataReaderListener.
"""

from typing import override, Callable
import logging

from rti.connextdds import DataReaderListener, DataReader

_logger = logging.getLogger(__name__)


class ReaderListener(DataReaderListener):
    """
    Bridges a simple callable to the DDS DataReaderListener interface.

    :param callback: Function to call when data is available
    :type callback: Callable[[DataReader], None]
    """

    def __init__(self, callback: Callable[[DataReader], None]):
        super().__init__()
        self._cb: Callable[[DataReader], None] = callback

    @override
    def on_data_available(self, reader: DataReader):
        """
        Handle the DDS on_data_available event by delegating to the provided callback.

        :param reader: The DDS DataReader instance with new data
        :type reader: DataReader
        """
        try:
            self._cb(reader)
        except Exception as e:
            _logger.warning(f"Reader listener callback raised an exception - {e}")
