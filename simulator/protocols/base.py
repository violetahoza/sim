from __future__ import annotations
import abc
from typing import Callable

from simulator.models.models import BatchUpdate

CloudRecvCallback = Callable[[BatchUpdate, bytes], None]


class ProtocolBackend(abc.ABC):
    """Publish batches from edge to cloud and receive them on the cloud side."""

    # Total wire bytes handed to the broker (not counting broker-internal overhead).
    bytes_sent: int = 0

    @abc.abstractmethod
    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        """
        Called by the edge node (in DES virtual time) to send a batch.
        Simulated backends schedule delivery via the DES clock.
        Real backends fire an async task that publishes to the broker.
        """


    # async def start(self) -> None:
    #     """Connect to broker and start consuming."""

    # async def stop(self) -> None:
    #     """Disconnect cleanly."""