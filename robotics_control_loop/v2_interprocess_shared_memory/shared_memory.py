from abc import ABC, abstractmethod
import numpy as np


### SM: Shared Memory Support
class SMCompliant(ABC):
    """Any data to be sent to shared memory should comply, i.e. be able to serialize"""

    @abstractmethod
    def buf_size(self) -> int:
        pass

    @abstractmethod
    def set_to_buffer(self, buffer: memoryview | bytearray) -> None:
        """Serialize data to buffer."""
        pass

    @abstractmethod
    def read_from_buffer(self, buffer: memoryview | bytes) -> None:
        """Deserialize data from buffer."""
        pass

    @abstractmethod
    def instantiation_params(self) -> tuple:
        pass


class NumpySMAdapter(SMCompliant):
    """
    Adapter between a NumPy array and a shared-memory (SM) communication block.
    It does not allocate SM, it only knows how to copy to/from a buffer
    """
    def __init__(self, shape: tuple[int, ...], dtype: np.dtype):
        """Array to be mapped to SM block"""
        self.array = np.empty(shape, dtype=dtype)

    @staticmethod
    def lazy_init(array: np.ndarray, adapter: 'NumpySMAdapter | None'=None) -> 'NumpySMAdapter':
        """Lazily initialize adapter and copy array data."""
        if adapter is None:
            adapter = NumpySMAdapter(array.shape, array.dtype)
        adapter.array[:] = array    # copy new data: equivalent to np.copyto(adapter.array, array)
        return adapter

    def instantiation_params(self) -> tuple:
        """ Receiving class it to reconstruct the same array """
        return (self.array.shape, self.array.dtype)

    def buf_size(self) -> int:
        return self.array.nbytes

    def set_to_buffer(self, buffer: memoryview | bytearray) -> None:
        # copy raw bytes into the shared memory buffer
        buffer[:self.array.nbytes] = self.array.tobytes()

    def read_from_buffer(self, buffer: memoryview | bytes) -> None:
        # a temporary view pointing to the data from SM
        self.array[:] = np.frombuffer(buffer[:self.array.nbytes], dtype=self.array.dtype).reshape(self.array.shape)

