from . import (
    pynumaflow_lite,  # type: ignore[attr-defined]  # Rust extension, resolved at runtime
)

# Surface the Python Mapper, BatchMapper, MapStreamer, Reducer, SessionReducer, ReduceStreamer, Accumulator, Sinker,
# Sourcer, SourceTransformer, and SideInput classes under the extension submodules for convenient access
from ._accumulator_dtypes import Accumulator
from ._batchmap_server import BatchMapAsyncServer
from ._batchmapper_dtypes import BatchMapper
from ._map_dtypes import Mapper
from ._map_server import MapAsyncServer
from ._mapstream_dtypes import MapStreamer
from ._reduce_dtypes import Reducer
from ._reducestreamer_dtypes import ReduceStreamer
from ._session_reduce_dtypes import SessionReducer
from ._sideinput_dtypes import SideInput
from ._sink_dtypes import Sinker
from ._sink_server import SinkAsyncServer
from ._source_dtypes import Sourcer
from ._sourcetransformer_dtypes import SourceTransformer
from .pynumaflow_lite import *  # noqa: F403  # Rust extension; exports resolved at runtime

# Submodules are defined by the Rust extension, which also registers them in sys.modules
# as `pynumaflow_lite.<name>`.
from .pynumaflow_lite import (  # type: ignore[attr-defined]
    accumulator,
    batchmapper,
    mapper,
    mapstreamer,
    reducer,
    reducestreamer,
    session_reducer,
    sideinputer,
    sinker,
    sourcer,
    sourcetransformer,
)

mapper.Mapper = Mapper
mapper.MapAsyncServer = MapAsyncServer
batchmapper.BatchMapper = BatchMapper
batchmapper.BatchMapAsyncServer = BatchMapAsyncServer
mapstreamer.MapStreamer = MapStreamer
reducer.Reducer = Reducer
session_reducer.SessionReducer = SessionReducer
reducestreamer.ReduceStreamer = ReduceStreamer
accumulator.Accumulator = Accumulator
sinker.Sinker = Sinker
sinker.SinkAsyncServer = SinkAsyncServer
sourcer.Sourcer = Sourcer
sourcetransformer.SourceTransformer = SourceTransformer
sideinputer.SideInput = SideInput

# Public API
__all__ = [
    "accumulator",
    "batchmapper",
    "mapper",
    "mapstreamer",
    "reducer",
    "reducestreamer",
    "session_reducer",
    "sideinputer",
    "sinker",
    "sourcer",
    "sourcetransformer",
]

__doc__ = pynumaflow_lite.__doc__
