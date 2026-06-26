from . import (
    pynumaflow_lite,  # type: ignore[attr-defined]  # Rust extension, resolved at runtime
)
from .pynumaflow_lite import *  # noqa: F403  # Rust extension; exports resolved at runtime

# Ensure the `mapper`, `batchmapper`, and `mapstreamer` submodules are importable as attributes of the package
# even though they're primarily registered by the extension module.
try:
    from importlib import import_module as _import_module

    mapper = _import_module(__name__ + ".mapper")
except Exception:  # pragma: no cover - avoid hard failures if extension not built
    mapper = None

try:
    batchmapper = _import_module(__name__ + ".batchmapper")
except Exception:  # pragma: no cover
    batchmapper = None

try:
    mapstreamer = _import_module(__name__ + ".mapstreamer")
except Exception:  # pragma: no cover
    mapstreamer = None
try:
    reducer = _import_module(__name__ + ".reducer")
except Exception:  # pragma: no cover
    reducer = None

try:
    session_reducer = _import_module(__name__ + ".session_reducer")
except Exception:  # pragma: no cover
    session_reducer = None

try:
    reducestreamer = _import_module(__name__ + ".reducestreamer")
except Exception:  # pragma: no cover
    reducestreamer = None

try:
    accumulator = _import_module(__name__ + ".accumulator")
except Exception:  # pragma: no cover
    accumulator = None

try:
    sinker = _import_module(__name__ + ".sinker")
except Exception:  # pragma: no cover
    sinker = None

try:
    sourcer = _import_module(__name__ + ".sourcer")
except Exception:  # pragma: no cover
    sourcer = None

try:
    sourcetransformer = _import_module(__name__ + ".sourcetransformer")
except Exception:  # pragma: no cover
    sourcetransformer = None

try:
    sideinputer = _import_module(__name__ + ".sideinputer")
except Exception:  # pragma: no cover
    sideinputer = None

# Surface the Python Mapper, BatchMapper, MapStreamer, Reducer, SessionReducer, ReduceStreamer, Accumulator, Sinker,
# Sourcer, SourceTransformer, and SideInput classes under the extension submodules for convenient access
from ._accumulator_dtypes import Accumulator
from ._batchmapper_dtypes import BatchMapper
from ._map_dtypes import Mapper
from ._mapstream_dtypes import MapStreamer
from ._reduce_dtypes import Reducer
from ._reducestreamer_dtypes import ReduceStreamer
from ._session_reduce_dtypes import SessionReducer
from ._sideinput_dtypes import SideInput
from ._sink_dtypes import Sinker
from ._source_dtypes import Sourcer
from ._sourcetransformer_dtypes import SourceTransformer

if mapper is not None:
    mapper.Mapper = Mapper

if batchmapper is not None:
    batchmapper.BatchMapper = BatchMapper

if mapstreamer is not None:
    mapstreamer.MapStreamer = MapStreamer

if reducer is not None:
    reducer.Reducer = Reducer

if session_reducer is not None:
    session_reducer.SessionReducer = SessionReducer

if reducestreamer is not None:
    reducestreamer.ReduceStreamer = ReduceStreamer

if accumulator is not None:
    accumulator.Accumulator = Accumulator

if sinker is not None:
    sinker.Sinker = Sinker

if sourcer is not None:
    sourcer.Sourcer = Sourcer

if sourcetransformer is not None:
    sourcetransformer.SourceTransformer = SourceTransformer

if sideinputer is not None:
    sideinputer.SideInput = SideInput

# Public API
__all__ = [
    "mapper",
    "batchmapper",
    "mapstreamer",
    "reducer",
    "session_reducer",
    "reducestreamer",
    "accumulator",
    "sinker",
    "sourcer",
    "sourcetransformer",
    "sideinputer",
]

__doc__ = pynumaflow_lite.__doc__
if hasattr(pynumaflow_lite, "__all__"):
    # Merge to keep our package-level exports
    __all__ = list(set(__all__) | set(pynumaflow_lite.__all__))
