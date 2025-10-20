from .pynumaflow_lite import *

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
    accumulator = _import_module(__name__ + ".accumulator")
except Exception:  # pragma: no cover
    accumulator = None

# Surface the Python Mapper, BatchMapper, MapStreamer, Reducer, SessionReducer, and Accumulator classes under the extension submodules for convenient access
from ._map_dtypes import Mapper
from ._batchmapper_dtypes import BatchMapper
from ._mapstream_dtypes import MapStreamer
from ._reduce_dtypes import Reducer
from ._session_reduce_dtypes import SessionReducer
from ._accumulator_dtypes import Accumulator

if mapper is not None:
    try:
        setattr(mapper, "Mapper", Mapper)
    except Exception:
        pass

if batchmapper is not None:
    try:
        setattr(batchmapper, "BatchMapper", BatchMapper)
    except Exception:
        pass

if mapstreamer is not None:
    try:
        setattr(mapstreamer, "MapStreamer", MapStreamer)
    except Exception:
        pass

if reducer is not None:
    try:
        setattr(reducer, "Reducer", Reducer)
    except Exception:
        pass

if session_reducer is not None:
    try:
        setattr(session_reducer, "SessionReducer", SessionReducer)
    except Exception:
        pass

if accumulator is not None:
    try:
        setattr(accumulator, "Accumulator", Accumulator)
    except Exception:
        pass

# Public API
__all__ = ["mapper", "batchmapper", "mapstreamer", "reducer", "session_reducer", "accumulator"]

__doc__ = pynumaflow_lite.__doc__
if hasattr(pynumaflow_lite, "__all__"):
    # Merge to keep our package-level exports
    __all__ = list(set(__all__) | set(pynumaflow_lite.__all__))
