"""The small public entry point for TIDE.

Implementation modules remain internal for now, but applications can use the
names below without reaching into that internal layout.
"""

import os


if os.environ.get("TIDE_NOW_CAPTURE") == "1":
    from tide_tooling._now_compat import install_now_argument_binding_patch

    install_now_argument_binding_patch()

from tide_tooling.tooling_core.engine.classes import (
    DataRef,
    DELETE,
    DELETE_NOW,
    EMIT,
    END,
    Effect,
    Node,
    Pipeline,
    RAISE,
    READ,
    READ_EFFECT,
    RUN,
    WRITE,
    WRITE_NOW,
    __fallback__
)
from tide_tooling.tooling_core.engine.decorators import node, pipeline
from tide_tooling.tooling_core.engine.events import Effects
from tide_tooling.tooling_core.engine.errors import (
    DeleteNowInsideProcessPoolError,
    RaiseHandlingError,
    ReadInsideProcessPoolError,
    WriteNowInsideProcessPoolError,
)
from tide_tooling.tooling_core.engine.tide_links import (
    EffectEdge,
    EmissionEdge,
    ReadEdge,
)
from tide_tooling.tooling_core.engine.tide_records import (
    AffectedRecord,
    Actor,
    DependencyUseRecord,
    EffectRecord,
    EmittedNodeRecord,
    ExecRecord,
    ExpandedDepRecord,
    InputUseRecord,
    Inputs,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.tide_query_results import (
    Compared,
    ConsumedBy,
    Definition,
    Downstream,
    ExecutionAncestry,
    Inspected,
    InspectedGroup,
    Investigate,
    Nearby,
    Object,
    Pathed,
    ProducedBy,
    Searched,
    Upstream,
    UsedAs,
)
from tide_tooling.tooling_core.engine.tide_logging import Trace
from tide_tooling.tooling_core.engine.tide_rendering import strip_ansi
from tide_tooling.tooling_core.engine.ocean import Ocean
from tide_tooling.tooling_core.engine.stores import MemoryKVStore
from tide_tooling.tooling_core.bundling.TIDEBundle import TIDEBundle
from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue
from tide_tooling.core import TIDE, TIDEModelValidityReport
from tide_tooling.comparator import TIDEModelComparisonReport
from tide_tooling.TIDEAdapters import (
    NOWAdapter,
    NOWMeta,
    OpenInferenceAdapter,
    OpenLineageAdapter,
    OpenTelemetryAdapter,
)


__all__ = [
    "TIDE",
    "TIDEModelValidityReport",
    "TIDEModelComparisonReport",
    "OpenLineageAdapter",
    "OpenTelemetryAdapter",
    "NOWAdapter",
    "NOWMeta",
    "OpenInferenceAdapter",
    "TIDEBundle",
    "TIDEOpaqueValue",
    "Trace",
    "Searched",
    "Nearby",
    "Pathed",
    "Upstream",
    "Downstream",
    "ExecutionAncestry",
    "ProducedBy",
    "ConsumedBy",
    "UsedAs",
    "Definition",
    "Actor",
    "Compared",
    "DataRef",
    "Node",
    "Pipeline",
    "ExecRecord",
    "EmittedNodeRecord",
    "EmissionEdge",
    "DependencyUseRecord",
    "ExpandedDepRecord",
    "InputUseRecord",
    "Inputs",
    "WriteRecord",
    "EffectRecord",
    "AffectedRecord",
    "strip_ansi",
    "Inspected",
    "InspectedGroup",
    "Investigate",
    "Object",
    "ReadEdge",
    "EffectEdge",
    "Ocean",
    "MemoryKVStore",
    "node",
    "pipeline",
    "Effect",
    "Effects",
    "ReadInsideProcessPoolError",
    "WriteNowInsideProcessPoolError",
    "DeleteNowInsideProcessPoolError",
    "RaiseHandlingError",
    "READ",
    "READ_EFFECT",
    "WRITE",
    "WRITE_NOW",
    "END",
    "DELETE",
    "DELETE_NOW",
    "EMIT",
    "RAISE",
    "RUN",
]
