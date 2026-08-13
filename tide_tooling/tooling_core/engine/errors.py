# Base Exception Class
class PipelineError(Exception):
    """Base class for all Pipeline error objects"""
    pass

# Node Errors:
class NodeDefinitionError(PipelineError):
    """Raised when a route (node) is incorrectly defined e.g (bad func, bad target etc) """
    pass

# Resolution Errors:
class NodeResolutionError(PipelineError):
    """Raised when a route's (node's) dependencies fail to be resolved"""
    pass
 
# Definiton Errors:
class StageDefinitionError(PipelineError):
    """Raised when a stage (collection of nodes) is incorrectly defined (bad name, bad list/tuple)"""
    pass

# Indexing Errors:

class NodeIndexError(PipelineError):
    """Raised when a node fails to be indexed"""
    pass 

class StageIndexError(PipelineError):
    """Raised when a stage fails to be indexed"""
    pass

class DAGIndexError(PipelineError):
    """Raised when DAG graph elements are failed to be indexed meaning The DAG graph fails to be built"""
    pass

class PipelineIndexError(PipelineError):
    """Raised when a route (node) fails to be indexed """
    pass

# Runtime Errors:

class PipelineRuntimeError(PipelineError):
    """Raised when a route (node) fails to execute properly or the pipeline_runner hits an error  """
    pass

class PipelineCycleError(PipelineError):
    """Raised when pipeline nodes form a definite dependency cycle."""
    pass

class PipelineDeadlockError(PipelineRuntimeError):
    """Raised when a non-persistent pipeline cannot satisfy its remaining refs."""
    pass

# Dependency Errors:
class DependencyExtractionError(PipelineError):
    """Raised when DependencyExtractor fails to exttact a @dependency.string correctly"""
    pass

# DependencyTools Errors:
class DependencyToolsInitError(PipelineError):
    """Raised when DependencyTools hits an error while initialising"""
    pass

class CallableDependencyError(PipelineError):
    """Raised when a Callable Node input hits an error while calling"""
    pass

# KVStore Processing Errors
class ContactMeNowError(PipelineError):
    def __init__(self, str, error: Exception):
        self.str = str
        self.error = error
        pass

class StoreReferenceAmbiguityError(PipelineError):
    def __init__(self, message, name=None):
        super().__init__(message)
        self.name = name

class MissingStoreError(PipelineError):
    """Raised when a referenced store doesn't exist in the StoreRegistry""" 
    pass

class MissingDepKeyError(PipelineError):
    """Raised when a key is missing from the data_store"""
    pass

class NodeToDagRegistrationError(PipelineError):
    """Raised when a node fails to be registered to the DAG"""
    pass

class RuntimeDagConsumptionError(PipelineError):
    """Raised when DAG fail to be copied to RuntimeDag object"""
    pass

class DataRefInitError(PipelineError):
    """Raised when DatRef is not initialised properly"""
    pass

class EventHandlerError(PipelineError):
    """Raised when there is an error handling an event during runtime"""
    pass

class NodeFailed(PipelineError):
    '''Raised when a node fails to evaluate'''
    def __init__(self, node_id, input_name, error):
        self.node_id = node_id
        self.input_name = input_name
        self.error = error
        super().__init__(str(error))

class GetResultFromFutureError(PipelineError):
    """Raise when a result is failed to be gathered from a Future object"""
    pass

# Base Exception Class
class TIDEError(Exception):
    """Base class for all TIDE error objects"""
    pass


class ReadInsideProcessPoolError(TIDEError):
    """Signal that a runtime READ must be retried in shared-memory mode."""

    pass


class DeleteNowInsideProcessPoolError(ReadInsideProcessPoolError):
    """Signal that DELETE_NOW must be retried in shared-memory mode."""

    pass


class MissingExecutionEvidenceError(TIDEError):
    """Raised when executed-node history lacks its immutable ExecMeta evidence."""

    pass


class RaiseHandlingError(TIDEError):
    """A controlled RAISE could not complete its fall-through contract."""

    def __init__(self, node_id, raised_error, handling_error):
        self.node_id = node_id
        self.raised_error = raised_error
        self.handling_error = handling_error
        super().__init__(
            f"Node {node_id} produced a controlled RAISE for "
            f"{raised_error!r}, but TIDE failed while handling it: "
            f"{handling_error}"
        )


class NoExecRecordError(TIDEError):
    pass

class MissingRefDuringResolutionError(TIDEError):
    def __init__(self, error_str, ref, available_keys):
        super().__init__(error_str)
        self.ref = ref
        self.available_keys = available_keys 
    pass

class DynamicRefCountLessThanAvailableKeysError(TIDEError):
    def __init__(self, error_str, ref, available_keys):
        super().__init__(error_str)
        self.ref = ref 
        self.available_keys = available_keys
    pass

class InvalidObjectKindError(TIDEError):
    def __init__(self, error_str, kind):
        super().__init__(error_str)
        self.kind = kind 
        pass

class StoreAlreadyRegisteredError(TIDEError):
    def __init__(self, error_str, store):
        super().__init__(error_str)
        self.store = store 
        pass
