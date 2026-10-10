# Base Exception Class
class PipelineError(Exception):
    """Base class for all Pipeline error objects"""
    pass

# Node Errors:
class NodeDefinitionError(PipelineError):
    """Raised when a route (node) is incorrectly defined e.g (bad func, bad target etc) """
    pass

# Runtime Errors:

class PipelineRuntimeError(PipelineError):
    """Raised when a route (node) fails to execute properly or the pipeline_runner hits an error  """
    pass

class PipelineDeadlockError(PipelineRuntimeError):
    """Raised when a non-persistent pipeline cannot satisfy its remaining refs."""
    pass

class CallableDependencyError(PipelineError):
    """Raised when a Callable Node input hits an error while calling"""
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

# Base Exception Class
class TIDEError(Exception):
    """Base class for all TIDE error objects"""
    pass


class ReadInsideProcessPoolError(TIDEError):
    """A live runtime READ is unavailable inside an explicit process pool."""

    pass


class DeleteNowInsideProcessPoolError(ReadInsideProcessPoolError):
    """DELETE_NOW is unavailable inside an explicit process pool."""

    pass


class WriteNowInsideProcessPoolError(ReadInsideProcessPoolError):
    """WRITE_NOW is unavailable inside an explicit process pool."""

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

class ActorOccurrenceAlreadyAssignedError(TIDEError):
    def __init__(self, error_str, exec_record_id):
        super().__init__(error_str)
        self.exec_id = exec_record_id
        
