from typing import Any, Union
from warnings import warn
from .errors import MissingDepKeyError

# add MissingKeyError for 
#=================#
#---=DICT PROC=---#  
#=================#
class DictProcessor:  
    
    """
    DictProcessor:
        WEAK exception guarantee

    """
  
    def __init__(self, dict_src: dict[str, Union[dict | Any]]):
        if dict_src is None:   
            raise ValueError("Dict is None. Please provide a valid dict when using class functions!")
        elif not isinstance(dict_src, dict):
            raise TypeError(f"DICTPROC: Data_source {dict_src} must be a dict! Please provide one")
        else:
            self.dict = dict_src
    
    def set(self, set_path: str, set_value: Any, replace=False):
        '''
            takes a namespace input like:
                - sessions
                or 
                - sessions/telemetry/raw_telemetry
            and sets a value at that place in a dict object

            # needs to be converted to a the data-stack storage stuff - keyvalue store?
            #
        '''
        
        if not set_path or not isinstance(set_path, str):
            raise ValueError("set_path is empty or not a str! Please check path provided!")
        
        if "." in set_path.split("/")[-1]:
            raise ValueError(f"Path looks like a filesystem file: {set_path}")

        if ":" in set_path:
            raise ValueError(f"Path looks like a drive or URL: {set_path}")

        path_components = set_path.split("/")
        set_component = path_components[-1]
        
        dict_window = self.dict

        for component in path_components[:-1]:
            dict_window = dict_window.setdefault(component, {}) # traverse deeper into dict, or create dict if not exists, till the penultimate element.

        if replace:
            dict_window[set_component] = set_value      
        elif isinstance(dict_window.get(set_component), dict) and isinstance(set_value, dict): # if both values are dicts, and deep is true, merge dicts if possible.
            self.deep_update(dict_window.get(set_component), set_value)
        else:                                                                   
            dict_window[set_component] = set_value     
                
        return self.dict
        
    def get(self, get_path: str) -> Any:
        if not get_path or not isinstance(get_path, str):
            raise ValueError("get_path is empty or not a str! Please check path provided!") 
        
        if "." in get_path.split("/")[-1]:
            raise ValueError(f"Path looks like a filesystem file: {get_path}")

        if ":" in get_path:
            raise ValueError(f"Path looks like a drive or URL: {get_path}")

        path_components = get_path.split("/")
        
        dict_window = self.dict
        for component in path_components:
            if component not in dict_window:
                warn(f"Path_component {component} not in dict! Check path exists! | Returning default!", UserWarning)
                return get_path # return dep untouched
            dict_window = dict_window[component]
                
        return dict_window

    def fget(self, get_path):
        if not get_path or not isinstance(get_path, str):
            raise ValueError("get_path is empty or not a str! Please check path provided!") 
        
        try:
            return self.dict[get_path]
        except KeyError as e:
            warn(f"[DictProcessor.fget] Path not found: {get_path}")
            raise MissingDepKeyError(f"[DictProcessor.fget] dependency_key/get_path not found: {e}. Does a node produce this key? Is it available in the data store?")

    def fset(self, set_path, value):
        self.dict[set_path] = value
        return 

    def flatten(nested_dict_store: dict, curr_path="", sep="."):
        flat_store = {}     
        for k, v in nested_dict_store.items(): # for key/value
            new_path = curr_path + sep + k if curr_path else k # curr_path + . + k if curr_path
            if isinstance(v, dict):
                child_flat = DictProcessor.flatten(v, new_path, sep)
                DictProcessor.deep_update(flat_store, child_flat)
            else:
                flat_store[new_path] = v
    
        return flat_store

    def unflatten(kv_store: dict, sep: str='.'):
        root = {}
        for k, v in kv_store.items():
            kv_split_paths = k.split(sep)

            current = root
            for split_path in kv_split_paths[:-1]:
                if split_path not in current:
                    current[split_path] = {}
                current = current[split_path]
            current[kv_split_paths[-1]] = v
        return root

    @staticmethod
    def deep_update(dict_src, update_dict):
        for k, v in update_dict.items():
            if isinstance(v, dict) and k in dict_src and isinstance(dict_src[k], dict):
                DictProcessor.deep_update(dict_src[k], v)
            else: 
                dict_src[k] = v
        return dict_src
    
