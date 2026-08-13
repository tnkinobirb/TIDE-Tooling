from tide_tooling.tooling_core.engine.classes import DataRef

def _safe_parse_func_from_string(_str: str):
    import importlib
    import builtins
    
    try:
        if _str.startswith('.'):
            raise ValueError(
                "SafeParsingError! Relative imports are not supported: "
                f"{_str} starts with '.'. Please specify a package, "
                "for example math.sqrt rather than '.sqrt'."
            )

        if "." in _str:
            module_name, func_name = _str.rsplit('.', 1)
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
            return func

        if _str in globals():
            return globals()[_str]

        if hasattr(builtins, _str):
            return getattr(builtins, _str)

        raise ValueError(f"Function '{_str}' not found")

    except Exception as e: # add func parsing error
        raise Exception(f"Safe parsing error!: {e}")
### cli input parsers and handlers###


def _ast_parse_value_from_string(_str: str):
    import ast 
    import re
    
    ref_map = {}

    def replace_ref(match):
        ref_key = f"__REF_{len(ref_map)}__"
        ref_map[ref_key] = (
            match.group("quoted_ref") or match.group("bare_ref")
        )
        return repr(ref_key)

    ref_str = re.sub(
        r"(?:(?P<quote>['\"])(?P<quoted_ref>@[\w.]+)(?P=quote))|"
        r"(?P<bare_ref>@[\w.]+)",
        replace_ref,
        _str,
    )

    try:
        parsed = ast.literal_eval(ref_str)
    except (SyntaxError, ValueError):
        parsed = ref_str

    def replace_ref_key_with_ref(obj):
        
        if isinstance(obj, str) and obj in ref_map:
            return DataRef(ref_map[obj])
        elif isinstance(obj, list):
            return [replace_ref_key_with_ref(v) for v in obj]
        elif isinstance(obj, tuple):
            return tuple(replace_ref_key_with_ref(v) for v in obj)
        elif isinstance(obj, dict):
            return {k: replace_ref_key_with_ref(v) for k,v in obj}
        return obj

    return replace_ref_key_with_ref(parsed)

def _is_valid_node_input(val: str):
    ALLOWED_INPUTS = ["args", "kwargs", "p_args", "p_kwargs", "k_args", "k_kwargs"]
    if val not in ALLOWED_INPUTS:
        # raise ValueError(f"Error: {val} is not a valid node attribute. Please specify a valid input : {ALLOWED_INPUTS}")
        return False
    return True
