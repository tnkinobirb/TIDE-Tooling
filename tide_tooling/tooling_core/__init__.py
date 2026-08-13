# Internal engine diagnostics are quiet by default.  Runtime modules import
# ``debug_print`` instead of writing directly to stdout, while public ``show``
# methods keep using the real built-in print function.
DEBUG = False


def set_debug(enabled=True):
    global DEBUG
    DEBUG = bool(enabled)


def debug_enabled():
    return DEBUG


def debug_print(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

