"""Shared terminal-rendering primitives for TIDE's inspectable objects."""

import re
import sys


SHOW_SECTION_WIDTH = 70
SHOW_RESET = "\033[0m"
SHOW_BOLD = "\033[1m"
SHOW_DIM = "\033[2m"
SHOW_CYAN = "\033[36m"
SHOW_GREEN = "\033[32m"
SHOW_YELLOW = "\033[33m"
SHOW_RED = "\033[31m"
SHOW_WHITE = "\033[37m"
SHOW_TREE_LAST = "\u2514\u2500\u2500"
SHOW_TREE_BRANCH = "\u251c\u2500\u2500"
SHOW_TREE_PIPE = "\u2502   "
SHOW_TREE_SHORT_LAST = "\u2514\u2500"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
ASCII_TERMINAL_REPLACEMENTS = (
    ("\u2514\u2500\u2500", "+--"),
    ("\u251c\u2500\u2500", "+--"),
    ("\u2514\u2500", "+-"),
    ("\u251c\u2500", "+-"),
    ("\u2502", "|"),
    ("\u2500", "-"),
    ("\u2192", "->"),
    ("\u2193", "v"),
    ("\u219d", "~>"),
)


def _ascii_terminal_text(output):
    """Return a readable ASCII rendering for limited terminal encodings."""
    for glyph, replacement in ASCII_TERMINAL_REPLACEMENTS:
        output = output.replace(glyph, replacement)
    return output


def _print_terminal_safe(output):
    """Print without failing when a terminal cannot encode TIDE glyphs."""
    stream = sys.stdout
    encoding = getattr(stream, "encoding", None)
    printable = output

    if encoding:
        try:
            output.encode(encoding)
        except (LookupError, UnicodeEncodeError):
            printable = _ascii_terminal_text(output).encode(
                encoding,
                errors="replace",
            ).decode(encoding)

    print(printable)


def shorten_identity(value, max_length=30):
    """Shorten an identity for display without changing its stored value."""
    text = str(value)
    if len(text) <= max_length:
        return text

    tail_length = min(8, max_length // 3)
    head_length = max_length - tail_length - 1
    return f"{text[:head_length]}\u2026{text[-tail_length:]}"


def display_identity(value, *, full_ids=True, max_length=56):
    """Return a full identity or a display-only shortened identity."""
    if full_ids:
        return str(value)
    return shorten_identity(value, max_length=max_length)


def show_section(title):
    return [f"{SHOW_DIM}{title}:{SHOW_RESET}"]


def show_header(title, summary=None, identifier=None):
    header = [
        "",
        f"{SHOW_CYAN}{'=' * SHOW_SECTION_WIDTH}{SHOW_RESET}",
    ]

    title_line = f"{SHOW_CYAN}{SHOW_BOLD}{title}{SHOW_RESET}"
    if summary is not None:
        title_line += f" {SHOW_DIM}| {summary}{SHOW_RESET}"

    header.append(title_line)
    if identifier is not None:
        header.append(f"{SHOW_DIM}{identifier}{SHOW_RESET}")
    header.append(f"{SHOW_CYAN}{'=' * SHOW_SECTION_WIDTH}{SHOW_RESET}")
    return header


def show_output(lines):
    output = "\n".join(lines)
    _print_terminal_safe(output)
    return output


def strip_ansi(value):
    return ANSI_RE.sub("", str(value))


def _show_options(color=True, simple=False, colour=None):
    if colour is not None:
        color = colour

    if not isinstance(color, bool):
        raise TypeError("show color must be a bool")
    if not isinstance(simple, bool):
        raise TypeError("show simple must be a bool")

    return color, simple


def _show_render(lines, color=True):
    output = "\n".join(lines)
    if not color:
        output = strip_ansi(output)
    _print_terminal_safe(output)
    return output


def _show_fields(
    title,
    identifier,
    fields,
    color=True,
    simple=False,
    colour=None,
):
    if colour is not None:
        color = colour

    if not isinstance(color, bool):
        raise TypeError("show color must be a bool")
    if not isinstance(simple, bool):
        raise TypeError("show simple must be a bool")

    reset = SHOW_RESET if color else ""
    bold = SHOW_BOLD if color else ""
    dim = SHOW_DIM if color else ""
    cyan = SHOW_CYAN if color else ""
    white = SHOW_WHITE if color else ""

    lines = [
        "",
        f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
        f"{cyan}{bold}{title}{reset}",
        f"{dim}{identifier}{reset}",
        f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
    ]

    if not simple:
        lines.append(f"{dim}FIELDS:{reset}")
        for name, value in fields.items():
            lines.append(
                f"  {dim}{name}:{reset} {white}{repr(value)}{reset}"
            )

    return show_output(lines)


def show_query_display(
    kind,
    identifier,
    display=None,
    grouped=False,
    version=None,
):
    if grouped:
        identity = f"GROUPED[{identifier}]"
        if version is not None:
            identity += f"[v{version}]"
        return f"{str(kind).upper()} {identity}"

    if display is not None:
        return str(display)

    return f"{str(kind).upper()}[{identifier}]"


def show_identity(identifier, grouped=False):
    if grouped:
        return f"GROUPED[{identifier}]"

    return str(identifier)
