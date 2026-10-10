from __future__ import annotations

import argparse
import sys
from pathlib import Path

from tide_tooling import TIDE
from tide_tooling.tooling_core.engine.events import Effects


KINDS = (
    "actor",
    "seq_id",
    "exec_record",
    "reference",
    "value",
    "hash",
    "effect",
)


def _show_options(parser, *, values=False, raw=False):
    parser.add_argument("--no-color", action="store_true")
    parser.add_argument("--simple", action="store_true")
    if values:
        parser.add_argument("--values", action="store_true")
    if raw:
        parser.add_argument("--raw", action="store_true")


def _selector_options(parser):
    parser.add_argument("--kind", choices=KINDS)
    parser.add_argument("--version", type=int)
    parser.add_argument("--ungrouped", action="store_true")


def _identity(value, kind=None):
    if kind == "seq_id":
        return int(value)
    if kind == "effect":
        return Effects(value)
    return value


def _render(result, args, **options):
    return result.show(
        color=not args.no_color,
        simple=args.simple,
        **options,
    )


def _trace_parser():
    parser = argparse.ArgumentParser(prog="tide")
    parser.add_argument("bundle", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)

    show = commands.add_parser("show")
    _show_options(show)
    show.add_argument("--structural", action="store_true")
    show.add_argument("--full-trace", action="store_true")
    show.add_argument("--sources", action="store_true")
    show.add_argument("--undeclared", action="store_true")
    show.add_argument("--values", action="store_true")

    find = commands.add_parser("find")
    find.add_argument("identity")
    find.add_argument("--version", type=int)
    _show_options(find)

    for name in ("inspect", "why"):
        query = commands.add_parser(name)
        query.add_argument("identity")
        _selector_options(query)
        _show_options(query, values=name == "why")

    for name in ("upstream", "downstream"):
        stream = commands.add_parser(name)
        stream.add_argument("identity")
        _selector_options(stream)
        stream.add_argument("--depth", type=int)
        stream.add_argument("--max-paths", type=int)
        _show_options(stream)

    for name in ("consumed-by", "produced-by"):
        relation = commands.add_parser(name)
        relation.add_argument("identity")
        _selector_options(relation)
        _show_options(relation)

    path = commands.add_parser("path")
    path.add_argument("source")
    path.add_argument("target")
    path.add_argument("--source-kind", choices=KINDS)
    path.add_argument("--target-kind", choices=KINDS)
    path.add_argument("--source-version", type=int)
    path.add_argument("--target-version", type=int)
    path.add_argument("--ungrouped", action="store_true")
    path.add_argument("--shallow", action="store_true")
    path.add_argument("--direction", choices=("upstream", "downstream"), default="downstream")
    _show_options(path)

    for name in ("inputs-of", "output-of"):
        values = commands.add_parser(name)
        values.add_argument("execution")
        _show_options(values, raw=True)

    value = commands.add_parser("value-of")
    value.add_argument("reference")
    value.add_argument("--version", type=int)

    diff = commands.add_parser("diff")
    diff.add_argument("other_bundle", type=Path)
    _show_options(diff)

    compare = commands.add_parser("compare")
    compare.add_argument("other_bundle", type=Path)
    compare.add_argument("--mapping", choices=("structural", "literal"), default="structural")
    compare.add_argument("--full", action="store_true")
    _show_options(compare)

    return parser


def _run_trace_command(argv):
    args = _trace_parser().parse_args(argv)
    trace = TIDE.from_bundle(args.bundle)

    if args.command == "show":
        return trace.show(
            color=not args.no_color,
            simple=args.simple,
            structural=args.structural,
            full_trace=args.full_trace,
            show_sources=args.sources,
            show_undeclared=args.undeclared,
            values=args.values,
        )

    if args.command == "find":
        return _render(trace.find(args.identity, args.version), args)

    if args.command in {"inspect", "why"}:
        identity = _identity(args.identity, args.kind)
        if args.command == "why":
            return trace.why(
                identity,
                args.kind,
                args.version,
                grouped=not args.ungrouped,
                color=not args.no_color,
                simple=args.simple,
                values=args.values,
            )
        return _render(
            trace.inspect(
                identity,
                args.kind,
                args.version,
                grouped=not args.ungrouped,
            ),
            args,
        )

    if args.command in {"upstream", "downstream"}:
        identity = _identity(args.identity, args.kind)
        stream_options = {
            "grouped": not args.ungrouped,
            "depth": args.depth,
        }
        if args.max_paths is not None:
            stream_options["max_paths"] = args.max_paths
        stream = getattr(trace, args.command)(
            identity,
            args.kind,
            **stream_options,
        )
        return _render(stream, args)

    if args.command in {"consumed-by", "produced-by"}:
        identity = _identity(args.identity, args.kind)
        method_name = args.command.replace("-", "_")
        result = getattr(trace, method_name)(
            identity,
            args.kind,
            args.version,
            group=not args.ungrouped,
        )
        return _render(result, args)

    if args.command == "path":
        result = trace.path(
            _identity(args.source, args.source_kind),
            _identity(args.target, args.target_kind),
            source_type=args.source_kind,
            target_type=args.target_kind,
            source_ver=args.source_version,
            target_ver=args.target_version,
            group=not args.ungrouped,
            deep_link=not args.shallow,
            direction=args.direction,
        )
        return _render(result, args)

    if args.command in {"inputs-of", "output-of"}:
        execution = int(args.execution) if args.execution.isdigit() else args.execution
        method = trace.inputs_of if args.command == "inputs-of" else trace.output_of
        return _render(method(execution), args, raw=args.raw)

    if args.command == "value-of":
        trace.value_of(args.reference, args.version, show=True)
        return None

    if args.command in {"diff", "compare"}:
        other = TIDE.from_bundle(args.other_bundle)
        if args.command == "diff":
            return _render(trace.diff(other), args)
        return _render(
            trace.compare(other, mapping=args.mapping),
            args,
            full=args.full,
        )

    raise ValueError(f"Unsupported Trace command: {args.command}")


def _now_parser():
    parser = argparse.ArgumentParser(prog="tide now")
    parser.add_argument("script", type=Path)
    parser.add_argument("-o", "--output", type=Path)
    parser.add_argument("--project-dir", type=Path)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--show", action="store_true")
    return parser


def _run_now(argv):
    script_args = []
    if "--" in argv:
        separator = argv.index("--")
        argv, script_args = argv[:separator], argv[separator + 1:]

    args = _now_parser().parse_args(argv)
    script = args.script.expanduser().resolve()
    output = (
        args.output.expanduser().resolve()
        if args.output is not None
        else script.with_suffix(".tide.json")
    )

    if not script.is_file():
        raise FileNotFoundError(f"Python script does not exist: {script}")

    if output.exists() and not args.force:
        print(f"Bundle already exists: {output}", file=sys.stderr)
        print("Use --force to overwrite it.", file=sys.stderr)
        return 2

    trace = TIDE.now(
        script,
        *script_args,
        project_dir=args.project_dir,
    )
    trace.to_bundle(path=output)
    print(f"Bundle: {output}")

    if args.show:
        trace.show()

    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)

    if not argv or argv[0] in {"-h", "--help"}:
        print(
            "usage:\n"
            "  tide now <script> [-o bundle] [--force] [-- script arguments]\n"
            "  tide <bundle> <Trace command> [options]"
        )
        return 0

    if argv[0] == "now":
        return _run_now(argv[1:])

    _run_trace_command(argv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
