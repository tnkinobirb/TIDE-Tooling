# Notes on noWorkflow

TIDE's optional noWorkflow adapter provides automatic, fine-grained capture of
ordinary Python execution. noWorkflow records Evaluations and Activations;
TIDE projects the recorded Evaluations as execution/value evidence and uses
Activation relationships as direct execution ancestry.

Therefore, TIDE also inherits the behaviour and current
limitations of the noWorkflow and PyPosAST versions doing the capture. TIDE
does not reinterpret a failed capture or invent evidence that the host did not
record.

## Capture and projection scope

`TIDE.now()` runs noWorkflow in a subprocess with main-script collection.
Only that capture subprocess opts into TIDE's compatibility patch, through the
`TIDE_NOW_CAPTURE` environment marker. `TIDE.from_now()` only reads and
projects an existing noWorkflow project; it does not install the patch or
modify the source database.

The adapter is a projection boundary. It retains the relationships and value
representations noWorkflow recorded, but does not repair failed capture after
the fact or manufacture missing provenance.

## Known upstream capture limitations

PyPosAST:
1. PyPosAST dictionary unpacking positions.

noWorkflow:
1. noWorkflow property-decorator capture.
2. noWorkflow walrus inside yield.
3. noWorkflow dictionary-unpacking capture.
4. noWorkflow zero-argument callback binding.

Related focused upstream work includes:

- [PyPosAST dictionary-unpacking positions](https://github.com/JoaoFelipe/pyposast/pull/2)
- [noWorkflow dictionary-unpacking item dependency](https://github.com/gems-uff/noworkflow/pull/201)

These links describe proposed upstream changes; their presence here does not
imply that they have been merged or released.

## Current compatibility patch

`TIDE.now()` enables one narrow, process-local compatibility patch via an
in-memory monkeypatch targeting the noWorkflow zero-argument callback-binding
bug.

1. captured code passes a function to uninstrumented (black-box) code;
2. that code invokes the captured function with no arguments;
3. the captured function accepts no arguments; and
4. noWorkflow otherwise carries the outer call's argument evidence into the
   child activation and attempts to bind it to an empty parameter list.

The unpatched result is an `IndexError` in noWorkflow's argument matching.
TIDE gives that zero-argument call an empty argument-binding view while
retaining the outer call's evidence where it was originally observed.

The patch:

- exists only in memory for the capture process;
- does not modify the installed noWorkflow package;
- is idempotent; and
- is not a general noWorkflow fork or compatibility layer.

It is not applied by `TIDE.from_now()` because importing an existing trial
requires no capture-time behaviour change.
