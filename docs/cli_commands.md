# CLI quick reference

```text
tide now <script> [capture options] [-- script arguments]
tide <bundle> <command> [command options]
```

## Capture

```bash
# Capture a script as a TIDE Bundle
tide now workflow.py -o workflow.tide.json

# Pass arguments to the script
tide now workflow.py -o workflow.tide.json -- arg1 arg2

# Select the noWorkflow project/storage directory
tide now workflow.py --project-dir capture-project -o workflow.tide.json

# Replace an existing Bundle
tide now workflow.py -o workflow.tide.json --force

# Display the Trace after capture
tide now workflow.py -o workflow.tide.json --show
```

Without `-o`, output is written beside the script as `<script>.tide.json`.

## Inspect recorded evidence

```text
show | find | inspect | why | upstream | downstream | path |
consumed-by | produced-by | inputs-of | output-of | value-of
```

```bash
tide workflow.tide.json show
tide workflow.tide.json find clean

tide workflow.tide.json inspect clean --kind actor
tide workflow.tide.json why clean --kind actor

tide workflow.tide.json upstream clean --kind actor
tide workflow.tide.json downstream clean --kind actor
tide workflow.tide.json path source target --direction downstream

tide workflow.tide.json consumed-by clean --kind actor
tide workflow.tide.json produced-by global:result --kind reference

tide workflow.tide.json inputs-of execution_id
tide workflow.tide.json output-of execution_id

tide workflow.tide.json value-of global:result
tide workflow.tide.json value-of global:result --version 1
```

Where applicable, selectors accept:

```text
--kind actor|seq_id|exec_record|reference|value|hash|effect
--version <number>
--ungrouped
```

Most rendered commands accept `--no-color` and `--simple`.

Additional options:

```text
show                  --structural --full-trace --sources --undeclared --values
why                   --values
upstream/downstream   --depth <number> --max-paths <number>
path                  --source-kind --target-kind --source-version
                      --target-version --shallow --direction
inputs-of/output-of   --raw
```

## Diff and compare

```bash
# Diff concrete evidence
tide first.json diff second.json

# Compare represented execution structure
tide first.json compare second.json --mapping structural

# Compare literal identities and show the full report
tide first.json compare second.json --mapping literal --full
```

The CLI supports structural and literal comparison mappings. Explicit
supplied mappings are available through the Python API.
