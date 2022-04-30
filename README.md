# FolCa
**FOLder CAche** is a CLI utility for directory-based caching.

It receives three arguments:  

*input* path -> *command* to run (+args) -> *output* target path.

- If the input or the command are new, the command is run; and the results in the output path are copied to a local folder.
- If the command already ran on the exact same input in the past with the same arguments, the output will be retreived from the cache and not recomputed.

An example use-case would be a build server that builds several branches, some repeatedly.

Respects `.gitignore` in the input folder by default.

## Usage
```
folca INPUT_PATH OUTPUT_PATH COMMAND COMMAND_ARG1 COMMAND_ARG2 ...
```

## Installation
### Downloading binaries
`Folca` is downloadable from the releases [page](/../../releases).
### Cargo 
Run `cargo install folca`
