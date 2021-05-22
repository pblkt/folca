# Folca
**FOLder CAche** is a utility that caches the  *output* folder/file of a *command* originating from an *input* folder/file.

If the command already ran on the exact same input in the past, the output will be retreived from the cache and not recomputed.

An example use-case would be a build server that builds several branches, some repeatedly.

Respects `.gitignore` in the input folder by default.

## Usage
```
folca INPUT_PATH OUTPUT_PATH COMMAND COMMAND_ARG1 COMMAND_ARG2 ...
```
