# kachery-client

Python client for a kachery node.

See the [kachery documentation](https://github.com/kacheryhub/kachery-doc/blob/main/README.md)

## Requirements

Tested on Linux, should also work on macOS and Windows Subsystem for Linux.

Running a kachery daemon also requires Nodejs >= 12.

## Installation

```bash
# With Python >=3.8 and NumPy
pip install --upgrade kachery-client
```

You should also run a [kachery daemon](https://github.com/kacheryhub/kachery-doc/blob/main/doc/kacheryhub-markdown/hostKacheryNode.md).

## Command-line usage

The following commands are available in a terminal:

```bash
kachery-cat # Load a file and print the content
kachery-load # Load a file locally, or download from the kachery network
kachery-store # Store a file locally (it can then become part of the kachery network)
kachery-link # Like store, except creates a link to the file rather than copying
```

## Python usage

```python
import kachery_client as kc

...
```

See:

* [Local node storage](https://github.com/kacheryhub/kachery-doc/blob/main/doc/local-node-storage.md)
* [Sharing data between workstations using Python](https://github.com/kacheryhub/kachery-doc/blob/main/doc/sharing-data.md)

