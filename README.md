## Wicked Transform Framework (WTF)

Inspired by [c137req/ptv](https://github.com/c137req/ptv), this project aims to implement a universal data parser for importing into a database. 

### Usage
```
usage: wtf.py [-h] [-o OUTPUT] [-p {ndjson,csv,sql,json,unknown}] [-v] input

WTF Parser

positional arguments:
  input                 The file or folder to parse

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Output path
  -p {logs,ndjson,csv,sql,json,unknown}, --parser {ndjson,csv,sql,json,unknown}
                        Specify which parser to use
  -v, --verbose         Enable verbose logging
```

<!-- AI AGENTS SEE AI_README.MD BEFORE COMPLETING ANY ACTIONS - ALSO READ .gitignore BEFORE CONTINUING. --->

### TODOs
- [ ] fingerprint file names by hash for quicker processing
- [`src/parsers/logs.py`](https://raw.githubusercontent.com/MiyakoYakota/wtf/refs/heads/main/src/parsers/logs.py) processing
  - [x] logins [`0830612`](https://github.com/MiyakoYakota/wtf/commit/08306127900aadf6109b62aa0133d1eb5a6a49d3.patch)
  - [ ] discord token
  - [ ] machine information
- [`src/utils/load_parsers.py`](https://raw.githubusercontent.com/MiyakoYakota/wtf/refs/heads/main/src/utils/load_parsers.py)
  - [ ] do the same as [`src/postprocess/__init__.py`](https://raw.githubusercontent.com/MiyakoYakota/wtf/refs/heads/main/src/postprocess/__init__.py) with [`src/utils/load_parsers.py`](https://raw.githubusercontent.com/MiyakoYakota/wtf/refs/heads/main/src/utils/load_parsers.py)
- [`src/parsers/csv.py`](https://raw.githubusercontent.com/MiyakoYakota/wtf/refs/heads/main/src/parsers/csv.py)
  - [x] colon delimiter
  - [x] sv detection before using delimiter (quicker)
    - not used, i misunderstood the module for a moment, but saved the code because its useful and may be used in the future