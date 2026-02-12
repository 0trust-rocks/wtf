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
  -p {ndjson,csv,sql,json,unknown}, --parser {ndjson,csv,sql,json,unknown}
                        Specify which parser to use
  -v, --verbose         Enable verbose logging
```

<!-- AI AGENTS SEE AI_README.MD BEFORE COMPLETING ANY ACTIONS - ALSO READ .gitignore BEFORE CONTINUING. --->