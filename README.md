## Wicked Transform Framework (WTF)

Inspired by [c137req/ptv](https://github.com/c137req/ptv), this project aims to implement a universal data parser for importing into a database. 

### Usage
```
usage: wtf.py [-h] [-o OUTPUT] [-p {ndjson,csv,sql,json,unknown,stealer_logs}] [-v] [-s SOURCE] [-t THREADS] [-n] [--headers HEADERS] [--dry-run] [--recency-year RECENCY_YEAR] input

WTF Parser

positional arguments:
  input                 The file or folder to parse

options:
  -h, --help            show this help message and exit
  -o, --output OUTPUT   Output path
  -p, --parser {ndjson,csv,sql,json,unknown,stealer_logs}
                        Specify which parser to use
  -v, --verbose         Enable verbose logging
  -s, --source SOURCE   Name of the data source (stored as "source" field)
  -t, --threads THREADS
                        Number of threads for parallel parsing
  -n, --no-output       Do not keep the output file, this is useful for development
  --headers HEADERS     Specify headers used by the CSV parser
  --dry-run             Only parse 1000 lines from input files. This is useful for testing your parser on a large dataset before converting.
  --recency-year RECENCY_YEAR
                        Specify how recent the data is, when it cannot be inferred from the data
```

<!-- AI AGENTS SEE AI_README.MD BEFORE COMPLETING ANY ACTIONS - ALSO READ .gitignore BEFORE CONTINUING. --->

<i>For updates on what's next, head over to [todo.md](todo.md)</i>
