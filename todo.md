### TODOs
- [ ] fingerprint file names by hash for quicker processing - `parsers/stealer_logs.py`

- `parsers/stealer_logs.py`

  - [ ] update to use ir.Record() for yielding returns

  - [x] logins [`0830612`](https://github.com/MiyakoYakota/wtf/commit/08306127900aadf6109b62aa0133d1eb5a6a49d3)

  - [ ] discord token

  - [ ] machine information

- `parsers/pgp.py`
  - [x] `get_itr`
  - [x] follow `ir/record.py` fields

### done

- `parsers/csv.py`

  - [x] colon delimiter

  - [x] sv detection before using delimiter (quicker)

    - not used, i misunderstood the module for a moment, but saved the code because its useful and may be used in the future