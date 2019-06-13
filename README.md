# Data Extractor

## Extract data from mongo database

### Extract from local mongo and/or remote server

Usage

Provide environment flag and output flag

* environment flag
	* `--dev`
	* `--test`
	* `--prod`

* output flag
	* `--count`
	* `--print`
	* `--csv`

Example

```
python some_file.py --dev --csv
```

Restrictions

* Cannot print or save data from `--PROD` for security reasons

Connection defined in `.env` file

* DB connection string, name, and collection loaded from .env file in same directory

