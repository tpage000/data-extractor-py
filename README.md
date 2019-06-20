# Data extractor

## Extract mongo data from local or remote server

### Basic usage

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

> Extract data from dev db into csv files

```
python extract.py --dev --csv
```

> Count number of records in test db

```
python extract.py --test --count
```

> Print to console contents of dev db

```
python extract.py --dev --print
```

* Cannot print or save `--prod` data.
* Max rows per file configurable

--

### Advanced / optional usage

Optional arguments for **query** and **fields**

Example

> Retrieve only documents where column "somevalue" has value True

```
python extract.py --test --print "{ 'somevalue': True }"
```

> Retrieve all documents but only the "somevalue" column

```
python extract.py --test --print {} "{ 'somevalue': 1 }"
```

> Retrieve all documents but remove the "somevalue" column

```
python extract.py --dev --csv {} "{ 'somevalue': 0 }"
```

--

### Environment variables

```
DEV_DB_CONNECTION='connection string incl username and password'
DEV_DB_NAME='name of sub database'
DEV_DB_COLLECTION='name of collection'

TEST_DB_CONNECTION=''
TEST_DB_NAME=''
TEST_DB_COLLECTION=''

PROD_DB_CONNECTION=''
PROD_DB_NAME=''
PROD_DB_COLLECTION=''
```

