# Scripts

## smoke_test_dev.sh

Validates the dev setup end-to-end using values from `conf/dev.yaml`.

### Prerequisites

- `gcloud`, `bq`, and Python 3
- Signed in: `gcloud auth login`
- Active project set: `gcloud config set project <PROJECT_ID>`

### Run

```bash
bash scripts/smoke_test_dev.sh
```

### What it checks

- Prints active gcloud account
- Uploads a tiny JSONL file to `gs://<raw_bucket>/smoke/`
- Lists and reads the uploaded object
- Ensures `ops.smoke_test` exists
- Inserts one row and queries the last 5 rows

### Output

Ends with `PASS` on success or `FAIL` on error.
