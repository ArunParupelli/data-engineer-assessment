\# Data Engineering Take-Home Assessment – Eligibility Ingestion



\## What this does

This project builds a configuration-driven ingestion pipeline that reads partner eligibility files in different formats, maps partner columns to a standard schema using config (no hardcoding), applies required transformations, and outputs a single unified dataset.



\## Input files

Place the partner files in `data/`:

\- `data/acme.txt` (pipe-delimited)

\- `data/bettercare.csv` (comma-delimited)



\## How to run

```bash

pip install -r requirements.txt

python pipeline.py





\## Output

The pipeline generates:

\- `output/unified\_eligibility.csv`



Output columns:

\- external\_id

\- first\_name

\- last\_name

\- dob

\- email

\- phone

\- partner\_code



\## Transformations

\- Names are converted to Title Case

\- Email is lowercased

\- Date of birth is standardized to YYYY-MM-DD

\- Phone numbers are formatted as XXX-XXX-XXXX

\- Partner code is added from configuration



\## Notes

\- Sample input files were created based on the formats described in the assessment PDF.

\- Rows missing external\_id are excluded.

\- Malformed rows are skipped during ingestion.



