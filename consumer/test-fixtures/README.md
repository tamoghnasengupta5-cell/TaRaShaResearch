# Historical SEC-only API test fixture

`nvda-sec-only-company-dataset-historical-v6.json.gz` contains an unchanged
historical TaRaShaData Discover API response for NVIDIA (CIK 0001045810), saved
at 2026-10-11T21:55:23.544845+00:00 during the local validation audit. It includes issuer SEC financial
facts and provenance. That capture used a self-only industry dataset and disabled
market acquisition; it does not benchmark the current full industry response.

This fixture is test input only. It must never be used as serving data, a live
snapshot, a financial-data ingestion source, or a reconciliation acceptance
result. No third-party financial data was acquired for it. Tests may add mock
snapshot identity metadata without changing its historical financial values.

The original UTF-8 JSON bytes are preserved exactly. The gzip header has mtime
zero for reproducibility. Uncompressed SHA-256:

`ffb8ce15950065b6d49f344841f2ef70505edec8e977d2c6423949377603cef8`
