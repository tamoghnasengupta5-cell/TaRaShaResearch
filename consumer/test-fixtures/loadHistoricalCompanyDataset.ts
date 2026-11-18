import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";

const expectedSha256 = "ffb8ce15950065b6d49f344841f2ef70505edec8e977d2c6423949377603cef8";

/** Historical test input only; this module is never imported by serving code. */
export function loadHistoricalCompanyDataset() {
  const bytes = gunzipSync(readFileSync(new URL("./nvda-sec-only-company-dataset-historical-v6.json.gz", import.meta.url)));
  if (createHash("sha256").update(bytes).digest("hex") !== expectedSha256) {
    throw new Error("Historical SEC-only API fixture does not match its original bytes.");
  }
  return JSON.parse(bytes.toString("utf8"));
}
