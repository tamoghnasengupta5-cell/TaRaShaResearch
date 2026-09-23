import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { localDataReconciliationAdminKey } from "./localAuthDev";

const temporaryDirectories: string[] = [];

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("local Data Reconciliation configuration", () => {
  it("prefers the explicitly configured Discover server secret", () => {
    expect(localDataReconciliationAdminKey(
      { TARASHA_DATA_ADMIN_KEY: "discover-secret" },
      ["/missing/upstream.env"],
    )).toBe("discover-secret");
  });

  it("reads the server-only key from the upstream TaRaShaData env file", () => {
    const directory = mkdtempSync(join(tmpdir(), "tarasha-reconciliation-env-"));
    temporaryDirectories.push(directory);
    const filename = join(directory, ".env");
    writeFileSync(
      filename,
      "SEC_USER_AGENT=qa@example.com\nADMIN_API_KEY=\"upstream-secret\"\n",
      "utf8",
    );

    expect(localDataReconciliationAdminKey({}, [filename])).toBe("upstream-secret");
  });

  it("returns an empty value when neither configuration source exists", () => {
    expect(localDataReconciliationAdminKey({}, ["/missing/upstream.env"])).toBe("");
  });
});
