import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { CompanyStoryHome } from "./CompanyStoryHome";
import { PreparedStoryStatus } from "./PreparedStoryStatus";
import type { Company, PreparedStoryDelivery } from "./types";

const delivery: PreparedStoryDelivery = {
  preparedAtUtc: "2026-10-13T16:30:12.123456+00:00", snapshotId: "a".repeat(64),
  isLatest: false, latestStatus: "preparing",
};

describe("company story preparation notice", () => {
  it("shows the actual preparation timestamp and a pending update on the usable saved story", () => {
    const markup = renderToStaticMarkup(<PreparedStoryStatus delivery={delivery} />);
    expect(markup).toContain("Prepared on");
    expect(markup).toContain(`dateTime="${delivery.preparedAtUtc}"`);
    expect(markup).toContain("Showing the last prepared story. An update is being prepared.");
    expect(markup).toContain('role="status"');
    expect(markup).not.toContain("2025");
  });

  it("shows only the preparation date when the delivered view is current", () => {
    const markup = renderToStaticMarkup(<PreparedStoryStatus delivery={{ ...delivery, isLatest: true, latestStatus: "ready" }} />);
    expect(markup).toContain("Prepared on");
    expect(markup).not.toContain("Showing the last prepared story");
    expect(markup).not.toContain("update");
  });

  it("reports an unavailable update without claiming that it is still being prepared", () => {
    const markup = renderToStaticMarkup(<PreparedStoryStatus delivery={{ ...delivery, latestStatus: "failed" }} />);
    expect(markup).toContain("The latest update is unavailable.");
    expect(markup).not.toContain("An update is being prepared.");
  });

  it("avoids inventing an update state when the backend cannot prove it", () => {
    const markup = renderToStaticMarkup(<PreparedStoryStatus delivery={{ ...delivery, latestStatus: "unknown" }} />);
    expect(markup).toContain("Showing the last prepared story.");
    expect(markup).not.toContain("An update is being prepared.");
    expect(markup).not.toContain("The latest update is unavailable.");
  });

  it("never invents a preparation date from the current time or a filing date", () => {
    expect(renderToStaticMarkup(<PreparedStoryStatus />)).toBe("");
    expect(renderToStaticMarkup(<PreparedStoryStatus delivery={{ ...delivery, preparedAtUtc: "not-a-date" }} />)).toBe("");
  });

  it("keeps preparation date separate from the SEC filing update date in the full story header", () => {
    const company: Company = {
      id: "data-0001045810", name: "Nvidia", symbol: "NVDA", sector: "Technology", description: "",
      currency: "US$ millions", reportingPeriod: "FY 2025", updatedAt: "1 March 2026",
      preparedStoryDelivery: delivery,
      metrics: { revenue: [{ year: 2025, value: 10 }], operatingMargin: [], freeCashFlow: [], netDebt: [] },
      notes: { growth: "", profitability: "", cash: "", debt: "" },
    };
    const markup = renderToStaticMarkup(<CompanyStoryHome company={company} watched={false}
      toggleWatch={() => {}} backToDiscover={() => {}} />);
    expect(markup).toContain("Updated 1 March 2026");
    expect(markup).toContain(`dateTime="${delivery.preparedAtUtc}"`);
    expect(markup).toContain("An update is being prepared.");
    expect(markup).toContain("Revenue (FY2025)");
  });
});
