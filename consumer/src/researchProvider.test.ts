import { afterEach, describe, expect, it, vi } from "vitest";
import { growthStatistics, pullResearchCompany, searchResearchCompanies } from "../functions/researchProvider";

const env = { SHARED_RESEARCH_URL: "https://research.example", SHARED_RESEARCH_SERVICE_KEY: "server-secret" };

describe("shared Research provider", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("maps Research search rows into the provider-neutral catalogue", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify([
      { id: 9, name: "Microsoft Corporation", ticker: "MSFT", country: "USA", industry_bucket: "AI Cloud, Data & AI Platform, Model Layer : Hyperscale Cloud & AI Platforms" },
    ]), { status: 200 })));

    const companies = await searchResearchCompanies(env, "MSFT", "USA");

    expect(companies[0]).toMatchObject({ id: "research-9", ticker: "MSFT", industryBucket: "AI Cloud, Data & AI Platform, Model Layer : Hyperscale Cloud & AI Platforms", data_access: "normalized", provider: "TaRaSha Research database" });
  });

  it("normalizes Indian amounts and derives the consumer research-shelf story", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify([{ id: 44, name: "Example India", ticker: "EXAMPLE", country: "India", industry_bucket: "India : Industrials : Industrial Products Companies" }]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { company_id: 44, statement_key: "income", fact_key: "revenue", label: "Revenue", unit_kind: "amount", fiscal_year: 2024, value: 8000 },
        { company_id: 44, statement_key: "income", fact_key: "revenue", label: "Revenue", unit_kind: "amount", fiscal_year: 2025, value: 10000 },
        { company_id: 44, statement_key: "income", fact_key: "costOfRevenue", label: "Cost of revenue", unit_kind: "amount", fiscal_year: 2024, value: 5000 },
        { company_id: 44, statement_key: "income", fact_key: "costOfRevenue", label: "Cost of revenue", unit_kind: "amount", fiscal_year: 2025, value: 6000 },
        { company_id: 44, statement_key: "income", fact_key: "sga", label: "SG&A", unit_kind: "amount", fiscal_year: 2024, value: 1000 },
        { company_id: 44, statement_key: "income", fact_key: "sga", label: "SG&A", unit_kind: "amount", fiscal_year: 2025, value: 1100 },
        { company_id: 44, statement_key: "income", fact_key: "researchAndDevelopment", label: "R&D", unit_kind: "amount", fiscal_year: 2024, value: 200 },
        { company_id: 44, statement_key: "income", fact_key: "researchAndDevelopment", label: "R&D", unit_kind: "amount", fiscal_year: 2025, value: 300 },
        { company_id: 44, statement_key: "income", fact_key: "ebitda", label: "EBITDA", unit_kind: "amount", fiscal_year: 2024, value: 1600 },
        { company_id: 44, statement_key: "income", fact_key: "ebitda", label: "EBITDA", unit_kind: "amount", fiscal_year: 2025, value: 2000 },
        { company_id: 44, statement_key: "income", fact_key: "ebit", label: "EBIT", unit_kind: "amount", fiscal_year: 2025, value: 1500 },
        { company_id: 44, statement_key: "income", fact_key: "pretaxIncome", label: "Pre-tax income", unit_kind: "amount", fiscal_year: 2025, value: 1300 },
        { company_id: 44, statement_key: "income", fact_key: "netIncome", label: "Net income", unit_kind: "amount", fiscal_year: 2025, value: 1000 },
        { company_id: 44, statement_key: "income", fact_key: "minorityInterestInEarnings", label: "Minority interest in earnings", unit_kind: "amount", fiscal_year: 2025, value: 100 },
        { company_id: 44, statement_key: "income", fact_key: "earningsFromDiscontinuedOperations", label: "Earnings from discontinued operations", unit_kind: "amount", fiscal_year: 2025, value: -50 },
        { company_id: 44, statement_key: "cash", fact_key: "commonDividendsPaid", label: "Common dividends paid", unit_kind: "amount", fiscal_year: 2025, value: 200 },
        { company_id: 44, statement_key: "income", fact_key: "netIncomeToCommon", label: "Net income to common", unit_kind: "amount", fiscal_year: 2025, value: 600 },
        { company_id: 44, statement_key: "income", fact_key: "operatingIncome", label: "Operating income", unit_kind: "amount", fiscal_year: 2024, value: 1200 },
        { company_id: 44, statement_key: "income", fact_key: "operatingIncome", label: "Operating income", unit_kind: "amount", fiscal_year: 2025, value: 2000 },
        { company_id: 44, statement_key: "cash", fact_key: "operatingCash", label: "Operating cash flow", unit_kind: "amount", fiscal_year: 2025, value: 1500 },
        { company_id: 44, statement_key: "cash", fact_key: "capex", label: "Capital expenditure", unit_kind: "amount", fiscal_year: 2025, value: -500 },
        { company_id: 44, statement_key: "cash", fact_key: "depreciation", label: "D&A", unit_kind: "amount", fiscal_year: 2024, value: 400 },
        { company_id: 44, statement_key: "cash", fact_key: "depreciation", label: "D&A", unit_kind: "amount", fiscal_year: 2025, value: 500 },
        { company_id: 44, statement_key: "balance", fact_key: "totalDebt", label: "Total debt", unit_kind: "amount", fiscal_year: 2024, value: 2800 },
        { company_id: 44, statement_key: "balance", fact_key: "totalDebt", label: "Total debt", unit_kind: "amount", fiscal_year: 2025, value: 3000 },
        { company_id: 44, statement_key: "balance", fact_key: "cash", label: "Cash", unit_kind: "amount", fiscal_year: 2024, value: 800 },
        { company_id: 44, statement_key: "balance", fact_key: "cash", label: "Cash", unit_kind: "amount", fiscal_year: 2025, value: 1000 },
        { company_id: 44, statement_key: "balance", fact_key: "shortTermInvestments", label: "Short-term investments", unit_kind: "amount", fiscal_year: 2024, value: 200 },
        { company_id: 44, statement_key: "balance", fact_key: "shortTermInvestments", label: "Short-term investments", unit_kind: "amount", fiscal_year: 2025, value: 200 },
      ]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { company_id: 44, bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies" },
      ]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "revenue", fiscal_year: 2024, value: 8000 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "revenue", fiscal_year: 2025, value: 10000 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "costOfRevenue", fiscal_year: 2024, value: 5000 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "costOfRevenue", fiscal_year: 2025, value: 6000 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "operatingIncome", fiscal_year: 2024, value: 1200 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "operatingIncome", fiscal_year: 2025, value: 2000 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "sga", fiscal_year: 2024, value: 1000 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "sga", fiscal_year: 2025, value: 1100 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "depreciation", fiscal_year: 2024, value: 400 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "depreciation", fiscal_year: 2025, value: 500 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "researchAndDevelopment", fiscal_year: 2024, value: 200 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "researchAndDevelopment", fiscal_year: 2025, value: 300 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "ebitda", fiscal_year: 2025, value: 2000 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "ebit", fiscal_year: 2025, value: 1500 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "pretaxIncome", fiscal_year: 2025, value: 1300 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "netIncome", fiscal_year: 2025, value: 1000 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "minorityInterestInEarnings", fiscal_year: 2025, value: 100 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "earningsFromDiscontinuedOperations", fiscal_year: 2025, value: -50 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "commonDividendsPaid", fiscal_year: 2025, value: 200 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 44, country: "India", fact_key: "netIncomeToCommon", fiscal_year: 2025, value: 600 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "revenue", fiscal_year: 2024, value: 12000 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "revenue", fiscal_year: 2025, value: 13200 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "costOfRevenue", fiscal_year: 2024, value: 7000 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "costOfRevenue", fiscal_year: 2025, value: 7500 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "operatingIncome", fiscal_year: 2024, value: 1800 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "operatingIncome", fiscal_year: 2025, value: 1980 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "sga", fiscal_year: 2024, value: 1200 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "sga", fiscal_year: 2025, value: 1320 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "depreciation", fiscal_year: 2024, value: 600 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "depreciation", fiscal_year: 2025, value: 660 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "researchAndDevelopment", fiscal_year: 2024, value: 240 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "researchAndDevelopment", fiscal_year: 2025, value: 264 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "ebitda", fiscal_year: 2025, value: 2640 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "ebit", fiscal_year: 2025, value: 1980 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "pretaxIncome", fiscal_year: 2025, value: 1716 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "netIncome", fiscal_year: 2025, value: 1320 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "minorityInterestInEarnings", fiscal_year: 2025, value: 132 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "earningsFromDiscontinuedOperations", fiscal_year: 2025, value: 0 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "commonDividendsPaid", fiscal_year: 2025, value: 264 },
        { bucket_id: 7, bucket_name: "India : Industrials : Industrial Products Companies", company_id: 45, country: "India", fact_key: "netIncomeToCommon", fiscal_year: 2025, value: 924 },
      ]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { id: 44, name: "Example India", ticker: "EXAMPLE", country: "India", industry_bucket: "India : Industrials : Industrial Products Companies" },
        { id: 45, name: "Peer India", ticker: "PEER", country: "India", industry_bucket: "India : Industrials : Industrial Products Companies" },
      ]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { company_id: 44, enterprise_value: 50000, enterprise_value_source: "yfinance_direct", enterprise_value_as_of: "2026-07-18 12:00:00", enterprise_value_detail: "Pulled direct Enterprise Value from yfinance.", trailing_pe: 25, trailing_pe_source: "yfinance_direct", trailing_pe_as_of: "2026-07-18 12:00:00", trailing_pe_detail: "Pulled trailing P/E from yfinance.", updated_at: "2026-07-18T12:00:00" },
        { company_id: 45, enterprise_value: 66000, enterprise_value_source: "yfinance_direct", enterprise_value_as_of: "2026-07-18 12:00:00", enterprise_value_detail: "Pulled direct Enterprise Value from yfinance.", trailing_pe: 22, trailing_pe_source: "yfinance_direct", trailing_pe_as_of: "2026-07-18 12:00:00", trailing_pe_detail: "Pulled trailing P/E from yfinance.", updated_at: "2026-07-18T12:00:00" },
      ]), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const company = await pullResearchCompany(env, "research-44", 2024, 2025);

    expect(company?.currency).toBe("₹ crore");
    expect(company?.metrics.revenue).toEqual([{ year: 2024, value: 800 }, { year: 2025, value: 1000 }]);
    expect(company?.metrics.operatingMargin[1].value).toBe(20);
    expect(company?.metrics.freeCashFlow[0].value).toBe(100);
    expect(company?.metrics.netDebt[1].value).toBe(180);
    expect(company?.researchShelf?.industryBucket).toBe("India : Industrials : Industrial Products Companies");
    expect(company?.researchShelf?.industryCompanyCount).toBe(2);
    expect(company?.researchShelf?.industryConstituents).toEqual([
      expect.objectContaining({ id: "research-44", ticker: "EXAMPLE", revenueObservations: 2 }),
      expect.objectContaining({ id: "research-45", ticker: "PEER", grossProfitObservations: 2 }),
    ]);
    expect(company?.researchShelf?.industryConstituentsCustomized).toBe(false);
    expect(company?.researchShelf?.growthComparisons.revenue.company.median).toBe(25);
    expect(company?.researchShelf?.growthComparisons.revenue.industryBucket.median).toBe(17.5);
    expect(company?.researchShelf?.growthComparisons.revenue.company.distribution).toEqual([
      { label: "Company · FY 2024–2025", value: 25 },
    ]);
    expect(company?.researchShelf?.growthComparisons.revenue.industryBucket.distribution).toEqual(expect.arrayContaining([
      { label: "Peer India (PEER) · FY 2024–2025", value: 10 },
    ]));
    expect(company?.researchShelf?.growthComparisons.grossProfit.company.median).toBeCloseTo(100 / 3);
    expect(company?.researchShelf?.growthComparisons.operatingIncome.industryBucket.median).toBeCloseTo(115 / 3);
    expect(company?.researchShelf?.companyDeltas[0]).toMatchObject({
      fromYear: 2024,
      toYear: 2025,
      revenue: 200,
      revenueChangePercent: 25,
      grossProfit: 100,
      operatingIncome: 80,
    });
    expect(company?.researchShelf?.companyDeltas[0].grossProfitChangePercent).toBeCloseTo(100 / 3);
    expect(company?.researchShelf?.companyDeltas[0].operatingIncomeChangePercent).toBeCloseTo(200 / 3);
    expect(company?.researchShelf?.industryDeltas.revenue[0].industryMedian).toBe(160);
    expect(company?.researchShelf?.industryDeltas.revenue[0].industryMedianChangePercent).toBe(17.5);
    expect(company?.researchShelf?.industryDeltas.grossProfit[0].industryMedian).toBe(85);
    expect(company?.researchShelf?.industryDeltas.operatingIncome[0].industryMedian).toBe(49);
    expect(company?.researchShelf?.rawIncome[1]).toMatchObject({ year: 2025, revenue: 1000, revenueChangePercent: 25, grossProfit: 400, operatingIncome: 200 });
    expect(company?.researchShelf?.profitability.yearly[0]).toMatchObject({ year: 2024, grossMargin: 37.5, grossProfit: 300, operatingMargin: 15, operatingIncome: 120, cogsRatio: 62.5, cogs: 500, sgaRatio: 12.5, sga: 100, daRatio: 5, da: 40, rdRatio: 2.5, rd: 20 });
    expect(company?.researchShelf?.profitability.statistics.grossMargin.median).toBeCloseTo(38.75);
    expect(company?.researchShelf?.profitability.statistics.operatingMargin.standardDeviation).toBeCloseTo(Math.sqrt(12.5));
    expect(company?.researchShelf?.profitability.industryStatistics.grossMargin.median).toBeCloseTo(2450 / 60);
    expect(company?.researchShelf?.profitability.industryStatistics.grossMargin.distribution).toEqual(expect.arrayContaining([
      { label: "Peer India (PEER) · FY 2025", value: expect.closeTo(5700 / 132) },
    ]));
    expect(company?.researchShelf?.profitability.industryComparisons.grossMargin[0].industryMedian).toBeCloseTo(475 / 12);
    expect(company?.researchShelf?.profitability.industryComparisons.grossMargin[0].companyAbsoluteValue).toBe(300);
    expect(company?.researchShelf?.profitability.industryComparisons.grossMargin[0].industryMedianAbsoluteValue).toBe(400);
    expect(company?.researchShelf?.profitability.industryComparisons.rdRatio[1].industryMedian).toBeCloseTo(2.5);
    expect(company?.researchShelf?.profitability.industryComparisons.rdRatio[1].industryMedianAbsoluteValue).toBeCloseTo(28.2);
    expect(company?.researchShelf?.profitability.performanceBands.grossMargin.level.observations).toBe(4);
    expect(company?.researchShelf?.profitability.performanceBands.cogsRatio.level.direction).toBe("lower");
    expect(company?.researchShelf?.profitability.performanceBands.operatingMargin.level.direction).toBe("higher");
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.grossProfit.companyValue).toBe(400);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.grossProfit.companyMarginPercent).toBe(40);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.grossProfit.industryMedianMarginPercent).toBeCloseTo(41.5909);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.ebitda.companyMarginPercent).toBe(20);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.ebit.companyMarginPercent).toBe(15);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.ebt.companyMarginPercent).toBe(13);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.netProfit.companyMarginPercent).toBe(10);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.otherOperatingExpense.companyValue).toBe(60);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.minorityInterestInEarnings.companyValue).toBe(10);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.earningsFromDiscontinuedOperations.companyValue).toBe(-5);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.other.companyValue).toBe(-25);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.other.industryMedian).toBeCloseTo(-25.7);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.netIncomeToCommon.companyValue).toBe(60);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.commonDividendsPaid.companyValue).toBe(20);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.currentYearEarningsRetained.companyValue).toBe(40);
    expect(company?.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.currentYearEarningsRetained.industryMedian).toBeCloseTo(53);
    expect(company?.researchShelf?.earningsAndValuation.valuation.enterpriseValue).toBe(5000);
    expect(company?.researchShelf?.earningsAndValuation.valuation.comparisons.evRevenue.companyValue).toBe(5);
    expect(company?.researchShelf?.earningsAndValuation.valuation.comparisons.evRevenue.industryMedian).toBe(5);
    expect(company?.researchShelf?.earningsAndValuation.valuation.comparisons.pe.companyValue).toBe(25);
    expect(company?.dataMode).toBe("research-db");
  });

  it("uses sample deviation for year-over-year growth", () => {
    const stats = growthStatistics([{ year: 2022, value: 100 }, { year: 2023, value: 110 }, { year: 2024, value: 132 }]);
    expect(stats.median).toBeCloseTo(15);
    expect(stats.standardDeviation).toBeCloseTo(Math.sqrt(50));
    expect(stats.totalChange).toBeCloseTo(32);
  });

  it("recalculates every industry benchmark from a custom constituent basket", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify([{ id: 44, name: "Example India", ticker: "EXAMPLE", country: "India", industry_bucket: "Industrial Products" }]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { company_id: 44, statement_key: "income", fact_key: "revenue", label: "Revenue", unit_kind: "amount", fiscal_year: 2024, value: 8000 },
        { company_id: 44, statement_key: "income", fact_key: "revenue", label: "Revenue", unit_kind: "amount", fiscal_year: 2025, value: 10000 },
        { company_id: 44, statement_key: "income", fact_key: "costOfRevenue", label: "Cost of revenue", unit_kind: "amount", fiscal_year: 2024, value: 5000 },
        { company_id: 44, statement_key: "income", fact_key: "costOfRevenue", label: "Cost of revenue", unit_kind: "amount", fiscal_year: 2025, value: 6000 },
        { company_id: 44, statement_key: "income", fact_key: "operatingIncome", label: "Operating income", unit_kind: "amount", fiscal_year: 2024, value: 1200 },
        { company_id: 44, statement_key: "income", fact_key: "operatingIncome", label: "Operating income", unit_kind: "amount", fiscal_year: 2025, value: 2000 },
      ]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([{ company_id: 44, bucket_id: 7, bucket_name: "Industrial Products" }]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { bucket_id: 8, bucket_name: "Custom source", company_id: 45, country: "India", fact_key: "revenue", fiscal_year: 2024, value: 12000 },
        { bucket_id: 8, bucket_name: "Custom source", company_id: 45, country: "India", fact_key: "revenue", fiscal_year: 2025, value: 13200 },
        { bucket_id: 8, bucket_name: "Custom source", company_id: 45, country: "India", fact_key: "costOfRevenue", fiscal_year: 2024, value: 7000 },
        { bucket_id: 8, bucket_name: "Custom source", company_id: 45, country: "India", fact_key: "costOfRevenue", fiscal_year: 2025, value: 7500 },
        { bucket_id: 8, bucket_name: "Custom source", company_id: 45, country: "India", fact_key: "operatingIncome", fiscal_year: 2024, value: 1800 },
        { bucket_id: 8, bucket_name: "Custom source", company_id: 45, country: "India", fact_key: "operatingIncome", fiscal_year: 2025, value: 1980 },
      ]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([{ id: 45, name: "Peer India", ticker: "PEER", country: "India", industry_bucket: "Custom source" }]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([]), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const company = await pullResearchCompany(env, "research-44", 2024, 2025, [45]);

    expect(company?.researchShelf?.industryConstituentsCustomized).toBe(true);
    expect(company?.researchShelf?.industryConstituents.map((item) => item.id)).toEqual(["research-45"]);
    expect(company?.researchShelf?.growthComparisons.revenue.industryBucket.median).toBeCloseTo(10);
    expect(company?.researchShelf?.industryDeltas.revenue[0]).toMatchObject({ industryMedian: 120, industryMedianChangePercent: 10 });
    expect(decodeURIComponent(String(fetchMock.mock.calls[3][0]))).toContain("company_id=in.(45)");
    expect(decodeURIComponent(String(fetchMock.mock.calls[3][0]))).not.toContain("bucket_id=in.");
  });
});
