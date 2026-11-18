import { useId, useState } from "react";
import type { ReactNode } from "react";
import type { Company, StatementFact, StatementGroup } from "./types";

export function formatStatementValue(company: Company, fact: StatementFact, value: number): string {
  if (!fact.unit) return "—";
  if (fact.unit === "ratio") return fact.displayFormat === "percentage"
    ? `${(value * 100).toFixed(2)}%` : value.toFixed(2);
  if (fact.unit === "%") return `${value.toFixed(2)}%`;
  if (fact.unit === "USD per share" || fact.unit === "US$ per share") return `$${value.toFixed(2)}`;
  if (fact.unit === "USD/shares") return `$${value.toFixed(2)}`;
  if (fact.unit.endsWith(" per share")) return `${value.toFixed(2)} ${fact.unit.replace(" per share", "")}`;
  if (fact.unit === "million shares") return `${value.toLocaleString("en-US", { maximumFractionDigits: 1 })}m`;
  if (fact.unit === "₹ crore") return `${value < 0 ? "−" : ""}₹${Math.abs(value).toLocaleString("en-IN", { maximumFractionDigits: 1 })} cr`;
  const monetary = fact.unit.match(/^(US\$|[A-Z]{3}) (thousands?|millions?|billions?|crores?)$/);
  const monetaryCurrency = monetary?.[1];
  if (monetaryCurrency ? !["USD", "US$"].includes(monetaryCurrency) : !company.currency.startsWith("US$")) return `${value.toLocaleString("en-IN", { maximumFractionDigits: 1 })}`;
  if (monetaryCurrency) {
    const suffix = monetary![2].startsWith("thousand") ? "k" : monetary![2].startsWith("billion") ? "B"
      : monetary![2].startsWith("crore") ? " cr" : "m";
    return `${value < 0 ? "−" : ""}$${Math.abs(value).toLocaleString("en-US", { maximumFractionDigits: 1 })}${suffix}`;
  }
  return value.toLocaleString("en-US", { maximumFractionDigits: 2 });
}

const tabLabels = { income: "Income statement", balance: "Balance sheet", cash: "Cash flow", shares: "Shares", ratios: "Ratios", kpis: "KPIs" } as const;

function sourceTitle(source: { periodEnd?: string; periodStart?: string | null; provenance?: Record<string, unknown> | null }) {
  const provenance = source.provenance;
  return [source.periodStart && `From ${source.periodStart}`, source.periodEnd && `Through ${source.periodEnd}`,
    provenance?.accession && `Accession ${provenance.accession}`, provenance?.filed_date && `Filed ${provenance.filed_date}`,
    provenance?.concept, provenance?.formula, provenance?.calculation_disclosure].filter(Boolean).join(" · ");
}

export function StatementExplorer({ company, initialGroupKey }: { company: Company; initialGroupKey?: StatementGroup["key"] }) {
  const id = useId();
  const supplied = company.statements ?? [];
  const groups: StatementGroup[] = (Object.keys(tabLabels) as Array<keyof typeof tabLabels>).map(key =>
    supplied.find(group => group.key === key) ?? { key, label: tabLabels[key], facts: [] });
  const first = supplied.find(group => group.facts.length)?.key ?? "income";
  const [activeKey, setActiveKey] = useState<StatementGroup["key"]>(initialGroupKey ?? first);
  const active = groups.find(group => group.key === activeKey)!;
  const years = [...new Set([...(active.annualPeriods ?? []).map(end => Number(end.slice(0, 4))),
    ...active.facts.flatMap(fact => [...fact.values.map(value => value.year), ...(fact.annualSources ?? []).map(source => source.year)])])]
    .filter(Number.isFinite).sort((a, b) => a - b);
  const tableHeader = <thead><tr><th scope="col">Reported fact</th>{years.map(year => <th scope="col" key={year}>FY {year}</th>)}<th scope="col">TTM<small>{active.ttmPeriodEnd ? `Ended ${active.ttmPeriodEnd}` : "Unavailable"}</small></th></tr></thead>;
  const factRow = (fact: StatementFact) => {
    const values = new Map(fact.values.map(value => [value.year, value.value]));
    const trailing = fact.ttm?.periodEnd === active.ttmPeriodEnd ? fact.ttm : null;
    const reason = (type: string, period: number | string) => fact.periodScope?.find(scope => scope.period.type === type
      && (type === "annual" ? Number(scope.period.end.slice(0, 4)) === period : scope.period.end === period))?.reason;
    return <tr key={fact.key}><td><strong>{fact.label}</strong><small>{fact.description}<br />{fact.unit}</small></td>{years.map(year => {
      const source = fact.annualSources?.find(source => source.year === year);
      const available = values.has(year);
      const displayFact = source ? { ...fact, unit: source.displayUnit === "%" ? "ratio" : source.displayUnit === "shares millions" ? "million shares" : source.displayUnit,
        displayFormat: source.displayFormat ?? fact.displayFormat } : fact;
      return <td key={year} title={available ? source && sourceTitle(source) : source?.reason ?? reason("annual", year)}>
        {available ? <>{formatStatementValue(company, displayFact, values.get(year)!)}{source?.sourceUrl && <small><a href={source.sourceUrl} target="_blank" rel="noreferrer">Filing</a></small>}</> : "—"}
      </td>;
    })}<td title={trailing?.value == null ? trailing?.reason ?? reason("ttm", active.ttmPeriodEnd ?? "") : sourceTitle(trailing)}>{trailing?.value != null ? <><span>{formatStatementValue(company, { ...fact, unit: trailing.unit, displayFormat: trailing.displayFormat }, trailing.value)}</span><small>{trailing.unit}{trailing.sourceUrl && <> · <a href={trailing.sourceUrl} target="_blank" rel="noreferrer">Filing</a></>}</small></> : "—"}</td></tr>;
  };
  const roots = active.facts.filter(fact => !fact.parentKey || !active.facts.some(parent => parent.key === fact.parentKey));
  const childrenFor = (fact: StatementFact, seen = new Set<string>()): ReactNode[] => {
    if (seen.has(fact.key)) return [];
    const next = new Set([...seen, fact.key]);
    const children = active.facts.filter(child => child.parentKey === fact.key && !next.has(child.key));
    return [factRow(fact), children.length ? <tr key={`${fact.key}-components`}><td colSpan={years.length + 2}><details><summary>Reported components</summary><table>{tableHeader}<tbody>{children.flatMap(child => childrenFor(child, next))}</tbody></table></details></td></tr> : null];
  };
  return <section className="statement-explorer financial-statements">
    <div className="statement-heading"><div><p className="eyebrow">{company.dataMode === "tarasha-data" ? "Source-linked financial history" : "Illustrative financial history"}</p><h2>Financials</h2><p>Annual fiscal years and the latest available TTM. A dash means the source-backed value is unavailable. Percentages and ratios display two decimal places; source values retain their original precision.</p></div></div>
    <div className="statement-tabs" role="tablist" aria-label="Financial tables">{groups.map((group, index) => <button type="button" role="tab"
      id={`${id}-${group.key}-tab`} aria-controls={`${id}-${group.key}-panel`} aria-selected={group.key === activeKey} tabIndex={group.key === activeKey ? 0 : -1}
      className={group.key === activeKey ? "active" : ""} onClick={() => setActiveKey(group.key)} key={group.key}
      onKeyDown={event => {
        const next = event.key === "ArrowRight" ? (index + 1) % groups.length : event.key === "ArrowLeft" ? (index + groups.length - 1) % groups.length
          : event.key === "Home" ? 0 : event.key === "End" ? groups.length - 1 : null;
        if (next === null) return;
        event.preventDefault(); setActiveKey(groups[next].key);
        document.getElementById(`${id}-${groups[next].key}-tab`)?.focus();
      }}>{tabLabels[group.key]}<small>{group.facts.length ? `${group.facts.length} facts` : "Unavailable"}</small></button>)}</div>
    <div role="tabpanel" id={`${id}-${active.key}-panel`} aria-labelledby={`${id}-${active.key}-tab`} tabIndex={0}>
      {active.facts.length ? <div className="statement-table-wrap" data-horizontal-scroll><table>{tableHeader}<tbody>{roots.flatMap(fact => childrenFor(fact))}</tbody></table></div>
        : <p className="financials-unavailable">TaRaShaData has not returned source-backed {tabLabels[active.key].toLowerCase()} for this company.</p>}
    </div>
    <div className="filing-and-limits"><details><summary>Source filings</summary>{company.filings?.length ? <div className="filing-list">{company.filings.map(filing => <a href={filing.url} target="_blank" rel="noreferrer" key={filing.accession}><span>{filing.form}</span><div><strong>{filing.title}</strong><small>Filed {filing.filed} · Period {filing.period || "not stated"}</small></div><b>↗</b></a>)}</div> : <p>No filing links were returned for this range.</p>}</details></div>
  </section>;
}
