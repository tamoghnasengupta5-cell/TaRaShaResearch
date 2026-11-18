import { Fragment, useId, useState } from "react";
import type { CompanyNcwcCalculation } from "./types";

const amount = (value: number | null) => value === null || !Number.isFinite(value)
  ? "—" : value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

export function NonCashWorkingCapital({ calculation, displayUnit }: {
  calculation: CompanyNcwcCalculation; displayUnit: string;
}) {
  const id = useId();
  const priorLabel = calculation.priorPeriodLabel ?? `FY${calculation.priorFiscalYear}`;
  const currentLabel = calculation.currentPeriodLabel ?? `FY${calculation.currentFiscalYear}`;
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  return <section className="cash-ncwc-calculation" aria-label="Non-cash working capital calculation">
    <header><h4>Non-cash working capital</h4><span>NCWC</span></header>
    <p className="cash-ncwc-formula">{calculation.formula}</p>
    {calculation.inputs.some(input => input.priorDetailStatus === "classification_proxy" || input.currentDetailStatus === "classification_proxy") && <p className="cash-ncwc-basis">This calculation uses an NCWC classification proxy where the filing does not report a complete current classification. These amounts are separate from reported current balances. Expand the marked rows to review the assumptions, components and SEC sources.</p>}
    <table className="cash-ncwc-table">
      <caption>Period-end balances · {displayUnit}</caption>
      <colgroup><col /><col /><col /></colgroup>
      <thead><tr><th scope="col">Component</th><th scope="col"><small>{calculation.periodType === "ttm" ? "Opening balance" : "Prior year"}</small>{priorLabel}</th><th scope="col"><small>{calculation.periodType === "ttm" ? "Closing balance" : "Current year"}</small>{currentLabel}</th></tr></thead>
      {calculation.inputs.map(input => <Fragment key={input.key}>
        <tbody><tr className="cash-ncwc-input"><th scope="row">
          <button type="button" id={`${id}-${input.key}-toggle`} aria-expanded={!!expanded[input.key]} aria-controls={`${id}-${input.key}-components`}
            onClick={() => setExpanded(previous => ({ ...previous, [input.key]: !previous[input.key] }))}>
            <span className="cash-ncwc-caret" aria-hidden="true">›</span><span aria-hidden="true">{input.operator}</span><span>{input.label}</span>
          </button>
        </th><td>{amount(input.priorValue)}</td><td>{amount(input.currentValue)}</td></tr></tbody>
        <tbody className="cash-ncwc-components" id={`${id}-${input.key}-components`} aria-labelledby={`${id}-${input.key}-toggle`} hidden={!expanded[input.key]}>
          {input.components.map(component => <tr key={component.key}><th scope="row">{component.label}</th>
            <td>{component.priorSourceUrl ? <a href={component.priorSourceUrl} target="_blank" rel="noreferrer" aria-label={`${component.label}, ${priorLabel}, SEC source`}>{amount(component.priorValue)}</a> : amount(component.priorValue)}</td>
            <td>{component.currentSourceUrl ? <a href={component.currentSourceUrl} target="_blank" rel="noreferrer" aria-label={`${component.label}, ${currentLabel}, SEC source`}>{amount(component.currentValue)}</a> : amount(component.currentValue)}</td>
          </tr>)}
          {!input.components.length && <tr className="cash-ncwc-note"><td colSpan={3}>No separate component detail reported.</td></tr>}
          {(input.priorNote || input.currentNote) && <tr className="cash-ncwc-note"><td colSpan={3}>
            {input.priorNote && <span>{priorLabel}: {input.priorNote}</span>}
            {input.currentNote && <span>{currentLabel}: {input.currentNote}</span>}
          </td></tr>}
        </tbody>
        {input.key === "cash_input" && <tbody><tr className="cash-ncwc-intermediate"><th scope="row">Operating current assets</th><td>{amount(calculation.priorOperatingCurrentAssets)}</td><td>{amount(calculation.currentOperatingCurrentAssets)}</td></tr></tbody>}
      </Fragment>)}
      <tbody><tr className="cash-ncwc-total"><th scope="row">NCWC</th><td>{amount(calculation.priorNcwc)}</td><td>{amount(calculation.currentNcwc)}</td></tr></tbody>
    </table>
    <div className={`cash-ncwc-equation ${calculation.cashEffect === null ? "unavailable" : calculation.cashEffect < 0 ? "outflow" : ""}`}>
      <div><small>Prior NCWC − current NCWC</small><strong>{amount(calculation.priorNcwc)} − {amount(calculation.currentNcwc)} = {calculation.cashEffect !== null && calculation.cashEffect > 0 ? "+" : ""}{amount(calculation.cashEffect)}</strong></div>
      <span>{calculation.cashEffect === null ? "Unavailable" : calculation.cashEffect > 0 ? "Cash released" : calculation.cashEffect < 0 ? "Cash invested" : "No cash change"}</span>
    </div>
    <p className="cash-ncwc-basis">{calculation.cashInputPolicy === "cash_and_cash_equivalents_plus_current_investments"
      ? "Cash input: cash and cash equivalents plus current investments. Expand the row to see both components."
      : "Cash input: cash and cash equivalents only. This saved story uses the earlier cash-only calculation."}</p>
    {calculation.inputs.some(input => input.priorDetailStatus === "assumed_zero" || input.currentDetailStatus === "assumed_zero") && <p className="cash-ncwc-basis">Unreported current debt is assumed zero for this calculation; this is not an SEC-reported zero.</p>}
    {calculation.reason && <details className="cash-ncwc-gaps"><summary>Working-capital input availability</summary><p className="cash-ncwc-basis">{calculation.reason.replaceAll("current_assets", "current assets").replaceAll("cash_input", "cash and current investments").replaceAll("current_liabilities", "current liabilities").replaceAll("current_debt", "current debt")}</p></details>}
  </section>;
}
