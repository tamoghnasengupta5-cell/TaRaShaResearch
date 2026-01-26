import streamlit as st

from core import *  # noqa: F401,F403

# --- Key Data display helpers (country, units) ---
# Money values in this module are assumed to be stored in *millions of local currency*.
# Display rule:
#   - USA: show in USD Millions ($ M)  -> scale = 1.0
#   - India: show in INR Crores (₹ Cr) -> 1 Cr = 10 M, so scale = 0.1
# Other countries default to "Millions" (M) with their currency symbol where obvious.


def _canonicalize_country(country: Optional[str]) -> str:
    """Normalize country values used by the app to the canonical set."""
    if country is None:
        return "USA"
    c = str(country).strip()
    if not c:
        return "USA"
    cu = c.upper()

    if cu in ("USA", "US", "UNITED STATES", "UNITED STATES OF AMERICA"):
        return "USA"
    if cu in ("INDIA", "IN", "REPUBLIC OF INDIA"):
        return "India"
    if cu in ("CHINA", "CN", "PRC", "PEOPLE'S REPUBLIC OF CHINA", "PEOPLES REPUBLIC OF CHINA"):
        return "China"
    if cu in ("JAPAN", "JP"):
        return "Japan"
    if cu in ("UK", "UNITED KINGDOM", "GREAT BRITAIN", "BRITAIN", "GB"):
        return "UK"
    if cu in ("UAE", "UNITED ARAB EMIRATES", "AE"):
        return "UAE"

    # Unknown country string: keep as-is (trimmed)
    return c


def _get_company_country(conn: sqlite3.Connection, company_id: int) -> str:
    """Return the stored country for the company, falling back to USA."""
    try:
        row = conn.execute("SELECT country FROM companies WHERE id = ?", (company_id,)).fetchone()
        if row is not None and row[0] is not None and str(row[0]).strip():
            return _canonicalize_country(str(row[0]))
    except Exception:
        pass
    return "USA"

_COUNTRY_TO_FLAG = {
    "USA": "🇺🇸",
    "India": "🇮🇳",
    "China": "🇨🇳",
    "Japan": "🇯🇵",
    "UK": "🇬🇧",
    "UAE": "🇦🇪",
}

_COUNTRY_TO_UNIT = {
    "USA": "$ M",
    "India": "₹ Cr",
    "China": "CN¥ M",
    "Japan": "JP¥ M",
    "UK": "£ M",
    "UAE": "AED M",
}

def _country_display_and_units(country: Optional[str]) -> Tuple[str, str, float]:
    c = _canonicalize_country(country)
    flag = _COUNTRY_TO_FLAG.get(c, "🏳️")
    unit = _COUNTRY_TO_UNIT.get(c, "M")
    scale = 0.1 if c == "India" else 1.0
    return f"{flag} {c}", unit, scale

def _fmt_money_by_country(x: Optional[float], country: Optional[str]) -> str:
    if x is None or pd.isna(x):
        return "—"
    try:
        xv = float(x)
    except Exception:
        return str(x)
    _, _, scale = _country_display_and_units(country)
    return f"{xv * scale:,.2f}"


def _apply_key_filter(disp: pd.DataFrame, widget_key: str) -> pd.DataFrame:
    """Apply a Key filter to a Key Data table.

    - Multi-select keys (default = all).
    - 'Growth%' rows are grouped under the most recent non-'Growth%' key within each company block,
      so selecting a metric (e.g., 'Revenue') keeps its associated 'Growth%' row(s) as well.
    """
    if disp is None or disp.empty:
        return disp
    if "Key" not in disp.columns or "Company" not in disp.columns:
        return disp

    companies = disp["Company"].astype(str).tolist()
    keys = disp["Key"].astype(str).tolist()

    key_groups = []
    options = []
    seen = set()

    last_company = None
    last_non_growth = None
    for c, k in zip(companies, keys):
        if c != last_company:
            last_company = c
            last_non_growth = None
        if k != "Growth%":
            last_non_growth = k
        g = last_non_growth if last_non_growth is not None else k
        key_groups.append(g)
        if g not in seen and str(g).strip() != "":
            options.append(g)
            seen.add(g)

    if not options:
        return disp

    selected = st.multiselect(
        "Filter Keys",
        options=options,
        default=options,
        key=widget_key,
    )

    disp2 = disp.copy()
    disp2["_KeyGroup"] = key_groups

    if selected:
        disp2 = disp2[disp2["_KeyGroup"].isin(selected)].copy()
    else:
        disp2 = disp2.iloc[0:0].copy()

    disp2 = disp2.drop(columns=["_KeyGroup"])
    return disp2

def render_key_data_tab() -> None:
    """Master tab: Key Data."""
    st.title("Key Data")

    tab_pl_key, tab_bs_key, tab_cs_key, tab_cf_key = st.tabs(
        ["P&L Key data", "Balance Sheet Key Data", "Capital Structure & Spread", "Cash Flow & Reinvestment"]
    )
    with tab_pl_key:
        _render_pl_key_data()
    with tab_bs_key:
        _render_bs_key_data()
    with tab_cs_key:
        _render_cs_spread_key_data()
    with tab_cf_key:
        _render_cf_reinvestment_key_data()


def _render_pl_key_data() -> None:
    st.subheader("P&L Key data")

    conn = get_db()
    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies in the database yet. Upload a spreadsheet under Equity Research → Data Upload.")
        return

    # Base selection: individual companies
    all_company_options = [
        f"{row.name} ({row.ticker}) [id={row.id}]" for _, row in companies_df.iterrows()
    ]

    # Default selection: companies not present in any bucket (same behavior as P&L Metrics)
    try:
        bucket_df = pd.read_sql_query(
            "SELECT DISTINCT company_id FROM company_group_members",
            conn,
        )
        bucketed_ids = set(int(x) for x in bucket_df["company_id"].tolist()) if not bucket_df.empty else set()
    except Exception:
        bucketed_ids = set()

    if bucketed_ids:
        default_company_options = [
            f"{row.name} ({row.ticker}) [id={row.id}]"
            for _, row in companies_df.iterrows()
            if int(row.id) not in bucketed_ids
        ]
    else:
        default_company_options = all_company_options

    # If a company was just ingested this session, override default to only that company
    last_company_id = st.session_state.get("last_ingested_company_id")
    if last_company_id is not None:
        selected_label = None
        for _, row in companies_df.iterrows():
            if int(row.id) == int(last_company_id):
                selected_label = f"{row.name} ({row.ticker}) [id={row.id}]"
                break
        if selected_label:
            default_company_options = [selected_label]

    options = st.multiselect(
        "Companies to analyze",
        options=all_company_options,
        default=default_company_options,
        key="keydata_pl_companies_select",
    )

    # Bucket definition: allow user to save current selection as a named bucket
    bucket_col1, bucket_col2 = st.columns([3, 1])
    with bucket_col1:
        bucket_name = st.text_input(
            "Save current selection as bucket (optional)",
            placeholder="e.g. US_SaaS_Leaders",
            key="keydata_pl_bucket_name_input",
        )
    with bucket_col2:
        save_bucket = st.button("Save bucket", key="keydata_pl_save_bucket_button")

    if save_bucket:
        sel_company_ids = []
        for opt in options:
            m_sel = re.search(r"\[id=(\d+)\]$", opt)
            if m_sel:
                sel_company_ids.append(int(m_sel.group(1)))

        if not bucket_name or not bucket_name.strip():
            st.error("Please provide a non-empty bucket name.")
        elif not sel_company_ids:
            st.error("Please select at least one company before saving a bucket.")
        else:
            bname = bucket_name.strip()
            cur = conn.cursor()
            # Create bucket if it does not exist
            cur.execute(
                "INSERT OR IGNORE INTO company_groups(name) VALUES(?)",
                (bname,),
            )
            cur.execute(
                "SELECT id FROM company_groups WHERE name = ?",
                (bname,),
            )
            row = cur.fetchone()
            if row is None:
                st.error("Unexpected error while creating bucket.")
            else:
                gid = int(row[0])
                # Append current selection to existing membership (keep existing companies in the bucket)
                cur.executemany(
                    "INSERT OR IGNORE INTO company_group_members(group_id, company_id) VALUES(?, ?)",
                    [(gid, cid) for cid in sel_company_ids],
                )
                conn.commit()
                st.success(f"Saved bucket '{bname}' with {len(sel_company_ids)} companies.")

    # Bucket selection: use previously saved buckets to drive the analysis
    groups_df = pd.read_sql_query(
        "SELECT id, name FROM company_groups ORDER BY name",
        conn,
    )
    bucket_names_selected = []
    group_name_to_id: Dict[str, int] = {}
    if not groups_df.empty:
        group_name_to_id = {str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()}
        bucket_names_selected = st.multiselect(
            "Or select one or more saved buckets",
            options=list(group_name_to_id.keys()),
            key="keydata_pl_bucket_select",
        )

    yr_input = st.text_input(
        "Year range (e.g., 'Recent - 2020' or '2023-2018')",
        value="Recent - 2020",
        key="keydata_pl_year_range",
    )

    stdev_mode = st.radio(
        "Standard deviation mode",
        ["Sample (ddof=1)", "Population (ddof=0)"],
        horizontal=True,
        key="keydata_pl_stdev_mode",
    )
    sample = (stdev_mode == "Sample (ddof=1)")

    def parse_range(s: str, available_years: List[int]) -> Tuple[int, int]:
        s = (s or "").strip()
        if not available_years:
            raise ValueError("No annual years available.")
        most_recent = max(available_years)
        m_recent = re.match(r"^recent\s*[-–]\s*(\d{4})$", s, flags=re.IGNORECASE)
        m_two = re.match(r"^(\d{4})\s*[-–]\s*(\d{4})$", s)
        if m_recent:
            end = int(m_recent.group(1))
            return most_recent, end
        if m_two:
            start, end = int(m_two.group(1)), int(m_two.group(2))
            return start, end
        raise ValueError("Could not parse the year range. Use 'Recent - YYYY' or 'YYYY-YYYY'.")

    # Determine final set of company ids based on direct selection + buckets
    selected_company_ids: List[int] = []
    for opt in options:
        m = re.search(r"\[id=(\d+)\]$", opt)
        if m:
            selected_company_ids.append(int(m.group(1)))

    bucket_company_ids: List[int] = []
    if bucket_names_selected:
        group_ids = [group_name_to_id[name] for name in bucket_names_selected]
        placeholders = ",".join(["?"] * len(group_ids))
        bucket_df2 = pd.read_sql_query(
            f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
            conn,
            params=group_ids,
        )
        if not bucket_df2.empty:
            bucket_company_ids = [int(x) for x in bucket_df2["company_id"].tolist()]

    all_company_ids = sorted(set(selected_company_ids + bucket_company_ids))
    if not all_company_ids:
        st.info("Select at least one company or a saved bucket to view P&L key data.")
        return

    def fmt_money(x: Optional[float], country: Optional[str]) -> str:
        return _fmt_money_by_country(x, country)

    def fmt_pct_from_decimal(x: Optional[float]) -> str:
        if x is None or pd.isna(x):
            return "—"
        try:
            return f"{float(x) * 100.0:.2f}%"
        except Exception:
            return str(x)
    st.markdown("---")

    st.caption("Money values are displayed based on the company\'s country in the database: 🇺🇸 USA = USD Millions ($ M); 🇮🇳 India = INR Crores (₹ Cr).")

    # Build a horizontal table (years as columns) for the selected companies/buckets.
    # This keeps the same underlying calculations but pivots the display to match the requested layout.
    def get_annual_operating_income_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
        return pd.read_sql_query(
            """
            SELECT fiscal_year AS year, operating_income
            FROM operating_income_annual
            WHERE company_id = ?
            ORDER BY year
            """, conn, params=(company_id,)
        )

    def build_metric(
        ann_df: pd.DataFrame,
        value_col: str,
        yr_start: int,
        yr_end: int,
        stdev_sample: bool,
    ) -> Tuple[Dict[int, Optional[float]], Dict[int, Optional[float]], Optional[float], Optional[float]]:
        """Return (values_by_year, yoy_growth_by_year, median_growth, stdev_growth)."""
        if ann_df is None or ann_df.empty:
            return {}, {}, None, None
        df_m = ann_df.copy()
        df_m = df_m[(df_m["year"] >= yr_end) & (df_m["year"] <= yr_start)].sort_values("year")
        if df_m.empty or value_col not in df_m.columns:
            return {}, {}, None, None

        # YoY growth for display (keeps the same approach as Revenue).
        df_m["yoy_growth"] = df_m[value_col].astype(float).pct_change()
        med_g, std_g = compute_growth_stats(
            ann_df,
            yr_start,
            yr_end,
            stdev_sample=stdev_sample,
            value_col=value_col,
        )

        val_by_year = {int(y): (None if pd.isna(v) else float(v)) for y, v in zip(df_m["year"], df_m[value_col])}
        growth_by_year = {
            int(y): (None if pd.isna(g) else float(g)) for y, g in zip(df_m["year"], df_m["yoy_growth"])
        }
        return val_by_year, growth_by_year, med_g, std_g

    per_company = []
    year_set = set()

    for cid in all_company_ids:
        ann_rev = get_annual_series(conn, cid)
        if ann_rev.empty:
            continue

        try:
            yr_start, yr_end = parse_range(yr_input, ann_rev["year"].tolist())
            if yr_start < yr_end:
                yr_start, yr_end = yr_end, yr_start
        except Exception as e:
            st.error(f"Year range error: {e}")
            return

        # Revenue (baseline key) - unchanged calculations
        df_rev = ann_rev.copy()
        df_rev = df_rev[(df_rev["year"] >= yr_end) & (df_rev["year"] <= yr_start)].sort_values("year")
        if df_rev.empty:
            continue

        df_rev["yoy_growth"] = df_rev["revenue"].astype(float).pct_change()
        med_rev_g, std_rev_g = compute_growth_stats(
            ann_rev,
            yr_start,
            yr_end,
            stdev_sample=sample,
            value_col="revenue",
        )

        # Company label
        rowc = companies_df[companies_df["id"] == cid]
        if not rowc.empty:
            name = rowc.iloc[0]["name"]
            ticker = rowc.iloc[0]["ticker"]
        else:
            name = f"Company {cid}"
            ticker = ""

        country = _get_company_country(conn, cid)
        country_disp, unit_label, _ = _country_display_and_units(country)

        rev_by_year = {int(y): (None if pd.isna(v) else float(v)) for y, v in zip(df_rev["year"], df_rev["revenue"])}
        growth_by_year = {int(y): (None if pd.isna(g) else float(g)) for y, g in zip(df_rev["year"], df_rev["yoy_growth"])}

        # Preserve existing column behavior: year columns are driven by Revenue years
        year_set.update(rev_by_year.keys())

        # Additional keys
        pretax_by_year, pretax_growth_by_year, pretax_med_g, pretax_std_g = build_metric(
            get_annual_pretax_income_series(conn, cid),
            "pretax_income",
            yr_start,
            yr_end,
            sample,
        )
        net_by_year, net_growth_by_year, net_med_g, net_std_g = build_metric(
            get_annual_net_income_series(conn, cid),
            "net_income",
            yr_start,
            yr_end,
            sample,
        )
        nopat_by_year, nopat_growth_by_year, nopat_med_g, nopat_std_g = build_metric(
            get_annual_nopat_series(conn, cid),
            "nopat",
            yr_start,
            yr_end,
            sample,
        )

        # Operating margin (computed from Operating Income / Revenue)
        opm_by_year: Dict[int, Optional[float]] = {}
        opm_growth_by_year: Dict[int, Optional[float]] = {}
        opm_median: Optional[float] = None
        opm_stdev: Optional[float] = None
        opm_values_are_fraction: bool = True
        opm_med_g: Optional[float] = None
        opm_std_g: Optional[float] = None

        ann_opinc = get_annual_operating_income_series(conn, cid)
        if not ann_opinc.empty:
            df_opinc = ann_opinc.copy()
            df_opinc = df_opinc[(df_opinc["year"] >= yr_end) & (df_opinc["year"] <= yr_start)].sort_values("year")
            if not df_opinc.empty:
                merged = pd.merge(
                    df_rev[["year", "revenue"]],
                    df_opinc[["year", "operating_income"]],
                    on="year",
                    how="inner",
                )
                merged = merged[pd.notna(merged["revenue"]) & pd.notna(merged["operating_income"])]
                merged = merged[merged["revenue"].astype(float) != 0.0]
                if not merged.empty:
                    merged = merged.sort_values("year")
                    merged["margin"] = merged["operating_income"].astype(float) / merged["revenue"].astype(float)
                    margins = merged[["year", "margin"]].copy()
                    opm_by_year = {int(y): (None if pd.isna(v) else float(v)) for y, v in zip(margins["year"], margins["margin"])}

                    # YoY growth for margin uses abs(previous) denominator (same as margin growth stats helper)
                    prev_val = None
                    for y, v in zip(margins["year"], margins["margin"]):
                        yy = int(y)
                        if pd.isna(v):
                            opm_growth_by_year[yy] = None
                            prev_val = None
                            continue
                        cur_val = float(v)
                        if prev_val is None:
                            opm_growth_by_year[yy] = None
                        else:
                            denom = abs(prev_val)
                            opm_growth_by_year[yy] = None if denom == 0.0 else (cur_val - prev_val) / denom
                        prev_val = cur_val

                    opm_median, opm_stdev, opm_values_are_fraction = compute_margin_stats(
                        margins,
                        yr_start,
                        yr_end,
                        stdev_sample=sample,
                    )
                    opm_med_g, opm_std_g = compute_margin_growth_stats(
                        margins,
                        yr_start,
                        yr_end,
                        stdev_sample=sample,
                    )

        per_company.append(
            {
                "name": str(name),
                "ticker": str(ticker),
                "country": str(country),
                "country_disp": str(country_disp),
                "unit_label": str(unit_label),
                "rev_by_year": rev_by_year,
                "growth_by_year": growth_by_year,
                "median_growth": med_rev_g,
                "stdev_growth": std_rev_g,
                "pretax_by_year": pretax_by_year,
                "pretax_growth_by_year": pretax_growth_by_year,
                "pretax_median_growth": pretax_med_g,
                "pretax_stdev_growth": pretax_std_g,
                "net_by_year": net_by_year,
                "net_growth_by_year": net_growth_by_year,
                "net_median_growth": net_med_g,
                "net_stdev_growth": net_std_g,
                "nopat_by_year": nopat_by_year,
                "nopat_growth_by_year": nopat_growth_by_year,
                "nopat_median_growth": nopat_med_g,
                "nopat_stdev_growth": nopat_std_g,
                "opm_by_year": opm_by_year,
                "opm_growth_by_year": opm_growth_by_year,
                "opm_median": opm_median,
                "opm_stdev": opm_stdev,
                "opm_values_are_fraction": opm_values_are_fraction,
                "opm_median_growth": opm_med_g,
                "opm_stdev_growth": opm_std_g,
            }
        )
    if not per_company:
        st.info("No annual revenue data found for the selected companies in the chosen year range.")
        return

    year_cols = sorted(year_set)

    def fmt_margin_value(x: Optional[float], values_are_fraction: bool) -> str:
        if x is None or pd.isna(x):
            return "—"
        try:
            xv = float(x)
        except Exception:
            return str(x)
        if values_are_fraction:
            return fmt_pct_from_decimal(xv)
        return f"{xv:.2f}%"

    rows = []
    for item in per_company:
        # Revenue
        r = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Revenue",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            r[y] = fmt_money(item["rev_by_year"].get(y), item["country"])
        rows.append(r)

        g = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item["median_growth"]),
            "Standard Deviation": fmt_pct_from_decimal(item["stdev_growth"]),
        }
        for y in year_cols:
            gv = item["growth_by_year"].get(y)
            g[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(g)

        # Pretax Income
        p = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Pretax Income",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            p[y] = fmt_money(item["pretax_by_year"].get(y), item["country"])
        rows.append(p)

        pg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item["pretax_median_growth"]),
            "Standard Deviation": fmt_pct_from_decimal(item["pretax_stdev_growth"]),
        }
        for y in year_cols:
            gv = item["pretax_growth_by_year"].get(y)
            pg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(pg)

        # Net Income
        n = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Net Income",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            n[y] = fmt_money(item["net_by_year"].get(y), item["country"])
        rows.append(n)

        ng = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item["net_median_growth"]),
            "Standard Deviation": fmt_pct_from_decimal(item["net_stdev_growth"]),
        }
        for y in year_cols:
            gv = item["net_growth_by_year"].get(y)
            ng[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(ng)

        # NOPAT
        no = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "NOPAT",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            no[y] = fmt_money(item["nopat_by_year"].get(y), item["country"])
        rows.append(no)

        nog = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item["nopat_median_growth"]),
            "Standard Deviation": fmt_pct_from_decimal(item["nopat_stdev_growth"]),
        }
        for y in year_cols:
            gv = item["nopat_growth_by_year"].get(y)
            nog[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(nog)

        # Operating Margin
        values_are_fraction = bool(item.get("opm_values_are_fraction", True))
        opm = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Operating Margin",
            "Median": fmt_margin_value(item.get("opm_median"), values_are_fraction),
            "Standard Deviation": fmt_margin_value(item.get("opm_stdev"), values_are_fraction),
        }
        for y in year_cols:
            opm[y] = fmt_margin_value(item["opm_by_year"].get(y), values_are_fraction)
        rows.append(opm)

        opmg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item.get("opm_median_growth")),
            "Standard Deviation": fmt_pct_from_decimal(item.get("opm_stdev_growth")),
        }
        for y in year_cols:
            gv = item["opm_growth_by_year"].get(y)
            opmg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(opmg)
    # Final display
    ordered_cols = ["Company", "Ticker", "Country", "Units", "Key"] + year_cols + ["Median", "Standard Deviation"]
    disp = pd.DataFrame(rows)
    disp = disp.reindex(columns=ordered_cols)

    # Visually "merge" Company/Ticker cells (Excel-like) by blanking repeated values
    # for consecutive rows belonging to the same company.
    disp = _apply_key_filter(disp, "keydata_pl_key_filter")
    dup_mask = disp["Company"].eq(disp["Company"].shift())
    disp.loc[dup_mask, ["Company", "Ticker", "Country", "Units"]] = ""

    st.dataframe(disp, use_container_width=True)


def _render_bs_key_data() -> None:
    st.subheader("Balance Sheet Key Data")

    conn = get_db()
    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies in the database yet. Upload a spreadsheet under Equity Research → Data Upload.")
        return

    # Base selection: individual companies
    all_company_options = [
        f"{row.name} ({row.ticker}) [id={row.id}]" for _, row in companies_df.iterrows()
    ]

    # Default selection: companies not present in any bucket (same behavior as P&L Metrics)
    try:
        bucket_df = pd.read_sql_query(
            "SELECT DISTINCT company_id FROM company_group_members",
            conn,
        )
        bucketed_ids = set(int(x) for x in bucket_df["company_id"].tolist()) if not bucket_df.empty else set()
    except Exception:
        bucketed_ids = set()

    if bucketed_ids:
        default_company_options = [
            f"{row.name} ({row.ticker}) [id={row.id}]"
            for _, row in companies_df.iterrows()
            if int(row.id) not in bucketed_ids
        ]
    else:
        default_company_options = all_company_options[:]

    options = st.multiselect(
        "Select one or more companies",
        options=all_company_options,
        default=default_company_options,
        key="keydata_bs_company_select",
    )

    # Save bucket (optional)
    st.markdown("**Buckets**")
    bucket_col1, bucket_col2 = st.columns([3, 1])
    with bucket_col1:
        bucket_name = st.text_input(
            "Save current selection as bucket (optional)",
            placeholder="e.g. US_Quality_Compounders",
            key="keydata_bs_bucket_name_input",
        )
    with bucket_col2:
        save_bucket = st.button("Save bucket", key="keydata_bs_save_bucket_button")

    if save_bucket:
        sel_company_ids = []
        for opt in options:
            m_sel = re.search(r"\[id=(\d+)\]$", opt)
            if m_sel:
                sel_company_ids.append(int(m_sel.group(1)))
        bname = (bucket_name or "").strip()
        if not bname:
            st.error("Please provide a bucket name.")
        elif not sel_company_ids:
            st.error("Please select at least one company to save a bucket.")
        else:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO company_groups(name) VALUES(?)",
                (bname,),
            )
            conn.commit()
            cur.execute(
                "SELECT id FROM company_groups WHERE name = ?",
                (bname,),
            )
            row = cur.fetchone()
            if row is None:
                st.error("Unexpected error while creating bucket.")
            else:
                gid = int(row[0])
                # Append current selection to existing membership (keep existing companies in the bucket)
                cur.executemany(
                    "INSERT OR IGNORE INTO company_group_members(group_id, company_id) VALUES(?, ?)",
                    [(gid, cid) for cid in sel_company_ids],
                )
                conn.commit()
                st.success(f"Saved bucket '{bname}' with {len(sel_company_ids)} companies.")

    # Bucket selection: use previously saved buckets to drive the analysis
    groups_df = pd.read_sql_query(
        "SELECT id, name FROM company_groups ORDER BY name",
        conn,
    )
    bucket_names_selected = []
    group_name_to_id: Dict[str, int] = {}
    if not groups_df.empty:
        group_name_to_id = {str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()}
        bucket_names_selected = st.multiselect(
            "Or select one or more saved buckets",
            options=list(group_name_to_id.keys()),
            key="keydata_bs_bucket_select",
        )

    yr_input = st.text_input(
        "Year range (e.g., 'Recent - 2020' or '2023-2018')",
        value="Recent - 2020",
        key="keydata_bs_year_range",
    )

    stdev_mode = st.radio(
        "Standard deviation mode",
        ["Sample (ddof=1)", "Population (ddof=0)"],
        horizontal=True,
        key="keydata_bs_stdev_mode",
    )
    sample = (stdev_mode == "Sample (ddof=1)")

    def parse_range(s: str, available_years: List[int]) -> Tuple[int, int]:
        s = (s or "").strip()
        if not available_years:
            raise ValueError("No annual years available.")
        most_recent = max(available_years)
        m_recent = re.match(r"^recent\s*[-–]\s*(\d{4})$", s, flags=re.IGNORECASE)
        m_two = re.match(r"^(\d{4})\s*[-–]\s*(\d{4})$", s)
        if m_recent:
            end = int(m_recent.group(1))
            return most_recent, end
        if m_two:
            a = int(m_two.group(1))
            b = int(m_two.group(2))
            return a, b
        raise ValueError("Could not parse the year range. Use 'Recent - YYYY' or 'YYYY-YYYY'.")

    # Determine final set of company ids based on direct selection + buckets
    selected_company_ids: List[int] = []
    for opt in options:
        m = re.search(r"\[id=(\d+)\]$", opt)
        if m:
            selected_company_ids.append(int(m.group(1)))

    bucket_company_ids: List[int] = []
    if bucket_names_selected:
        group_ids = [group_name_to_id[name] for name in bucket_names_selected]
        placeholders = ",".join(["?"] * len(group_ids))
        bucket_df2 = pd.read_sql_query(
            f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
            conn,
            params=tuple(group_ids),
        )
        bucket_company_ids = [int(x) for x in bucket_df2["company_id"].tolist()] if not bucket_df2.empty else []

    all_company_ids = sorted(set(selected_company_ids + bucket_company_ids))
    if not all_company_ids:
        st.info("Select at least one company or bucket to display Balance Sheet Key Data.")
        return

    def build_metric(
        ann_df: pd.DataFrame,
        value_col: str,
        yr_start: int,
        yr_end: int,
        stdev_sample: bool,
    ) -> Tuple[Dict[int, Optional[float]], Dict[int, Optional[float]], Optional[float], Optional[float]]:
        """Return (values_by_year, yoy_growth_by_year, median_growth, stdev_growth)."""
        if ann_df is None or ann_df.empty:
            return {}, {}, None, None
        df_m = ann_df.copy()
        df_m = df_m[(df_m["year"] >= yr_end) & (df_m["year"] <= yr_start)].sort_values("year")
        if df_m.empty or value_col not in df_m.columns:
            return {}, {}, None, None

        df_m["yoy_growth"] = df_m[value_col].astype(float).pct_change()
        med_g, std_g = compute_growth_stats(
            ann_df,
            yr_start,
            yr_end,
            stdev_sample=stdev_sample,
            value_col=value_col,
        )

        val_by_year = {int(y): (None if pd.isna(v) else float(v)) for y, v in zip(df_m["year"], df_m[value_col])}
        growth_by_year = {
            int(y): (None if pd.isna(g) else float(g)) for y, g in zip(df_m["year"], df_m["yoy_growth"])
        }
        return val_by_year, growth_by_year, med_g, std_g

    def compute_value_stats(
        ann_df: pd.DataFrame,
        value_col: str,
        yr_start: int,
        yr_end: int,
        stdev_sample: bool,
    ) -> Tuple[Optional[float], Optional[float]]:
        """Return (median_value, stdev_value) for a value column over the chosen year range."""
        if ann_df is None or ann_df.empty or value_col not in ann_df.columns:
            return None, None
        df_v = ann_df.copy()
        df_v = df_v[(df_v["year"] >= yr_end) & (df_v["year"] <= yr_start)].sort_values("year")
        if df_v.empty:
            return None, None
        arr = df_v[value_col].astype(float).dropna().to_numpy()
        if arr.size == 0:
            return None, None
        median = float(np.median(arr))
        ddof = 1 if stdev_sample and arr.size > 1 else 0
        stdev = float(np.std(arr, ddof=ddof)) if arr.size > 1 or ddof == 0 else None
        return median, stdev

    def any_available_years(conn_in: sqlite3.Connection, company_id: int) -> List[int]:
        years: List[int] = []
        for df in [
            get_annual_accumulated_profit_series(conn_in, company_id),
            get_annual_roe_series(conn_in, company_id),
            get_annual_roce_series(conn_in, company_id),
            get_annual_non_cash_working_capital_series(conn_in, company_id),
            get_annual_revenue_yield_non_cash_working_capital_series(conn_in, company_id),
            get_annual_interest_coverage_series(conn_in, company_id),
            get_annual_interest_load_series(conn_in, company_id),
        ]:
            if df is not None and not df.empty and "year" in df.columns:
                years.extend([int(y) for y in df["year"].tolist() if pd.notna(y)])
        return years

    per_company = []
    year_set = set()

    for cid in all_company_ids:
        years = any_available_years(conn, cid)
        if not years:
            continue

        try:
            yr_start, yr_end = parse_range(yr_input, years)
            if yr_start < yr_end:
                yr_start, yr_end = yr_end, yr_start
        except Exception as e:
            st.error(f"Year range error: {e}")
            return

        # Company label
        rowc = companies_df[companies_df["id"] == cid]
        if not rowc.empty:
            name = rowc.iloc[0]["name"]
            ticker = rowc.iloc[0]["ticker"]
        else:
            name = f"Company {cid}"
            ticker = ""

        country = _get_company_country(conn, cid)
        country_disp, unit_label, _ = _country_display_and_units(country)

        # Pull required annual series
        ann_acc = get_annual_accumulated_profit_series(conn, cid)
        ann_roe = get_annual_roe_series(conn, cid)
        ann_roce = get_annual_roce_series(conn, cid)
        ann_ncwc = get_annual_non_cash_working_capital_series(conn, cid)
        ann_ry = get_annual_revenue_yield_non_cash_working_capital_series(conn, cid)
        ann_ic = get_annual_interest_coverage_series(conn, cid)
        ann_il = get_annual_interest_load_series(conn, cid)

        # Metrics
        acc_by_year, acc_growth_by_year, acc_med_g, acc_std_g = build_metric(
            ann_acc, "accumulated_profit", yr_start, yr_end, stdev_sample=sample
        )
        roe_by_year, roe_growth_by_year, _, _ = build_metric(
            ann_roe, "roe", yr_start, yr_end, stdev_sample=sample
        )
        roe_med_v, roe_std_v = compute_value_stats(ann_roe, "roe", yr_start, yr_end, stdev_sample=sample)

        roce_by_year, roce_growth_by_year, _, _ = build_metric(
            ann_roce, "roce", yr_start, yr_end, stdev_sample=sample
        )
        roce_med_v, roce_std_v = compute_value_stats(ann_roce, "roce", yr_start, yr_end, stdev_sample=sample)

        ncwc_by_year, ncwc_growth_by_year, ncwc_med_g, ncwc_std_g = build_metric(
            ann_ncwc, "non_cash_working_capital", yr_start, yr_end, stdev_sample=sample
        )
        ry_by_year, ry_growth_by_year, ry_med_g, ry_std_g = build_metric(
            ann_ry, "revenue_yield_ncwc", yr_start, yr_end, stdev_sample=sample
        )
        ic_by_year, ic_growth_by_year, ic_med_g, ic_std_g = build_metric(
            ann_ic, "interest_coverage_ratio", yr_start, yr_end, stdev_sample=sample
        )
        il_by_year, il_growth_by_year, il_med_g, il_std_g = build_metric(
            ann_il, "interest_load_pct", yr_start, yr_end, stdev_sample=sample
        )

        # Track years across any metric present
        for d in [acc_by_year, roe_by_year, roce_by_year, ncwc_by_year, ry_by_year, ic_by_year, il_by_year]:
            year_set.update(d.keys())

        per_company.append(
            {
                "name": str(name),
                "ticker": str(ticker),
                "country": str(country),
                "country_disp": str(country_disp),
                "unit_label": str(unit_label),
                "acc_by_year": acc_by_year,
                "acc_growth_by_year": acc_growth_by_year,
                "acc_median_growth": acc_med_g,
                "acc_stdev_growth": acc_std_g,
                "roe_by_year": roe_by_year,
                "roe_growth_by_year": roe_growth_by_year,
                "roe_median": roe_med_v,
                "roe_stdev": roe_std_v,
                "roce_by_year": roce_by_year,
                "roce_growth_by_year": roce_growth_by_year,
                "roce_median": roce_med_v,
                "roce_stdev": roce_std_v,
                "ncwc_by_year": ncwc_by_year,
                "ncwc_growth_by_year": ncwc_growth_by_year,
                "ncwc_median_growth": ncwc_med_g,
                "ncwc_stdev_growth": ncwc_std_g,
                "ry_by_year": ry_by_year,
                "ry_growth_by_year": ry_growth_by_year,
                "ry_median_growth": ry_med_g,
                "ry_stdev_growth": ry_std_g,
                "ic_by_year": ic_by_year,
                "ic_growth_by_year": ic_growth_by_year,
                "ic_median_growth": ic_med_g,
                "ic_stdev_growth": ic_std_g,
                "il_by_year": il_by_year,
                "il_growth_by_year": il_growth_by_year,
                "il_median_growth": il_med_g,
                "il_stdev_growth": il_std_g,
            }
        )

    if not per_company:
        st.info("No annual Balance Sheet data found for the selected companies in the chosen year range.")
        return

    year_cols = sorted(year_set)

    def fmt_money(x: Optional[float], country: Optional[str]) -> str:
        return _fmt_money_by_country(x, country)

    def fmt_pct_from_decimal(x: Optional[float]) -> str:
        if x is None or pd.isna(x):
            return "—"
        try:
            return f"{float(x) * 100.0:.2f}%"
        except Exception:
            return str(x)

    def fmt_pct_already(x: Optional[float]) -> str:
        if x is None or pd.isna(x):
            return "—"
        try:
            return f"{float(x):.2f}%"
        except Exception:
            return str(x)

    def fmt_ratio(x: Optional[float]) -> str:
        if x is None or pd.isna(x):
            return "—"
        try:
            return f"{float(x):.2f}"
        except Exception:
            return str(x)

    st.markdown("---")

    st.caption("Money values are displayed based on the company\'s country in the database: 🇺🇸 USA = USD Millions ($ M); 🇮🇳 India = INR Crores (₹ Cr).")

    rows = []
    for item in per_company:
        # Accumulated Profit
        a = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Accumulated Profit",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            a[y] = fmt_money(item["acc_by_year"].get(y), item["country"])
        rows.append(a)

        ag = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item["acc_median_growth"]),
            "Standard Deviation": fmt_pct_from_decimal(item["acc_stdev_growth"]),
        }
        for y in year_cols:
            gv = item["acc_growth_by_year"].get(y)
            ag[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(ag)

        # ROE
        r = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "ROE",
            "Median": fmt_pct_from_decimal(item["roe_median"]),
            "Standard Deviation": fmt_pct_from_decimal(item["roe_stdev"]),
        }
        for y in year_cols:
            r[y] = fmt_pct_from_decimal(item["roe_by_year"].get(y))
        rows.append(r)

        rg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            gv = item["roe_growth_by_year"].get(y)
            rg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(rg)

        # ROCE
        rc = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "ROCE",
            "Median": fmt_pct_from_decimal(item["roce_median"]),
            "Standard Deviation": fmt_pct_from_decimal(item["roce_stdev"]),
        }
        for y in year_cols:
            rc[y] = fmt_pct_from_decimal(item["roce_by_year"].get(y))
        rows.append(rc)

        rcg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            gv = item["roce_growth_by_year"].get(y)
            rcg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(rcg)

        # Non-Cash Working Capital
        n = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Non-Cash Working Capital",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            n[y] = fmt_money(item["ncwc_by_year"].get(y), item["country"])
        rows.append(n)

        ng = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item["ncwc_median_growth"]),
            "Standard Deviation": fmt_pct_from_decimal(item["ncwc_stdev_growth"]),
        }
        for y in year_cols:
            gv = item["ncwc_growth_by_year"].get(y)
            ng[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(ng)

        # Revenue Yield of Non-Cash Working Capital
        ry = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Revenue Yield of Non-Cash Working Capital",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            ry[y] = fmt_pct_from_decimal(item["ry_by_year"].get(y))
        rows.append(ry)

        ryg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item["ry_median_growth"]),
            "Standard Deviation": fmt_pct_from_decimal(item["ry_stdev_growth"]),
        }
        for y in year_cols:
            gv = item["ry_growth_by_year"].get(y)
            ryg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(ryg)

        # Interest Coverage Ratio
        ic = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Interest Coverage Ratio",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            ic[y] = fmt_ratio(item["ic_by_year"].get(y))
        rows.append(ic)

        icg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item["ic_median_growth"]),
            "Standard Deviation": fmt_pct_from_decimal(item["ic_stdev_growth"]),
        }
        for y in year_cols:
            gv = item["ic_growth_by_year"].get(y)
            icg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(icg)

        # Interest Load%
        il = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Interest Load%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            il[y] = fmt_pct_already(item["il_by_year"].get(y))
        rows.append(il)

        ilg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item["il_median_growth"]),
            "Standard Deviation": fmt_pct_from_decimal(item["il_stdev_growth"]),
        }
        for y in year_cols:
            gv = item["il_growth_by_year"].get(y)
            ilg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(ilg)

    ordered_cols = ["Company", "Ticker", "Country", "Units", "Key"] + year_cols + ["Median", "Standard Deviation"]
    disp = pd.DataFrame(rows)
    disp = disp.reindex(columns=ordered_cols)

    disp = _apply_key_filter(disp, "keydata_bs_key_filter")

    dup_mask = disp["Company"].eq(disp["Company"].shift())
    disp.loc[dup_mask, ["Company", "Ticker", "Country", "Units"]] = ""

    st.dataframe(disp, use_container_width=True)

def _render_cs_spread_key_data() -> None:
    st.subheader("Capital Structure & Spread")

    conn = get_db()
    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies in the database yet. Upload a spreadsheet under Equity Research → Data Upload.")
        return

    # Base selection: individual companies
    all_company_options = [
        f"{row.name} ({row.ticker}) [id={row.id}]" for _, row in companies_df.iterrows()
    ]

    # Default selection: companies not present in any bucket (same behavior as other tabs)
    try:
        bucket_df = pd.read_sql_query(
            "SELECT DISTINCT company_id FROM company_group_members",
            conn,
        )
        bucketed_ids = set(int(x) for x in bucket_df["company_id"].tolist()) if not bucket_df.empty else set()
    except Exception:
        bucketed_ids = set()

    if bucketed_ids:
        default_company_options = [
            f"{row.name} ({row.ticker}) [id={row.id}]"
            for _, row in companies_df.iterrows()
            if int(row.id) not in bucketed_ids
        ]
    else:
        default_company_options = all_company_options

    # If a company was just ingested this session, override default to only that company
    last_company_id = st.session_state.get("last_ingested_company_id")
    if last_company_id is not None:
        selected_label = None
        for _, row in companies_df.iterrows():
            if int(row.id) == int(last_company_id):
                selected_label = f"{row.name} ({row.ticker}) [id={row.id}]"
                break
        if selected_label:
            default_company_options = [selected_label]

    options = st.multiselect(
        "Companies to analyze",
        options=all_company_options,
        default=default_company_options,
        key="keydata_cs_companies_select",
    )

    # Bucket definition: allow user to save current selection as a named bucket
    bucket_col1, bucket_col2 = st.columns([3, 1])
    with bucket_col1:
        bucket_name = st.text_input(
            "Save current selection as bucket (optional)",
            placeholder="e.g. US_LowLeverage_Compounders",
            key="keydata_cs_bucket_name_input",
        )
    with bucket_col2:
        save_bucket = st.button("Save bucket", key="keydata_cs_save_bucket_button")

    if save_bucket:
        sel_company_ids = []
        for opt in options:
            m_sel = re.search(r"\[id=(\d+)\]$", opt)
            if m_sel:
                sel_company_ids.append(int(m_sel.group(1)))

        if not bucket_name or not bucket_name.strip():
            st.error("Please provide a non-empty bucket name.")
        elif not sel_company_ids:
            st.error("Please select at least one company before saving a bucket.")
        else:
            bname = bucket_name.strip()
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO company_groups(name) VALUES(?)",
                (bname,),
            )
            cur.execute(
                "SELECT id FROM company_groups WHERE name = ?",
                (bname,),
            )
            row = cur.fetchone()
            if row is None:
                st.error("Unexpected error while creating bucket.")
            else:
                gid = int(row[0])
                # Append current selection to existing membership (keep existing companies in the bucket)
                cur.executemany(
                    "INSERT OR IGNORE INTO company_group_members(group_id, company_id) VALUES(?, ?)",
                    [(gid, cid) for cid in sel_company_ids],
                )
                conn.commit()
                st.success(f"Saved bucket '{bname}' with {len(sel_company_ids)} companies.")

    # Bucket selection: use previously saved buckets to drive the analysis
    groups_df = pd.read_sql_query(
        "SELECT id, name FROM company_groups ORDER BY name",
        conn,
    )
    bucket_names_selected = []
    group_name_to_id: Dict[str, int] = {}
    if not groups_df.empty:
        group_name_to_id = {str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()}
        bucket_names_selected = st.multiselect(
            "Or select one or more saved buckets",
            options=list(group_name_to_id.keys()),
            key="keydata_cs_bucket_select",
        )

    yr_input = st.text_input(
        "Year range (e.g., 'Recent - 2020' or '2023-2018')",
        value="Recent - 2020",
        key="keydata_cs_year_range",
    )

    stdev_mode = st.radio(
        "Standard deviation mode",
        ["Sample (ddof=1)", "Population (ddof=0)"],
        horizontal=True,
        key="keydata_cs_stdev_mode",
    )
    sample = (stdev_mode == "Sample (ddof=1)")

    def parse_range(s: str, available_years: List[int]) -> Tuple[int, int]:
        s = (s or "").strip()
        if not available_years:
            raise ValueError("No annual years available.")
        most_recent = max(available_years)
        m_recent = re.match(r"^recent\s*[-–]\s*(\d{4})$", s, flags=re.IGNORECASE)
        m_two = re.match(r"^(\d{4})\s*[-–]\s*(\d{4})$", s)
        if m_recent:
            end = int(m_recent.group(1))
            return most_recent, end
        if m_two:
            start, end = int(m_two.group(1)), int(m_two.group(2))
            return start, end
        raise ValueError("Could not parse the year range. Use 'Recent - YYYY' or 'YYYY-YYYY'.")

    # Determine final set of company ids based on direct selection + buckets
    selected_company_ids: List[int] = []
    for opt in options:
        m = re.search(r"\[id=(\d+)\]$", opt)
        if m:
            selected_company_ids.append(int(m.group(1)))

    bucket_company_ids: List[int] = []
    if bucket_names_selected:
        group_ids = [group_name_to_id[name] for name in bucket_names_selected]
        placeholders = ",".join(["?"] * len(group_ids))
        bucket_df2 = pd.read_sql_query(
            f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
            conn,
            params=group_ids,
        )
        if not bucket_df2.empty:
            bucket_company_ids = [int(x) for x in bucket_df2["company_id"].tolist()]

    all_company_ids = sorted(set(selected_company_ids + bucket_company_ids))
    if not all_company_ids:
        st.info("Select at least one company or a saved bucket to view Capital Structure & Spread key data.")
        return

    def build_metric(
        ann_df: pd.DataFrame,
        value_col: str,
        yr_start: int,
        yr_end: int,
        stdev_sample: bool,
        *,
        abs_denom: bool = False,
    ) -> Tuple[Dict[int, Optional[float]], Dict[int, Optional[float]], Optional[float], Optional[float]]:
        """Return (values_by_year, yoy_growth_by_year, median_growth, stdev_growth)."""
        if ann_df is None or ann_df.empty:
            return {}, {}, None, None
        df_m = ann_df.copy()
        df_m = df_m[(df_m["year"] >= yr_end) & (df_m["year"] <= yr_start)].sort_values("year")
        if df_m.empty or value_col not in df_m.columns:
            return {}, {}, None, None

        try:
            df_m["_v"] = pd.to_numeric(df_m[value_col], errors="coerce").astype(float)
        except Exception:
            df_m["_v"] = pd.to_numeric(df_m[value_col], errors="coerce")

        df_m["yoy_growth"] = df_m["_v"].pct_change()
        # IMPORTANT: pass only a single numeric column into compute_growth_stats.
        # If we renamed _v to value_col while also keeping the original value_col,
        # we'd create duplicate column names. In that case, pandas returns a Series
        # for prev.get(value_col), which makes truth checks ambiguous.
        df_stats = df_m[["year", "_v"]].rename(columns={"_v": value_col})
        med_g, std_g = compute_growth_stats(
            df_stats,
            yr_start,
            yr_end,
            stdev_sample=stdev_sample,
            value_col=value_col,
            abs_denom=abs_denom,
        )

        val_by_year = {int(y): (None if pd.isna(v) else float(v)) for y, v in zip(df_m["year"], df_m["_v"])}
        growth_by_year = {
            int(y): (None if pd.isna(g) else float(g)) for y, g in zip(df_m["year"], df_m["yoy_growth"])
        }
        return val_by_year, growth_by_year, med_g, std_g

    def compute_value_stats(
        ann_df: pd.DataFrame,
        value_col: str,
        yr_start: int,
        yr_end: int,
        stdev_sample: bool,
    ) -> Tuple[Optional[float], Optional[float]]:
        """Return (median_value, stdev_value) for a value column over the chosen year range."""
        if ann_df is None or ann_df.empty or value_col not in ann_df.columns:
            return None, None
        df_v = ann_df.copy()
        df_v = df_v[(df_v["year"] >= yr_end) & (df_v["year"] <= yr_start)].sort_values("year")
        if df_v.empty:
            return None, None
        arr = pd.to_numeric(df_v[value_col], errors="coerce").dropna().astype(float).to_numpy()
        if arr.size == 0:
            return None, None
        median = float(np.median(arr))
        ddof = 1 if stdev_sample and arr.size > 1 else 0
        stdev = float(np.std(arr, ddof=ddof)) if arr.size > 1 or ddof == 0 else None
        return median, stdev

    def ratio_series(
        df_num: pd.DataFrame,
        num_col: str,
        df_den: pd.DataFrame,
        den_col: str,
        out_col: str,
    ) -> pd.DataFrame:
        if df_num is None or df_den is None or df_num.empty or df_den.empty:
            return pd.DataFrame(columns=["year", out_col])
        a = df_num[["year", num_col]].copy()
        b = df_den[["year", den_col]].copy()
        a[num_col] = pd.to_numeric(a[num_col], errors="coerce")
        b[den_col] = pd.to_numeric(b[den_col], errors="coerce")
        m = a.merge(b, on="year", how="inner")
        if m.empty:
            return pd.DataFrame(columns=["year", out_col])
        m[out_col] = np.where(m[den_col] == 0.0, np.nan, (m[num_col] / m[den_col]))
        return m[["year", out_col]].sort_values("year")

    def _get_industry_beta(conn_in: sqlite3.Connection, company_id: int, field: str) -> Optional[float]:
        if field not in ("unlevered_beta", "cash_adjusted_beta"):
            return None
        try:
            row = conn_in.execute(
                f"""
                SELECT AVG(ib.{field})
                FROM company_group_members m
                JOIN company_groups g ON g.id = m.group_id
                JOIN industry_betas ib ON ib.user_industry_bucket = g.name
                WHERE m.company_id = ?
                """,
                (company_id,),
            ).fetchone()
            if row is None or row[0] is None:
                return None
            v = float(row[0])
            return v if np.isfinite(v) else None
        except Exception:
            return None

    def any_available_years(conn_in: sqlite3.Connection, company_id: int) -> List[int]:
        years: List[int] = []
        for df in [
            get_annual_market_capitalization_series(conn_in, company_id),
            get_annual_total_debt_series(conn_in, company_id),
            get_annual_shareholders_equity_series(conn_in, company_id),
            get_annual_roic_direct_upload_series(conn_in, company_id),
            get_annual_cost_of_equity_series(conn_in, company_id),
            get_annual_pre_tax_cost_of_debt_series(conn_in, company_id),
            get_annual_wacc_series(conn_in, company_id),
            get_annual_roic_wacc_spread_series(conn_in, company_id),
        ]:
            if df is not None and not df.empty and "year" in df.columns:
                years.extend([int(y) for y in df["year"].tolist() if pd.notna(y)])
        return years

    per_company = []
    year_set = set()

    for cid in all_company_ids:
        years = any_available_years(conn, cid)
        if not years:
            continue

        try:
            yr_start, yr_end = parse_range(yr_input, years)
            if yr_start < yr_end:
                yr_start, yr_end = yr_end, yr_start
        except Exception as e:
            st.error(f"Year range error: {e}")
            return

        # Ensure derived metrics exist in DB (safe to call repeatedly)
        try:
            compute_and_store_debt_equity(conn, cid)
        except Exception:
            pass
        try:
            compute_and_store_levered_beta(conn, cid)
        except Exception:
            pass
        try:
            compute_and_store_cost_of_equity(conn, cid)
        except Exception:
            pass
        try:
            compute_and_store_default_spread(conn, cid)
        except Exception:
            pass
        try:
            compute_and_store_pre_tax_cost_of_debt(conn, cid)
        except Exception:
            pass
        try:
            compute_and_store_wacc(conn, cid)
        except Exception:
            pass
        try:
            compute_and_store_roic_wacc_spread(conn, cid)
        except Exception:
            pass

        # Company label
        rowc = companies_df[companies_df["id"] == cid]
        if not rowc.empty:
            name = rowc.iloc[0]["name"]
            ticker = rowc.iloc[0]["ticker"]
        else:
            name = f"Company {cid}"
            ticker = ""

        country = _get_company_country(conn, cid)
        country_disp, unit_label, _ = _country_display_and_units(country)

        # Pull required annual series
        ann_mc = get_annual_market_capitalization_series(conn, cid)
        ann_td = get_annual_total_debt_series(conn, cid)
        ann_eq = get_annual_shareholders_equity_series(conn, cid)
        ann_roic = get_annual_roic_direct_upload_series(conn, cid)
        ann_coe = get_annual_cost_of_equity_series(conn, cid)
        ann_pcd = get_annual_pre_tax_cost_of_debt_series(conn, cid)
        ann_wacc = get_annual_wacc_series(conn, cid)
        ann_spread = get_annual_roic_wacc_spread_series(conn, cid)

        # Derived ratios
        ann_eq_over_debt = ratio_series(ann_eq, "shareholders_equity", ann_td, "total_debt", "eq_over_debt")
        ann_mc_over_debt = ratio_series(ann_mc, "market_capitalization", ann_td, "total_debt", "mc_over_debt")

        # Metrics (level + growth)
        mc_by_year, mc_growth_by_year, mc_med_g, mc_std_g = build_metric(
            ann_mc, "market_capitalization", yr_start, yr_end, stdev_sample=sample
        )
        td_by_year, td_growth_by_year, td_med_g, td_std_g = build_metric(
            ann_td, "total_debt", yr_start, yr_end, stdev_sample=sample
        )
        eqd_by_year, eqd_growth_by_year, eqd_med_g, eqd_std_g = build_metric(
            ann_eq_over_debt, "eq_over_debt", yr_start, yr_end, stdev_sample=sample
        )
        mcd_by_year, mcd_growth_by_year, mcd_med_g, mcd_std_g = build_metric(
            ann_mc_over_debt, "mc_over_debt", yr_start, yr_end, stdev_sample=sample
        )

        roic_by_year, roic_growth_by_year, _, _ = build_metric(
            ann_roic, "roic_pct", yr_start, yr_end, stdev_sample=sample
        )
        roic_med_v, roic_std_v = compute_value_stats(ann_roic, "roic_pct", yr_start, yr_end, stdev_sample=sample)

        coe_by_year, coe_growth_by_year, _, _ = build_metric(
            ann_coe, "cost_of_equity", yr_start, yr_end, stdev_sample=sample
        )
        coe_med_v, coe_std_v = compute_value_stats(ann_coe, "cost_of_equity", yr_start, yr_end, stdev_sample=sample)

        pcd_by_year, pcd_growth_by_year, _, _ = build_metric(
            ann_pcd, "pre_tax_cost_of_debt", yr_start, yr_end, stdev_sample=sample
        )
        pcd_med_v, pcd_std_v = compute_value_stats(
            ann_pcd, "pre_tax_cost_of_debt", yr_start, yr_end, stdev_sample=sample
        )

        wacc_by_year, wacc_growth_by_year, _, _ = build_metric(
            ann_wacc, "wacc", yr_start, yr_end, stdev_sample=sample
        )
        wacc_med_v, wacc_std_v = compute_value_stats(ann_wacc, "wacc", yr_start, yr_end, stdev_sample=sample)

        spread_by_year, spread_growth_by_year, _, _ = build_metric(
            ann_spread, "spread_pct", yr_start, yr_end, stdev_sample=sample
        )
        spread_med_v, spread_std_v = compute_value_stats(ann_spread, "spread_pct", yr_start, yr_end, stdev_sample=sample)

        # Industry betas (single values) - repeat across the company-year set (for consistent columns)
        company_years = sorted(set(
            list(mc_by_year.keys())
            + list(td_by_year.keys())
            + list(eqd_by_year.keys())
            + list(mcd_by_year.keys())
            + list(roic_by_year.keys())
            + list(coe_by_year.keys())
            + list(pcd_by_year.keys())
            + list(wacc_by_year.keys())
            + list(spread_by_year.keys())
        ))

        ub = _get_industry_beta(conn, cid, "unlevered_beta")
        cab = _get_industry_beta(conn, cid, "cash_adjusted_beta")

        ub_by_year = {int(y): ub for y in company_years} if company_years else {}
        cab_by_year = {int(y): cab for y in company_years} if company_years else {}
        ub_growth_by_year = {}
        cab_growth_by_year = {}
        if company_years and ub is not None:
            ub_df = pd.DataFrame({"year": company_years, "beta": [ub] * len(company_years)})
            _, ub_growth_by_year, _, _ = build_metric(ub_df, "beta", yr_start, yr_end, stdev_sample=sample)
        if company_years and cab is not None:
            cab_df = pd.DataFrame({"year": company_years, "beta": [cab] * len(company_years)})
            _, cab_growth_by_year, _, _ = build_metric(cab_df, "beta", yr_start, yr_end, stdev_sample=sample)

        # Track years across any metric present
        for d in [
            mc_by_year, td_by_year, eqd_by_year, mcd_by_year, roic_by_year, coe_by_year, pcd_by_year, wacc_by_year, spread_by_year,
            ub_by_year, cab_by_year,
        ]:
            year_set.update(d.keys())

        per_company.append(
            {
                "name": str(name),
                "ticker": str(ticker),
                "country": str(country),
                "country_disp": str(country_disp),
                "unit_label": str(unit_label),
                "mc_by_year": mc_by_year,
                "mc_growth_by_year": mc_growth_by_year,
                "mc_median_growth": mc_med_g,
                "mc_stdev_growth": mc_std_g,
                "td_by_year": td_by_year,
                "td_growth_by_year": td_growth_by_year,
                "td_median_growth": td_med_g,
                "td_stdev_growth": td_std_g,
                "eqd_by_year": eqd_by_year,
                "eqd_growth_by_year": eqd_growth_by_year,
                "eqd_median_growth": eqd_med_g,
                "eqd_stdev_growth": eqd_std_g,
                "mcd_by_year": mcd_by_year,
                "mcd_growth_by_year": mcd_growth_by_year,
                "mcd_median_growth": mcd_med_g,
                "mcd_stdev_growth": mcd_std_g,
                "roic_by_year": roic_by_year,
                "roic_growth_by_year": roic_growth_by_year,
                "roic_median": roic_med_v,
                "roic_stdev": roic_std_v,
                "ub_by_year": ub_by_year,
                "ub_growth_by_year": ub_growth_by_year,
                "cab_by_year": cab_by_year,
                "cab_growth_by_year": cab_growth_by_year,
                "coe_by_year": coe_by_year,
                "coe_growth_by_year": coe_growth_by_year,
                "coe_median": coe_med_v,
                "coe_stdev": coe_std_v,
                "pcd_by_year": pcd_by_year,
                "pcd_growth_by_year": pcd_growth_by_year,
                "pcd_median": pcd_med_v,
                "pcd_stdev": pcd_std_v,
                "wacc_by_year": wacc_by_year,
                "wacc_growth_by_year": wacc_growth_by_year,
                "wacc_median": wacc_med_v,
                "wacc_stdev": wacc_std_v,
                "spread_by_year": spread_by_year,
                "spread_growth_by_year": spread_growth_by_year,
                "spread_median": spread_med_v,
                "spread_stdev": spread_std_v,
            }
        )

    if not per_company:
        st.info("No annual Capital Structure data found for the selected companies in the chosen year range.")
        return

    year_cols = sorted(year_set)

    def fmt_money(x: Optional[float], country: Optional[str]) -> str:
        return _fmt_money_by_country(x, country)

    def fmt_ratio(x: Optional[float]) -> str:
        if x is None or pd.isna(x):
            return "—"
        try:
            return f"{float(x):.2f}"
        except Exception:
            return str(x)

    def fmt_pct_from_decimal(x: Optional[float]) -> str:
        if x is None or pd.isna(x):
            return "—"
        try:
            return f"{float(x) * 100.0:.2f}%"
        except Exception:
            return str(x)

    def fmt_pct_points(x: Optional[float]) -> str:
        # Stored as percentage points (e.g., 6.62 means 6.62%)
        if x is None or pd.isna(x):
            return "—"
        try:
            return f"{float(x):.2f}%"
        except Exception:
            return str(x)

    st.markdown("---")

    st.caption("Money values are displayed based on the company\'s country in the database: 🇺🇸 USA = USD Millions ($ M); 🇮🇳 India = INR Crores (₹ Cr).")

    rows = []
    for item in per_company:
        # Market Cap
        mc = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Market Cap",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            mc[y] = fmt_money(item["mc_by_year"].get(y), item["country"])
        rows.append(mc)

        mcg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item.get("mc_median_growth")),
            "Standard Deviation": fmt_pct_from_decimal(item.get("mc_stdev_growth")),
        }
        for y in year_cols:
            gv = item["mc_growth_by_year"].get(y)
            mcg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(mcg)

        # Total Debt
        td = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Total Debt",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            td[y] = fmt_money(item["td_by_year"].get(y), item["country"])
        rows.append(td)

        tdg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item.get("td_median_growth")),
            "Standard Deviation": fmt_pct_from_decimal(item.get("td_stdev_growth")),
        }
        for y in year_cols:
            gv = item["td_growth_by_year"].get(y)
            tdg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(tdg)

        # Shareholder Equity / Total Debt
        ed = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Shareholder Equity/Total Debt",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            ed[y] = fmt_ratio(item["eqd_by_year"].get(y))
        rows.append(ed)

        edg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item.get("eqd_median_growth")),
            "Standard Deviation": fmt_pct_from_decimal(item.get("eqd_stdev_growth")),
        }
        for y in year_cols:
            gv = item["eqd_growth_by_year"].get(y)
            edg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(edg)

        # Market Cap / Total Debt
        md = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Market Cap/Total Debt",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            md[y] = fmt_ratio(item["mcd_by_year"].get(y))
        rows.append(md)

        mdg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item.get("mcd_median_growth")),
            "Standard Deviation": fmt_pct_from_decimal(item.get("mcd_stdev_growth")),
        }
        for y in year_cols:
            gv = item["mcd_growth_by_year"].get(y)
            mdg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(mdg)

        # ROIC% (median/std only for actual)
        roic = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "ROIC%",
            "Median": fmt_pct_points(item.get("roic_median")),
            "Standard Deviation": fmt_pct_points(item.get("roic_stdev")),
        }
        for y in year_cols:
            roic[y] = fmt_pct_points(item["roic_by_year"].get(y))
        rows.append(roic)

        roicg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            gv = item["roic_growth_by_year"].get(y)
            roicg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(roicg)

        # Unlevered Beta (Industry) (no median/std)
        ub = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Unlevered Beta (Industry)",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            ub[y] = fmt_ratio(item["ub_by_year"].get(y))
        rows.append(ub)

        ubg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            gv = item["ub_growth_by_year"].get(y)
            ubg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(ubg)

        # Cash-Adjusted Beta (Industry) (no median/std)
        cab = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Cash-Adjusted Beta (Industry)",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            cab[y] = fmt_ratio(item["cab_by_year"].get(y))
        rows.append(cab)

        cabg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            gv = item["cab_growth_by_year"].get(y)
            cabg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(cabg)

        # Cost of Equity % (median/std only for actual)
        coe = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Cost of Equity %",
            "Median": fmt_pct_points(item.get("coe_median")),
            "Standard Deviation": fmt_pct_points(item.get("coe_stdev")),
        }
        for y in year_cols:
            coe[y] = fmt_pct_points(item["coe_by_year"].get(y))
        rows.append(coe)

        coeg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            gv = item["coe_growth_by_year"].get(y)
            coeg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(coeg)

        # Pre-Tax Cost of Debt % (median/std only for actual)
        pcd = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Pre-Tax Cost of Debt %",
            "Median": fmt_pct_points(item.get("pcd_median")),
            "Standard Deviation": fmt_pct_points(item.get("pcd_stdev")),
        }
        for y in year_cols:
            pcd[y] = fmt_pct_points(item["pcd_by_year"].get(y))
        rows.append(pcd)

        pcdg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            gv = item["pcd_growth_by_year"].get(y)
            pcdg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(pcdg)

        # WACC % (median/std only for actual)
        w = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Weighted Average Cost of Capital (WACC) %",
            "Median": fmt_pct_points(item.get("wacc_median")),
            "Standard Deviation": fmt_pct_points(item.get("wacc_stdev")),
        }
        for y in year_cols:
            w[y] = fmt_pct_points(item["wacc_by_year"].get(y))
        rows.append(w)

        wg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            gv = item["wacc_growth_by_year"].get(y)
            wg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(wg)

        # Spread% (median/std only for actual)
        sp = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Spread%",
            "Median": fmt_pct_points(item.get("spread_median")),
            "Standard Deviation": fmt_pct_points(item.get("spread_stdev")),
        }
        for y in year_cols:
            sp[y] = fmt_pct_points(item["spread_by_year"].get(y))
        rows.append(sp)

        spg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            gv = item["spread_growth_by_year"].get(y)
            spg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(spg)

    ordered_cols = ["Company", "Ticker", "Country", "Units", "Key"] + year_cols + ["Median", "Standard Deviation"]
    disp = pd.DataFrame(rows)
    disp = disp.reindex(columns=ordered_cols)

    # Visually "merge" Company/Ticker cells (Excel-like) by blanking repeated values
    disp = _apply_key_filter(disp, "keydata_cs_key_filter")
    dup_mask = disp["Company"].eq(disp["Company"].shift())
    disp.loc[dup_mask, ["Company", "Ticker", "Country", "Units"]] = ""

    st.dataframe(disp, use_container_width=True)


def _render_cf_reinvestment_key_data() -> None:
    """Key Data sub-tab: Cash Flow & Reinvestment."""
    st.subheader("Cash Flow & Reinvestment")

    conn = get_db()
    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies in the database yet. Upload a spreadsheet under Equity Research → Data Upload.")
        return

    # Base selection: individual companies
    all_company_options = [
        f"{row.name} ({row.ticker}) [id={row.id}]" for _, row in companies_df.iterrows()
    ]

    # Default selection: companies not present in any bucket (same behavior as other Key Data tabs)
    try:
        bucket_df = pd.read_sql_query(
            "SELECT DISTINCT company_id FROM company_group_members",
            conn,
        )
        bucketed_ids = set(int(x) for x in bucket_df["company_id"].tolist()) if not bucket_df.empty else set()
    except Exception:
        bucketed_ids = set()

    if bucketed_ids:
        default_company_options = [
            f"{row.name} ({row.ticker}) [id={row.id}]"
            for _, row in companies_df.iterrows()
            if int(row.id) not in bucketed_ids
        ]
    else:
        default_company_options = all_company_options

    # If a company was just ingested this session, override default to only that company
    last_company_id = st.session_state.get("last_ingested_company_id")
    if last_company_id is not None:
        selected_label = None
        for _, row in companies_df.iterrows():
            if int(row.id) == int(last_company_id):
                selected_label = f"{row.name} ({row.ticker}) [id={row.id}]"
                break
        if selected_label:
            default_company_options = [selected_label]

    options = st.multiselect(
        "Companies to analyze",
        options=all_company_options,
        default=default_company_options,
        key="keydata_cf_companies_select",
    )

    # Bucket definition: allow user to save current selection as a named bucket
    bucket_col1, bucket_col2 = st.columns([3, 1])
    with bucket_col1:
        bucket_name = st.text_input(
            "Save current selection as bucket (optional)",
            placeholder="e.g. US_FCF_Machines",
            key="keydata_cf_bucket_name_input",
        )
    with bucket_col2:
        save_bucket = st.button("Save bucket", key="keydata_cf_save_bucket_button")

    if save_bucket:
        sel_company_ids = []
        for opt in options:
            m_sel = re.search(r"\[id=(\d+)\]$", opt)
            if m_sel:
                sel_company_ids.append(int(m_sel.group(1)))

        if not bucket_name or not bucket_name.strip():
            st.error("Please provide a non-empty bucket name.")
        elif not sel_company_ids:
            st.error("Please select at least one company before saving a bucket.")
        else:
            bname = bucket_name.strip()
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO company_groups(name) VALUES(?)",
                (bname,),
            )
            cur.execute(
                "SELECT id FROM company_groups WHERE name = ?",
                (bname,),
            )
            row = cur.fetchone()
            if row is None:
                st.error("Unexpected error while creating bucket.")
            else:
                gid = int(row[0])
                # Append current selection to existing membership (keep existing companies in the bucket)
                cur.executemany(
                    "INSERT OR IGNORE INTO company_group_members(group_id, company_id) VALUES(?, ?)",
                    [(gid, cid) for cid in sel_company_ids],
                )
                conn.commit()
                st.success(f"Saved bucket '{bname}' with {len(sel_company_ids)} companies.")

    # Bucket selection: use previously saved buckets to drive the analysis
    groups_df = pd.read_sql_query(
        "SELECT id, name FROM company_groups ORDER BY name",
        conn,
    )
    bucket_names_selected = []
    group_name_to_id: Dict[str, int] = {}
    if not groups_df.empty:
        group_name_to_id = {str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()}
        bucket_names_selected = st.multiselect(
            "Or select one or more saved buckets",
            options=list(group_name_to_id.keys()),
            key="keydata_cf_bucket_select",
        )

    yr_input = st.text_input(
        "Year range (e.g., 'Recent - 2020' or '2023-2018')",
        value="Recent - 2020",
        key="keydata_cf_year_range",
    )

    stdev_mode = st.radio(
        "Standard deviation mode",
        ["Sample (ddof=1)", "Population (ddof=0)"],
        horizontal=True,
        key="keydata_cf_stdev_mode",
    )
    sample = (stdev_mode == "Sample (ddof=1)")

    def parse_range(s: str, available_years: List[int]) -> Tuple[int, int]:
        s = (s or "").strip()
        if not available_years:
            raise ValueError("No annual years available.")
        most_recent = max(available_years)
        m_recent = re.match(r"^recent\s*[-–]\s*(\d{4})$", s, flags=re.IGNORECASE)
        m_two = re.match(r"^(\d{4})\s*[-–]\s*(\d{4})$", s)
        if m_recent:
            end = int(m_recent.group(1))
            return most_recent, end
        if m_two:
            start, end = int(m_two.group(1)), int(m_two.group(2))
            return start, end
        raise ValueError("Could not parse the year range. Use 'Recent - YYYY' or 'YYYY-YYYY'.")

    # Determine final set of company ids based on direct selection + buckets
    selected_company_ids: List[int] = []
    for opt in options:
        m = re.search(r"\[id=(\d+)\]$", opt)
        if m:
            selected_company_ids.append(int(m.group(1)))

    bucket_company_ids: List[int] = []
    if bucket_names_selected:
        group_ids = [group_name_to_id[name] for name in bucket_names_selected]
        placeholders = ",".join(["?"] * len(group_ids))
        bucket_df2 = pd.read_sql_query(
            f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
            conn,
            params=group_ids,
        )
        if not bucket_df2.empty:
            bucket_company_ids = [int(x) for x in bucket_df2["company_id"].tolist()]

    all_company_ids = sorted(set(selected_company_ids + bucket_company_ids))
    if not all_company_ids:
        st.info("Select at least one company or a saved bucket to view Cash Flow & Reinvestment key data.")
        return

    def fmt_money(x: Optional[float], country: Optional[str]) -> str:
        return _fmt_money_by_country(x, country)

    def fmt_pct_from_decimal(x: Optional[float]) -> str:
        if x is None or pd.isna(x):
            return "—"
        try:
            return f"{float(x) * 100.0:.2f}%"
        except Exception:
            return str(x)

    st.markdown("---")

    st.caption("Money values are displayed based on the company\'s country in the database: 🇺🇸 USA = USD Millions ($ M); 🇮🇳 India = INR Crores (₹ Cr).")

    def build_metric(
        ann_df: pd.DataFrame,
        value_col: str,
        yr_start: int,
        yr_end: int,
        stdev_sample: bool,
    ) -> Tuple[Dict[int, Optional[float]], Dict[int, Optional[float]], Optional[float], Optional[float]]:
        """Return (values_by_year, yoy_growth_by_year, median_growth, stdev_growth)."""
        if ann_df is None or ann_df.empty:
            return {}, {}, None, None
        df_m = ann_df.copy()
        df_m = df_m[(df_m["year"] >= yr_end) & (df_m["year"] <= yr_start)].sort_values("year")
        if df_m.empty or value_col not in df_m.columns:
            return {}, {}, None, None

        df_m["yoy_growth"] = pd.to_numeric(df_m[value_col], errors="coerce").astype(float).pct_change()
        med_g, std_g = compute_growth_stats(
            ann_df,
            yr_start,
            yr_end,
            stdev_sample=stdev_sample,
            value_col=value_col,
        )

        val_by_year = {int(y): (None if pd.isna(v) else float(v)) for y, v in zip(df_m["year"], df_m[value_col])}
        growth_by_year = {
            int(y): (None if pd.isna(g) else float(g)) for y, g in zip(df_m["year"], df_m["yoy_growth"])
        }
        return val_by_year, growth_by_year, med_g, std_g

    def compute_value_stats(
        ann_df: pd.DataFrame,
        value_col: str,
        yr_start: int,
        yr_end: int,
        stdev_sample: bool,
    ) -> Tuple[Optional[float], Optional[float]]:
        """Return (median_value, stdev_value) for a value column over the chosen year range."""
        if ann_df is None or ann_df.empty or value_col not in ann_df.columns:
            return None, None
        df_v = ann_df.copy()
        df_v = df_v[(df_v["year"] >= yr_end) & (df_v["year"] <= yr_start)].sort_values("year")
        if df_v.empty:
            return None, None
        arr = pd.to_numeric(df_v[value_col], errors="coerce").dropna().astype(float).to_numpy()
        if arr.size == 0:
            return None, None
        median = float(np.median(arr))
        ddof = 1 if stdev_sample and arr.size > 1 else 0
        stdev = float(np.std(arr, ddof=ddof)) if arr.size > 1 or ddof == 0 else None
        return median, stdev

    def any_available_years(conn_in: sqlite3.Connection, company_id: int) -> List[int]:
        years: List[int] = []
        for df in [
            get_annual_fcff_series(conn_in, company_id),
            get_annual_fcfe_series(conn_in, company_id),
            get_annual_reinvestment_rate_series(conn_in, company_id),
            get_annual_rd_spend_rate_series(conn_in, company_id),
        ]:
            if df is not None and not df.empty and "year" in df.columns:
                years.extend([int(y) for y in df["year"].tolist() if pd.notna(y)])
        return years

    per_company = []
    year_set = set()

    for cid in all_company_ids:
        # Ensure derived metrics exist in DB (safe to call repeatedly)
        try:
            compute_and_store_fcff_and_reinvestment_rate(conn, cid)
        except Exception:
            pass
        try:
            compute_and_store_fcfe(conn, cid)
        except Exception:
            pass
        try:
            compute_and_store_rd_spend_rate(conn, cid)
        except Exception:
            pass

        years = any_available_years(conn, cid)
        if not years:
            continue

        try:
            yr_start, yr_end = parse_range(yr_input, years)
            if yr_start < yr_end:
                yr_start, yr_end = yr_end, yr_start
        except Exception as e:
            st.error(f"Year range error: {e}")
            return

        # Company label
        rowc = companies_df[companies_df["id"] == cid]
        if not rowc.empty:
            name = rowc.iloc[0]["name"]
            ticker = rowc.iloc[0]["ticker"]
        else:
            name = f"Company {cid}"
            ticker = ""

        country = _get_company_country(conn, cid)
        country_disp, unit_label, _ = _country_display_and_units(country)

        # Pull required annual series
        ann_fcff = get_annual_fcff_series(conn, cid)
        ann_fcfe = get_annual_fcfe_series(conn, cid)
        ann_rr = get_annual_reinvestment_rate_series(conn, cid)
        ann_rd = get_annual_rd_spend_rate_series(conn, cid)

        fcff_by_year, fcff_growth_by_year, fcff_med_g, fcff_std_g = build_metric(
            ann_fcff, "fcff", yr_start, yr_end, stdev_sample=sample
        )
        fcfe_by_year, fcfe_growth_by_year, fcfe_med_g, fcfe_std_g = build_metric(
            ann_fcfe, "fcfe", yr_start, yr_end, stdev_sample=sample
        )

        rr_by_year, rr_growth_by_year, _, _ = build_metric(
            ann_rr, "reinvestment_rate", yr_start, yr_end, stdev_sample=sample
        )
        rr_med_v, rr_std_v = compute_value_stats(ann_rr, "reinvestment_rate", yr_start, yr_end, stdev_sample=sample)

        rd_by_year, rd_growth_by_year, _, _ = build_metric(
            ann_rd, "rd_spend_rate", yr_start, yr_end, stdev_sample=sample
        )
        rd_med_v, rd_std_v = compute_value_stats(ann_rd, "rd_spend_rate", yr_start, yr_end, stdev_sample=sample)

        for d in [fcff_by_year, fcfe_by_year, rr_by_year, rd_by_year]:
            year_set.update(d.keys())

        per_company.append(
            {
                "name": str(name),
                "ticker": str(ticker),
                "country": str(country),
                "country_disp": str(country_disp),
                "unit_label": str(unit_label),
                "fcff_by_year": fcff_by_year,
                "fcff_growth_by_year": fcff_growth_by_year,
                "fcff_median_growth": fcff_med_g,
                "fcff_stdev_growth": fcff_std_g,
                "fcfe_by_year": fcfe_by_year,
                "fcfe_growth_by_year": fcfe_growth_by_year,
                "fcfe_median_growth": fcfe_med_g,
                "fcfe_stdev_growth": fcfe_std_g,
                "rr_by_year": rr_by_year,
                "rr_growth_by_year": rr_growth_by_year,
                "rr_median": rr_med_v,
                "rr_stdev": rr_std_v,
                "rd_by_year": rd_by_year,
                "rd_growth_by_year": rd_growth_by_year,
                "rd_median": rd_med_v,
                "rd_stdev": rd_std_v,
            }
        )

    if not per_company:
        st.info("No annual Cash Flow & Reinvestment data found for the selected companies in the chosen year range.")
        return

    year_cols = sorted(year_set)

    rows = []
    for item in per_company:
        # FCFF
        fcff = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "FCFF",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            fcff[y] = fmt_money(item["fcff_by_year"].get(y), item["country"])
        rows.append(fcff)

        fcffg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item["fcff_median_growth"]),
            "Standard Deviation": fmt_pct_from_decimal(item["fcff_stdev_growth"]),
        }
        for y in year_cols:
            gv = item["fcff_growth_by_year"].get(y)
            fcffg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(fcffg)

        # FCFE
        fcfe = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "FCFE",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            fcfe[y] = fmt_money(item["fcfe_by_year"].get(y), item["country"])
        rows.append(fcfe)

        fcfe_g = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": fmt_pct_from_decimal(item["fcfe_median_growth"]),
            "Standard Deviation": fmt_pct_from_decimal(item["fcfe_stdev_growth"]),
        }
        for y in year_cols:
            gv = item["fcfe_growth_by_year"].get(y)
            fcfe_g[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(fcfe_g)

        # Reinvestment Rate % (median/std only for actual)
        rr = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Reinvestment Rate %",
            "Median": fmt_pct_from_decimal(item.get("rr_median")),
            "Standard Deviation": fmt_pct_from_decimal(item.get("rr_stdev")),
        }
        for y in year_cols:
            rr[y] = fmt_pct_from_decimal(item["rr_by_year"].get(y))
        rows.append(rr)

        rrg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            gv = item["rr_growth_by_year"].get(y)
            rrg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(rrg)

        # R&D Spend Rate % (median/std only for actual)
        rd = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "R&D Spend Rate %",
            "Median": fmt_pct_from_decimal(item.get("rd_median")),
            "Standard Deviation": fmt_pct_from_decimal(item.get("rd_stdev")),
        }
        for y in year_cols:
            rd[y] = fmt_pct_from_decimal(item["rd_by_year"].get(y))
        rows.append(rd)

        rdg = {
            "Company": item["name"],
            "Ticker": item["ticker"],
            "Country": item["country_disp"],
            "Units": item["unit_label"],
            "Key": "Growth%",
            "Median": "",
            "Standard Deviation": "",
        }
        for y in year_cols:
            gv = item["rd_growth_by_year"].get(y)
            rdg[y] = "" if gv is None else fmt_pct_from_decimal(gv)
        rows.append(rdg)

    ordered_cols = ["Company", "Ticker", "Country", "Units", "Key"] + year_cols + ["Median", "Standard Deviation"]
    disp = pd.DataFrame(rows)
    disp = disp.reindex(columns=ordered_cols)

    # Visually "merge" Company/Ticker cells (Excel-like) by blanking repeated values
    disp = _apply_key_filter(disp, "keydata_cf_key_filter")
    dup_mask = disp["Company"].eq(disp["Company"].shift())
    disp.loc[dup_mask, ["Company", "Ticker", "Country", "Units"]] = ""

    st.dataframe(disp, use_container_width=True)
