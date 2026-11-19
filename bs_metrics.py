from preview_mode import is_read_only
import streamlit as st
from core import *  # noqa: F401,F403
from ui_theme import company_label_map


def _get_interest_penalty_inputs(conn, company_id):
    """Read raw annual inputs without changing legacy stored ratios."""
    frame = read_df(
        "SELECT fiscal_year AS year, 'operating_income' AS metric, operating_income AS value "
        "FROM operating_income_annual WHERE company_id = ? "
        "UNION ALL "
        "SELECT fiscal_year AS year, 'interest_expense' AS metric, interest_expense AS value "
        "FROM interest_expense_annual WHERE company_id = ?",
        conn,
        params=(company_id, company_id),
    )
    return tuple(
        frame.loc[frame["metric"] == metric, ["year", "value"]]
        .rename(columns={"value": metric}).reset_index(drop=True)
        for metric in ("operating_income", "interest_expense")
    )


def render_balance_sheet_metrics_tab():
    st.title("Balance Sheet Metrics and Dashboard")

    with st.expander("Balance Sheet Metrics", expanded=True):
        conn = get_db()
        companies_df = list_companies(conn)
        if companies_df.empty:
            st.info("No companies in the database yet. Upload a spreadsheet above.")
        else:
            # Base selection: individual companies
            labels = company_label_map(companies_df)
            all_company_options = list(labels.keys())

            # Default selection: companies not present in any bucket
            try:
                bucket_df = read_df(
                    "SELECT DISTINCT company_id FROM company_group_members",
                    conn,
                )
                if bucket_df.empty:
                    bucketed_ids = set()
                else:
                    bucketed_ids = {int(x) for x in bucket_df["company_id"].tolist()}
            except Exception:
                bucketed_ids = set()

            default_company_options: List[int] = []
            if bucketed_ids:
                for _, row in companies_df.iterrows():
                    if int(row.id) not in bucketed_ids:
                        default_company_options.append(int(row.id))
            else:
                # If no bucket information yet, fall back to all companies
                default_company_options = all_company_options

            # If a company was just ingested this session, override default to only that company
            last_company_id = st.session_state.get("last_ingested_company_id")
            if last_company_id is not None:
                selected_company_id = None
                for _, row in companies_df.iterrows():
                    if int(row.id) == int(last_company_id):
                        selected_company_id = int(row.id)
                        break
                if selected_company_id is not None:
                    default_company_options = [selected_company_id]

            options = st.multiselect(
                "Companies to analyze (for Balance Sheet metrics)",
                options=all_company_options,
                default=default_company_options,
                format_func=lambda company_id: labels.get(company_id, str(company_id)),
                key="bs_companies_to_analyze",
            )

            # Bucket save UI
            bucket_col1, bucket_col2 = st.columns([3, 1])
            with bucket_col1:
                bucket_name = st.text_input(
                    "Save current selection as bucket (optional)",
                    placeholder="e.g. Strong_Balance_Sheet_Leaders",
                    key="bs_bucket_name_input",
                )
            with bucket_col2:
                save_bucket = st.button("Save bucket", key="bs_save_bucket_button", disabled=is_read_only())

            if save_bucket:
                sel_company_ids: List[int] = [int(company_id) for company_id in options]

                if not bucket_name or not bucket_name.strip():
                    st.error("Please provide a non-empty bucket name.")
                elif not sel_company_ids:
                    st.error("Please select at least one company before saving a bucket.")
                else:
                    bname = bucket_name.strip()
                    gid = get_company_group_id(conn, bname, create=True)
                    if gid is None:
                        st.error("Unexpected error while creating bucket.")
                    else:
                        # Append current selection to existing membership (keep existing companies in the bucket)
                        add_company_group_members(conn, gid, sel_company_ids)
                        st.success(f"Saved bucket '{bname}' with {len(sel_company_ids)} companies.")

            # Bucket selection: use previously saved buckets to drive the analysis
            groups_df = read_df(
                "SELECT id, name FROM company_groups ORDER BY name",
                conn,
            )
            bucket_names_selected: List[str] = []
            group_name_to_id: Dict[str, int] = {}
            if not groups_df.empty:
                group_name_to_id = {
                    str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()
                }
                bucket_names_selected = st.multiselect(
                    "Or select one or more saved buckets",
                    options=list(group_name_to_id.keys()),
                    key="bs_bucket_select",
                )

            yr_input_bs = st.text_input(
                "Year range (e.g., 'Recent - 2020' or '2023-2018')",
                value="Recent - 2020",
                key="bs_year_range_input",
            )

            stdev_mode_bs = st.radio(
                "Standard deviation mode",
                ['Sample (ddof=1)', 'Population (ddof=0)'],
                horizontal=True,
                key="bs_stdev_mode",
            )
            sample_bs = (stdev_mode_bs == 'Sample (ddof=1)')

            def parse_range_bs(s: str, available_years: List[int]) -> Tuple[int, int]:
                s = (s or '').strip()
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
            selected_company_ids: List[int] = [int(company_id) for company_id in options]

            # Add companies from selected buckets
            if bucket_names_selected:
                bucket_ids = [group_name_to_id[bname] for bname in bucket_names_selected if bname in group_name_to_id]
                if bucket_ids:
                    q_marks = ",".join("?" for _ in bucket_ids)
                    bucket_members_df = read_df(
                        f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({q_marks})",
                        conn,
                        params=bucket_ids,
                    )
                    for _, r in bucket_members_df.iterrows():
                        cid = int(r["company_id"])
                        if cid not in selected_company_ids:
                            selected_company_ids.append(cid)

            if not selected_company_ids:
                st.info("Select at least one company (directly or via buckets) to see Balance Sheet metrics.")
            else:
                rows_out: List[Dict[str, Optional[float]]] = []
                missing_interest_penalty_count = 0
                for cid in selected_company_ids:
                    # Ensure equity-derived metrics (Total Equity, Average Equity, ROE) are up-to-date
                    compute_and_store_total_equity_and_roe(conn, cid)

                    ann_acc = get_annual_accumulated_profit_series(conn, cid)
                    ann_acc_stats = exclude_recent_zero_accumulated_profit_for_stats(ann_acc)
                    ann_roe = get_annual_roe_series(conn, cid)
                    ann_roce = get_annual_roce_series(conn, cid)
                    ann_ncwc = get_annual_non_cash_working_capital_series(conn, cid)
                    ann_rev_yield_ncwc = get_annual_revenue_yield_non_cash_working_capital_series(conn, cid)
                    ann_interest_load = get_annual_interest_load_series(conn, cid)
                    ann_interest_cov = get_annual_interest_coverage_series(conn, cid)

                    # Build available years from any of the series we have
                    available_years: List[int] = []
                    if not ann_acc.empty:
                        available_years.extend(ann_acc['year'].tolist())
                    if not ann_roe.empty:
                        available_years.extend(ann_roe['year'].tolist())
                    if not ann_roce.empty:
                        available_years.extend(ann_roce['year'].tolist())
                    if not ann_ncwc.empty:
                        available_years.extend(ann_ncwc['year'].tolist())
                    if not ann_rev_yield_ncwc.empty:
                        available_years.extend(ann_rev_yield_ncwc['year'].tolist())
                    if not ann_interest_cov.empty:
                        available_years.extend(ann_interest_cov['year'].tolist())
                    if not ann_interest_load.empty:
                        available_years.extend(ann_interest_load['year'].tolist())
                    available_years = sorted(set(available_years))

                    # If we have no years at all for this company, skip it
                    if not available_years:
                        continue

                    try:
                        yr_start, yr_end = parse_range_bs(yr_input_bs, available_years)
                        if yr_start < yr_end:
                            yr_start, yr_end = yr_end, yr_start

                        # Accumulated profit growth stats (may be None if no data)
                        med_acc_g, std_acc_g = (None, None)
                        if not ann_acc_stats.empty:
                            med_acc_g, std_acc_g = compute_growth_stats(
                                ann_acc_stats,
                                yr_start,
                                yr_end,
                                stdev_sample=sample_bs,
                                value_col="accumulated_profit",
                                abs_denom=True,
                            )


                        # Non-Cash Working Capital YoY Change % stats
                        med_ncwc_g, std_ncwc_g = (None, None)
                        if not ann_ncwc.empty:
                            med_ncwc_g, std_ncwc_g = compute_growth_stats(
                                ann_ncwc,
                                yr_start,
                                yr_end,
                                stdev_sample=sample_bs,
                                value_col="non_cash_working_capital",
                                abs_denom=True,
                            )


                        # Revenue Yield of Non-Cash Working Capital % stats
                        med_rev_yield_ncwc, std_rev_yield_ncwc = (None, None)
                        if not ann_rev_yield_ncwc.empty:
                            df_ry_ncwc = ann_rev_yield_ncwc.copy()
                            df_ry_ncwc = df_ry_ncwc[(df_ry_ncwc['year'] >= yr_end) & (df_ry_ncwc['year'] <= yr_start)].sort_values('year')
                            vals_ry_ncwc = df_ry_ncwc['revenue_yield_ncwc'].dropna().astype(float).values
                            if vals_ry_ncwc.size > 0:
                                arr_ry_ncwc = np.array(vals_ry_ncwc, dtype=float)
                                med_rev_yield_ncwc = float(np.median(arr_ry_ncwc))
                                ddof_ry_ncwc = 1 if sample_bs and arr_ry_ncwc.size > 1 else 0
                                std_rev_yield_ncwc = float(np.std(arr_ry_ncwc, ddof=ddof_ry_ncwc))

                        # ROE stats
                        med_roe_g, std_roe_g = (None, None)
                        if not ann_roe.empty:
                            df_roe = ann_roe.copy()
                            df_roe = df_roe[(df_roe['year'] >= yr_end) & (df_roe['year'] <= yr_start)].sort_values('year')
                            vals_roe = df_roe['roe'].dropna().astype(float).values
                            if vals_roe.size > 0:
                                arr_roe = np.array(vals_roe, dtype=float)
                                med_roe_g = float(np.median(arr_roe))
                                ddof_roe = 1 if sample_bs and arr_roe.size > 1 else 0
                                std_roe_g = float(np.std(arr_roe, ddof=ddof_roe)) if arr_roe.size > 1 or ddof_roe == 0 else None

                        # ROCE stats
                        med_roce, std_roce = (None, None)
                        if not ann_roce.empty:
                            df_roce = ann_roce.copy()
                            df_roce = df_roce[(df_roce['year'] >= yr_end) & (df_roce['year'] <= yr_start)].sort_values('year')
                            vals_roce = df_roce['roce'].dropna().astype(float).values
                            if vals_roce.size > 0:
                                arr_roce = np.array(vals_roce, dtype=float)
                                med_roce = float(np.median(arr_roce))
                                ddof_roce = 1 if sample_bs and arr_roce.size > 1 else 0
                                std_roce = float(np.std(arr_roce, ddof=ddof_roce)) if arr_roce.size > 1 or ddof_roce == 0 else None

                        # Interest coverage stats
                        med_interest_cov, std_interest_cov = (None, None)
                        if not ann_interest_cov.empty:
                            df_ic = ann_interest_cov.copy()
                            df_ic = df_ic[(df_ic['year'] >= yr_end) & (df_ic['year'] <= yr_start)].sort_values('year')
                            vals_ic = df_ic['interest_coverage_ratio'].dropna().astype(float).values
                            if vals_ic.size > 0:
                                arr_ic = np.array(vals_ic, dtype=float)
                                med_interest_cov = float(np.median(arr_ic))
                                ddof_ic = 1 if sample_bs and arr_ic.size > 1 else 0
                                std_interest_cov = float(np.std(arr_ic, ddof=ddof_ic)) if arr_ic.size > 1 or ddof_ic == 0 else None

                        # Retain percentage display, using the corrected annual penalties.
                        ann_operating_income, ann_interest_expense = _get_interest_penalty_inputs(conn, cid)
                        median_penalty, std_penalty = compute_interest_penalty_stats(
                            ann_operating_income, ann_interest_expense, yr_start, yr_end,
                            stdev_sample=sample_bs,
                        )
                        med_interest_load = None if median_penalty is None else median_penalty * 100.0
                        std_interest_load = None if std_penalty is None else std_penalty * 100.0
                        if median_penalty is None:
                            missing_interest_penalty_count += 1

                        row = companies_df[companies_df["id"] == cid].iloc[0]
                        name = row["name"]
                        ticker = row["ticker"]

                        rows_out.append(
                            {
                                'Company Name': name,
                                'Ticker': ticker,
                                'Median Accumulated Profit Growth': (None if med_acc_g is None else med_acc_g * 100.0),
                                'Accumulated Profit Growth Standard Deviation': (None if std_acc_g is None else std_acc_g * 100.0),
                                'Median ROE': (None if med_roe_g is None else med_roe_g * 100.0),
                                'ROE Standard Deviation': (None if std_roe_g is None else std_roe_g * 100.0),
                                'Median ROCE': (None if med_roce is None else med_roce * 100.0),
                                'ROCE Standard Deviation': (None if std_roce is None else std_roce * 100.0),
                                'Median Non-Cash Working Capital YoY Change %': (None if med_ncwc_g is None else med_ncwc_g * 100.0),
                                'Non-Cash Working Capital YoY Change Standard Deviation': (None if std_ncwc_g is None else std_ncwc_g * 100.0),
                                'Median Revenue Yield of Non-Cash Working Capital %': (None if med_rev_yield_ncwc is None else med_rev_yield_ncwc * 100.0),
                                'Revenue Yield of Non-Cash Working Capital Standard Deviation': (None if std_rev_yield_ncwc is None else std_rev_yield_ncwc * 100.0),
                                'Median Interest Coverage Ratio': med_interest_cov,
                                'Interest Coverage Ratio Standard Deviation': std_interest_cov,
                                'Median Interest Load %': med_interest_load,
                                'Interest Load Standard Deviation': std_interest_load,
                            }
                        )
                    except Exception as e:
                        st.warning(f"Skipping company id={cid} for Balance Sheet metrics: {e}")
                if rows_out:
                    if missing_interest_penalty_count:
                        st.warning(
                            f"Interest-load statistics are unavailable for {missing_interest_penalty_count} "
                            "selected companies: every selected year requires valid operating income "
                            "and nonnegative interest expense. Missing inputs are not treated as zero burden."
                        )
                    out_df = pd.DataFrame(rows_out)

                    def fmt_pct_local(x):
                        return "—" if pd.isna(x) else f"{x:.2f}%"

                    def fmt_ratio_local(x):
                        return "—" if pd.isna(x) else f"{x:.2f}"

                    show_df = out_df.copy()
                    show_df['Median Accumulated Profit Growth'] = show_df['Median Accumulated Profit Growth'].map(fmt_pct_local)
                    show_df['Accumulated Profit Growth Standard Deviation'] = show_df['Accumulated Profit Growth Standard Deviation'].map(fmt_pct_local)
                    show_df['Median ROE'] = show_df['Median ROE'].map(fmt_pct_local)
                    show_df['ROE Standard Deviation'] = show_df['ROE Standard Deviation'].map(fmt_pct_local)
                    show_df['Median ROCE'] = show_df['Median ROCE'].map(fmt_pct_local)
                    show_df['ROCE Standard Deviation'] = show_df['ROCE Standard Deviation'].map(fmt_pct_local)

                    show_df['Median Non-Cash Working Capital YoY Change %'] = show_df['Median Non-Cash Working Capital YoY Change %'].map(fmt_pct_local)
                    show_df['Non-Cash Working Capital YoY Change Standard Deviation'] = show_df['Non-Cash Working Capital YoY Change Standard Deviation'].map(fmt_pct_local)
                    show_df['Median Revenue Yield of Non-Cash Working Capital %'] = show_df['Median Revenue Yield of Non-Cash Working Capital %'].map(fmt_pct_local)
                    show_df['Revenue Yield of Non-Cash Working Capital Standard Deviation'] = show_df['Revenue Yield of Non-Cash Working Capital Standard Deviation'].map(fmt_pct_local)
                    show_df['Median Interest Load %'] = show_df['Median Interest Load %'].map(fmt_pct_local)
                    show_df['Interest Load Standard Deviation'] = show_df['Interest Load Standard Deviation'].map(fmt_pct_local)

                    show_df['Median Interest Coverage Ratio'] = show_df['Median Interest Coverage Ratio'].map(fmt_ratio_local)
                    show_df['Interest Coverage Ratio Standard Deviation'] = show_df['Interest Coverage Ratio Standard Deviation'].map(fmt_ratio_local)

                    st.subheader("Results")
                    st.dataframe(
                        show_df,
                        use_container_width=True,
                        column_config={
                            "Median Accumulated Profit Growth": st.column_config.TextColumn(
                                help="Median year-over-year growth in Accumulated Profit (Retained Earnings + Comprehensive Income) over the selected range.",
                            ),
                            "Accumulated Profit Growth Standard Deviation": st.column_config.TextColumn(
                                help="Standard deviation of year-over-year growth in Accumulated Profit over the selected range.",
                            ),
                            "Median ROE": st.column_config.TextColumn(
                                help="Median year-over-year percentage change in Return on Equity (ROE = Net Income ÷ Average Equity) over the selected range.",
                            ),
                            "ROE Standard Deviation": st.column_config.TextColumn(
                                help="Standard deviation of year-over-year percentage change in Return on Equity over the selected range.",
                            ),

                            "Median Non-Cash Working Capital YoY Change %": st.column_config.TextColumn(
                                help="Median year-over-year percentage change in Non-Cash Working Capital over the selected range (using absolute value in the prior-year denominator).",
                            ),
                            "Non-Cash Working Capital YoY Change Standard Deviation": st.column_config.TextColumn(
                                help="Standard deviation of year-over-year percentage change in Non-Cash Working Capital over the selected range.",
                            ),
                            "Median Revenue Yield of Non-Cash Working Capital %": st.column_config.TextColumn(
                                help="Median Revenue Yield of Non-Cash Working Capital over the selected range (1 − Non-Cash Working Capital ÷ Revenue, expressed as a percentage).",
                            ),
                            "Revenue Yield of Non-Cash Working Capital Standard Deviation": st.column_config.TextColumn(
                                help="Standard deviation of Revenue Yield of Non-Cash Working Capital over the selected range.",
                            ),
                            "Median Interest Load %": st.column_config.TextColumn(
                                help="Median annual interest penalty × 100. Annual penalty = Interest Expense ÷ (max(Operating Income, 0) + Interest Expense), with confirmed zero expense giving 0. Values range from 0% to 100%; every selected year needs valid inputs.",
                            ),
                            "Interest Load Standard Deviation": st.column_config.TextColumn(
                                help="Standard deviation of the corrected annual interest penalties, expressed in percentage points, using the selected Sample or Population mode.",
                            ),
                        },
                    )
                else:
                    st.info("No Balance Sheet metrics to display for the selected year range.")

    st.markdown("---")

    with st.expander("Balance Sheet Score Dashboard", expanded=True):
        conn = get_db()
        companies_df = list_companies(conn)
        if companies_df.empty:
            st.info("No companies in the database yet. Upload a spreadsheet above.")
        else:
            yr_input_bs_score = st.text_input(
                "Year range for Balance Sheet Score (e.g., 'Recent - 2020' or '2023-2018')",
                value="Recent - 2020",
                key="bs_score_year_range",
            )
            stdev_mode_bs_score = st.radio(
                "Standard deviation mode for Balance Sheet Score",
                ['Sample (ddof=1)', 'Population (ddof=0)'],
                horizontal=True,
                key="bs_score_stdev_mode",
            )
            sample_bs_score = (stdev_mode_bs_score == 'Sample (ddof=1)')

            def parse_range_bs_score(s: str, available_years: List[int]) -> Tuple[int, int]:
                s = (s or '').strip()
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

            # Load weight factors once
            growth_weights_df = read_df(
                "SELECT factor, weight FROM growth_weight_factors",
                conn,
            )
            stddev_weights_df = read_df(
                "SELECT factor, weight FROM stddev_weight_factors",
                conn,
            )

            growth_weight_map = {str(row["factor"]): float(row["weight"]) for _, row in growth_weights_df.iterrows()}
            stddev_weight_map = {str(row["factor"]): float(row["weight"]) for _, row in stddev_weights_df.iterrows()}

            def get_factor_weight(weight_map: Dict[str, float], *names: str) -> Optional[float]:
                for nm in names:
                    if nm in weight_map:
                        return weight_map[nm]
                return None

            def weighted_score(pairs: List[Tuple[Optional[float], Optional[float]]]) -> Optional[float]:
                return compute_score_weighted_average(pairs)

            # Bucket-based company selection for Balance Sheet Score Dashboard
            groups_df = read_df(
                "SELECT id, name FROM company_groups ORDER BY name",
                conn,
            )
            bucket_names_selected: List[str] = []
            group_name_to_id: Dict[str, int] = {}
            if groups_df.empty:
                st.info("No buckets defined yet. Define buckets in the P&L or Balance Sheet Metrics sections.")
            else:
                group_name_to_id = {
                    str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()
                }
                bucket_names_selected = st.multiselect(
                    "Select one or more buckets for Balance Sheet Score computation",
                    options=list(group_name_to_id.keys()),
                    key="bs_score_bucket_select",
                )

            score_company_ids: List[int] = []
            if bucket_names_selected:
                group_ids = [group_name_to_id[name] for name in bucket_names_selected if name in group_name_to_id]
                if group_ids:
                    placeholders = ",".join(["?"] * len(group_ids))
                    bucket_members_df = read_df(
                        f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
                        conn,
                        params=group_ids,
                    )
                    if not bucket_members_df.empty:
                        score_company_ids = [int(x) for x in bucket_members_df["company_id"].tolist()]

            if not score_company_ids:
                st.info("Select at least one bucket to compute Balance Sheet scores.")

            debt_penalty_lambda = st.number_input(
                "Debt penalty strength (Lambda)",
                min_value=0.01,
                value=1.0,
                step=0.1,
                format="%.2f",
                help="Maximum deduction in score points: Lambda × the median annual interest penalty.",
                key="bs_score_debt_penalty_lambda",
            )
            if not np.isfinite(debt_penalty_lambda) or debt_penalty_lambda <= 0.0:
                st.error("Debt penalty strength (Lambda) must be a finite positive number.")
                return

            compute_bs_scores = st.button(
                "Compute Balance Sheet Scores",
                type="primary",
                key="compute_bs_scores",
                disabled=(len(score_company_ids) == 0),
            )
            if compute_bs_scores:
                bs_rows: List[Dict[str, Optional[float]]] = []
                missing_debt_penalty_count = 0

                for cid in score_company_ids:
                    row = companies_df[companies_df["id"] == cid]
                    if row.empty:
                        continue
                    row = row.iloc[0]
                    name = row["name"]
                    ticker = row["ticker"]

                    ann_acc = get_annual_accumulated_profit_series(conn, cid)
                    ann_acc_stats = exclude_recent_zero_accumulated_profit_for_stats(ann_acc)
                    ann_roe = get_annual_roe_series(conn, cid)
                    ann_roce = get_annual_roce_series(conn, cid)

                    # Build available years from any of the series we have
                    available_years: List[int] = []
                    if not ann_acc.empty:
                        available_years.extend(ann_acc['year'].tolist())
                    if not ann_roe.empty:
                        available_years.extend(ann_roe['year'].tolist())
                    if not ann_roce.empty:
                        available_years.extend(ann_roce['year'].tolist())
                    available_years = sorted(set(available_years))

                    if not available_years:
                        continue

                    try:
                        yr_start, yr_end = parse_range_bs_score(yr_input_bs_score, available_years)
                        if yr_start < yr_end:
                            yr_start, yr_end = yr_end, yr_start

                        # Accumulated profit growth stats
                        med_acc_g, std_acc_g = (None, None)
                        if not ann_acc_stats.empty:
                            med_acc_g, std_acc_g = compute_growth_stats(
                                ann_acc_stats,
                                yr_start,
                                yr_end,
                                stdev_sample=sample_bs_score,
                                value_col="accumulated_profit",
                                abs_denom=True,
                            )

                        # ROE stats
                        med_roe_g, std_roe_g = (None, None)
                        if not ann_roe.empty:
                            df_roe = ann_roe.copy()
                            df_roe = df_roe[(df_roe['year'] >= yr_end) & (df_roe['year'] <= yr_start)].sort_values('year')
                            vals_roe = df_roe['roe'].dropna().astype(float).values
                            if vals_roe.size > 0:
                                arr_roe = np.array(vals_roe, dtype=float)
                                med_roe_g = float(np.median(arr_roe))
                                ddof_roe = 1 if sample_bs_score and arr_roe.size > 1 else 0
                                std_roe_g = float(np.std(arr_roe, ddof=ddof_roe)) if arr_roe.size > 1 or ddof_roe == 0 else None

                        # ROCE stats
                        med_roce, std_roce = (None, None)
                        if not ann_roce.empty:
                            df_roce = ann_roce.copy()
                            df_roce = df_roce[(df_roce['year'] >= yr_end) & (df_roce['year'] <= yr_start)].sort_values('year')
                            vals_roce = df_roce['roce'].dropna().astype(float).values
                            if vals_roce.size > 0:
                                arr_roce = np.array(vals_roce, dtype=float)
                                med_roce = float(np.median(arr_roce))
                                ddof_roce = 1 if sample_bs_score and arr_roce.size > 1 else 0
                                std_roce = float(np.std(arr_roce, ddof=ddof_roce)) if arr_roce.size > 1 or ddof_roce == 0 else None

                        ann_operating_income, ann_interest_expense = _get_interest_penalty_inputs(conn, cid)
                        median_interest_penalty = compute_median_interest_penalty(
                            ann_operating_income, ann_interest_expense, yr_start, yr_end,
                        )

                        # Convert to percentage units where applicable
                        def to_pct_val(x: Optional[float]) -> Optional[float]:
                            if x is None:
                                return None
                            return x * 100.0

                        median_acc_pct = to_pct_val(med_acc_g) if med_acc_g is not None else None
                        std_acc_pct = to_pct_val(std_acc_g) if std_acc_g is not None else None

                        median_roe_pct = to_pct_val(med_roe_g) if med_roe_g is not None else None
                        std_roe_pct = to_pct_val(std_roe_g) if std_roe_g is not None else None

                        median_roce_pct = to_pct_val(med_roce) if med_roce is not None else None
                        std_roce_pct = to_pct_val(std_roce) if std_roce is not None else None

                        # Retrieve relevant weights
                        gw_acc = get_factor_weight(growth_weight_map, "Accumulated Equity Growth", "Accumulated Profit Growth")
                        gw_roe = get_factor_weight(growth_weight_map, "ROE")
                        gw_roce = get_factor_weight(growth_weight_map, "ROCE")

                        sw_acc = get_factor_weight(stddev_weight_map, "Accumulated Equity Growth", "Accumulated Profit Growth")
                        sw_roe = get_factor_weight(stddev_weight_map, "ROE")
                        sw_roce = get_factor_weight(stddev_weight_map, "ROCE")

                        strength_pairs = [
                            (median_acc_pct, gw_acc),
                            (median_roe_pct, gw_roe),
                            (median_roce_pct, gw_roce),
                        ]

                        stddev_pairs = [
                            (std_acc_pct, sw_acc),
                            (std_roe_pct, sw_roe),
                            (std_roce_pct, sw_roce),
                        ]

                        weighted_strength = weighted_score(strength_pairs)
                        weighted_stddev = weighted_score(stddev_pairs)

                        additive_adjusted = None
                        scaled_adjusted = None
                        debt_adjusted = None
                        if weighted_strength is not None and weighted_stddev is not None:
                            additive_adjusted = weighted_strength - weighted_stddev
                            scaled_adjusted = compute_scaled_volatility_score(weighted_strength, weighted_stddev)
                        if scaled_adjusted is not None:
                            if median_interest_penalty is not None:
                                debt_adjusted = sum_finite_scores(scaled_adjusted, -debt_penalty_lambda * median_interest_penalty)
                            else:
                                missing_debt_penalty_count += 1

                        bs_rows.append(
                            {
                                "Company Name": name,
                                "Ticker": ticker,
                                "Weighted Balance Sheet Strength Score": weighted_strength,
                                "Weighted Balance Sheet Standard Deviation Score": weighted_stddev,
                                "Additive Volatility-Adjusted Balance Sheet Strength Score": additive_adjusted,
                                "Scaled Volatility-Adjusted Balance Sheet Strength Score": scaled_adjusted,
                                "Debt-Adjusted Balance Sheet Strength Score": debt_adjusted,
                            }
                        )

                    except Exception as e:
                        st.warning(f"Skipping company {name} ({ticker}) in Balance Sheet Score computation: {e}")

                if bs_rows:
                    if missing_debt_penalty_count:
                        st.warning(
                            f"Debt-adjusted scores are unavailable for {missing_debt_penalty_count} "
                            "selected companies because annual interest-penalty inputs are missing or invalid."
                        )
                    bs_df = pd.DataFrame(bs_rows)

                    bs_df = bs_df.sort_values(
                        by="Additive Volatility-Adjusted Balance Sheet Strength Score",
                        ascending=False,
                        na_position="last",
                    )

                    def fmt_score(x: Optional[float]) -> str:
                        if x is None or pd.isna(x):
                            return "—"
                        return f"{x:.2f}"

                    display_df = bs_df.copy()
                    display_df["Weighted Balance Sheet Strength Score"] = display_df[
                        "Weighted Balance Sheet Strength Score"
                    ].map(fmt_score)
                    display_df["Weighted Balance Sheet Standard Deviation Score"] = display_df[
                        "Weighted Balance Sheet Standard Deviation Score"
                    ].map(fmt_score)
                    display_df["Additive Volatility-Adjusted Balance Sheet Strength Score"] = display_df[
                        "Additive Volatility-Adjusted Balance Sheet Strength Score"
                    ].map(fmt_score)
                    display_df["Scaled Volatility-Adjusted Balance Sheet Strength Score"] = display_df[
                        "Scaled Volatility-Adjusted Balance Sheet Strength Score"
                    ].map(fmt_score)
                    display_df["Debt-Adjusted Balance Sheet Strength Score"] = display_df[
                        "Debt-Adjusted Balance Sheet Strength Score"
                    ].map(fmt_score)

                    st.subheader("Balance Sheet Score — Ranked by Additive Volatility-Adjusted Balance Sheet Strength Score")
                    st.dataframe(
                        display_df,
                        use_container_width=True,
                        column_config={
                            "Additive Volatility-Adjusted Balance Sheet Strength Score": st.column_config.TextColumn(
                                help="Formula: Weighted Balance Sheet Strength Score − Weighted Balance Sheet Standard Deviation Score"
                            ),
                            "Scaled Volatility-Adjusted Balance Sheet Strength Score": st.column_config.TextColumn(
                                help="Formula: G − ABS(G) × V / (1 + V), where G is Weighted Balance Sheet Strength and V is its Weighted Standard Deviation. Higher volatility cannot improve the score."
                            ),
                            "Debt-Adjusted Balance Sheet Strength Score": st.column_config.TextColumn(
                                help="Formula: Scaled Volatility-Adjusted Balance Sheet Strength Score − Lambda × Median Annual Interest Penalty. Equivalently, subtract Lambda × Median Interest Load % ÷ 100. Missing or invalid annual inputs make the adjustment unavailable."
                            ),
                        },
                    )
                else:
                    st.info("No Balance Sheet scores to display for the selected year range.")
