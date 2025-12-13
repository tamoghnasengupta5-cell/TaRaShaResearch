import streamlit as st
from core import *  # noqa: F401,F403

def render_balance_sheet_metrics_tab():
    st.title("Balance Sheet Metrics and Dashboard")

    with st.expander("Balance Sheet Metrics", expanded=True):
        conn = get_conn()
        init_db(conn)
        companies_df = list_companies(conn)
        if companies_df.empty:
            st.info("No companies in the database yet. Upload a spreadsheet above.")
        else:
            # Base selection: individual companies
            all_company_options = [
                f"{row.name} ({row.ticker}) [id={row.id}]" for _, row in companies_df.iterrows()
            ]

            # Default selection: companies not present in any bucket
            try:
                bucket_df = pd.read_sql_query(
                    "SELECT DISTINCT company_id FROM company_group_members",
                    conn,
                )
                if bucket_df.empty:
                    bucketed_ids = set()
                else:
                    bucketed_ids = {int(x) for x in bucket_df["company_id"].tolist()}
            except Exception:
                bucketed_ids = set()

            default_company_options: List[str] = []
            if bucketed_ids:
                for _, row in companies_df.iterrows():
                    if int(row.id) not in bucketed_ids:
                        default_company_options.append(
                            f"{row.name} ({row.ticker}) [id={row.id}]"
                        )
            else:
                # If no bucket information yet, fall back to all companies
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
                "Companies to analyze (for Balance Sheet metrics)",
                options=all_company_options,
                default=default_company_options,
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
                save_bucket = st.button("Save bucket", key="bs_save_bucket_button")

            if save_bucket:
                sel_company_ids: List[int] = []
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
            selected_company_ids: List[int] = []
            for opt in options:
                m = re.search(r"\[id=(\d+)\]$", opt)
                if m:
                    selected_company_ids.append(int(m.group(1)))

            # Add companies from selected buckets
            if bucket_names_selected:
                bucket_ids = [group_name_to_id[bname] for bname in bucket_names_selected if bname in group_name_to_id]
                if bucket_ids:
                    q_marks = ",".join("?" for _ in bucket_ids)
                    bucket_members_df = pd.read_sql_query(
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
                for cid in selected_company_ids:
                    # Ensure equity-derived metrics (Total Equity, Average Equity, ROE) are up-to-date
                    compute_and_store_total_equity_and_roe(conn, cid)

                    ann_acc = get_annual_accumulated_profit_series(conn, cid)
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
                        if not ann_acc.empty:
                            med_acc_g, std_acc_g = compute_growth_stats(
                                ann_acc,
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

                        # Interest Load % stats
                        med_interest_load, std_interest_load = (None, None)
                        if not ann_interest_load.empty:
                            df_il = ann_interest_load.copy()
                            df_il = df_il[(df_il['year'] >= yr_end) & (df_il['year'] <= yr_start)].sort_values('year')
                            vals_il = df_il['interest_load_pct'].dropna().astype(float).values
                            if vals_il.size > 0:
                                arr_il = np.array(vals_il, dtype=float)
                                med_interest_load = float(np.median(arr_il))
                                ddof_il = 1 if sample_bs and arr_il.size > 1 else 0
                                std_interest_load = float(np.std(arr_il, ddof=ddof_il)) if arr_il.size > 1 or ddof_il == 0 else None

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
                                help="Median Interest Load % over the selected range, where Interest Load % = (1 ÷ Interest Coverage Ratio) × 100. Lower is generally better.",
                            ),
                            "Interest Load Standard Deviation": st.column_config.TextColumn(
                                help="Standard deviation of Interest Load % over the selected range.",
                            ),
                        },
                    )
                else:
                    st.info("No Balance Sheet metrics to display for the selected year range.")

    st.markdown("---")

    with st.expander("Balance Sheet Score Dashboard", expanded=True):
        conn = get_conn()
        init_db(conn)
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
            growth_weights_df = pd.read_sql_query(
                "SELECT factor, weight FROM growth_weight_factors",
                conn,
            )
            stddev_weights_df = pd.read_sql_query(
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
                num = 0.0
                den = 0.0
                for val, wt in pairs:
                    if val is not None and wt is not None and wt > 0:
                        num += val * wt
                        den += wt
                if den == 0.0:
                    return None
                return num / den

            # Bucket-based company selection for Balance Sheet Score Dashboard
            groups_df = pd.read_sql_query(
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
                    bucket_members_df = pd.read_sql_query(
                        f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
                        conn,
                        params=group_ids,
                    )
                    if not bucket_members_df.empty:
                        score_company_ids = [int(x) for x in bucket_members_df["company_id"].tolist()]

            if not score_company_ids:
                st.info("Select at least one bucket to compute Balance Sheet scores.")

            compute_bs_scores = st.button(
                "Compute Balance Sheet Scores",
                type="primary",
                key="compute_bs_scores",
                disabled=(len(score_company_ids) == 0),
            )
            if compute_bs_scores:
                bs_rows: List[Dict[str, Optional[float]]] = []

                for cid in score_company_ids:
                    row = companies_df[companies_df["id"] == cid]
                    if row.empty:
                        continue
                    row = row.iloc[0]
                    name = row["name"]
                    ticker = row["ticker"]

                    ann_acc = get_annual_accumulated_profit_series(conn, cid)
                    ann_roe = get_annual_roe_series(conn, cid)
                    ann_roce = get_annual_roce_series(conn, cid)
                    ann_interest_load = get_annual_interest_load_series(conn, cid)

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
                        if not ann_acc.empty:
                            med_acc_g, std_acc_g = compute_growth_stats(
                                ann_acc,
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

                        # Interest Load % stats for score
                        median_interest_load_pct: Optional[float] = None
                        if not ann_interest_load.empty:
                            df_il = ann_interest_load.copy()
                            df_il = df_il[(df_il['year'] >= yr_end) & (df_il['year'] <= yr_start)].sort_values('year')
                            vals_il = df_il['interest_load_pct'].dropna().astype(float).values
                            if vals_il.size > 0:
                                arr_il = np.array(vals_il, dtype=float)
                                median_interest_load_pct = float(np.median(arr_il))

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
                            scaled_adjusted = weighted_strength / (1.0 + weighted_stddev)
                        if scaled_adjusted is not None and median_interest_load_pct is not None:
                            debt_adjusted = scaled_adjusted / (1.0 + (median_interest_load_pct / 100.0))

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
                                help="Formula: Weighted Balance Sheet Strength Score ÷ (1 + Weighted Balance Sheet Standard Deviation Score)"
                            ),
                            "Debt-Adjusted Balance Sheet Strength Score": st.column_config.TextColumn(
                                help="Formula: Scaled Volatility-Adjusted Balance Sheet Strength Score ÷ (1 + Median Interest Load % ÷ 100). Median Interest Load % is derived from Interest Coverage Ratio over the selected range; higher debt load reduces this score."
                            ),
                        },
                    )
                else:
                    st.info("No Balance Sheet scores to display for the selected year range.")