import streamlit as st
from core import *  # noqa: F401,F403

def render_pl_metrics_tab():
    st.title("P&L Metrics and Dashboard")

    with st.expander("P&L Metrics", expanded=True):
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
                        bucketed_ids = set(int(x) for x in bucket_df["company_id"].tolist())
                except Exception:
                    bucketed_ids = set()

                default_company_options = []
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
                    "Companies to analyze",
                    options=all_company_options,
                    default=default_company_options,
                )

                # Bucket definition: allow user to save current selection as a named bucket
                bucket_col1, bucket_col2 = st.columns([3, 1])
                with bucket_col1:
                    bucket_name = st.text_input(
                        "Save current selection as bucket (optional)",
                        placeholder="e.g. US_SaaS_Leaders",
                        key="bucket_name_input",
                    )
                with bucket_col2:
                    save_bucket = st.button("Save bucket", key="save_bucket_button")

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
                    group_name_to_id = {
                        str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()
                    }
                    bucket_names_selected = st.multiselect(
                        "Or select one or more saved buckets",
                        options=list(group_name_to_id.keys()),
                        key="bucket_select",
                    )

                yr_input = st.text_input(
                    "Year range (e.g., 'Recent - 2020' or '2023-2018')",
                    value="Recent - 2020",
                )

                stdev_mode = st.radio(
                    "Standard deviation mode",
                    ['Sample (ddof=1)', 'Population (ddof=0)'],
                    horizontal=True,
                )
                sample = (stdev_mode == 'Sample (ddof=1)')

                def parse_range(s: str, available_years: List[int]) -> Tuple[int, int]:
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

                bucket_company_ids: List[int] = []
                if bucket_names_selected:
                    group_ids = [group_name_to_id[name] for name in bucket_names_selected]
                    placeholders = ",".join(["?"] * len(group_ids))
                    bucket_df = pd.read_sql_query(
                        f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
                        conn,
                        params=group_ids,
                    )
                    if not bucket_df.empty:
                        bucket_company_ids = [int(x) for x in bucket_df["company_id"].tolist()]

                all_company_ids = sorted(set(selected_company_ids + bucket_company_ids))

                rows_out: List[Dict[str, Optional[float]]] = []
                for cid in all_company_ids:
                    ann_rev = get_annual_series(conn, cid)
                    if ann_rev.empty:
                        continue

                    try:
                        yr_start, yr_end = parse_range(yr_input, ann_rev['year'].tolist())
                        if yr_start < yr_end:
                            yr_start, yr_end = yr_end, yr_start

                        med_rev_g, std_rev_g = compute_growth_stats(
                            ann_rev,
                            yr_start,
                            yr_end,
                            stdev_sample=sample,
                        )

                        # Pretax Income growth stats (using same growth logic)
                        ann_pt = get_annual_pretax_income_series(conn, cid)
                        med_pt_g, std_pt_g = (None, None)
                        if not ann_pt.empty:
                            med_pt_g, std_pt_g = compute_growth_stats(
                                ann_pt,
                                yr_start,
                                yr_end,
                                stdev_sample=sample,
                                value_col="pretax_income",
                                abs_denom=True,
                            )

                        # Net Income growth stats
                        ann_ni = get_annual_net_income_series(conn, cid)
                        med_ni_g, std_ni_g = (None, None)
                        if not ann_ni.empty:
                            med_ni_g, std_ni_g = compute_growth_stats(
                                ann_ni,
                                yr_start,
                                yr_end,
                                stdev_sample=sample,
                                value_col="net_income",
                                abs_denom=True,
                            )

                        # NOPAT growth stats
                        ann_nopat = get_annual_nopat_series(conn, cid)
                        med_nopat_g, std_nopat_g = (None, None)
                        if not ann_nopat.empty:
                            med_nopat_g, std_nopat_g = compute_growth_stats(
                                ann_nopat,
                                yr_start,
                                yr_end,
                                stdev_sample=sample,
                                value_col="nopat",
                                abs_denom=True,
                            )

                        ann_om = get_annual_op_margin_series(conn, cid)
                        med_om, std_om, om_is_fraction = (None, None, True)
                        med_om_g, std_om_g = (None, None)
                        if not ann_om.empty:
                            med_om, std_om, om_is_fraction = compute_margin_stats(
                                ann_om,
                                yr_start,
                                yr_end,
                                stdev_sample=sample,
                            )
                            med_om_g, std_om_g = compute_margin_growth_stats(
                                ann_om,
                                yr_start,
                                yr_end,
                                stdev_sample=sample,
                            )

                        name, ticker = companies_df.loc[
                            companies_df['id'] == cid, ['name', 'ticker']
                        ].values[0]

                        def to_pct(x, assume_fraction=True):
                            if x is None:
                                return None
                            return x * 100.0 if assume_fraction else x

                        med_om_disp = to_pct(med_om, assume_fraction=om_is_fraction) if med_om is not None else None
                        std_om_disp = to_pct(std_om, assume_fraction=om_is_fraction) if std_om is not None else None

                        rows_out.append(
                            {
                                'Company Name': name,
                                'Ticker': ticker,
                                'Median Revenue Growth': (None if med_rev_g is None else med_rev_g * 100.0),
                                'Revenue Growth Standard Deviation': (None if std_rev_g is None else std_rev_g * 100.0),
                                'Median Pretax Income Growth': (None if med_pt_g is None else med_pt_g * 100.0),
                                'Pretax Income Growth Standard Deviation': (None if std_pt_g is None else std_pt_g * 100.0),
                                'Median Net Income Growth': (None if med_ni_g is None else med_ni_g * 100.0),
                                'Net Income Growth Standard Deviation': (None if std_ni_g is None else std_ni_g * 100.0),
                                'Median NOPAT Growth': (None if med_nopat_g is None else med_nopat_g * 100.0),
                                'NOPAT Growth Standard Deviation': (None if std_nopat_g is None else std_nopat_g * 100.0),
                                'Median YoY Operating Margin Growth': (None if med_om_g is None else med_om_g * 100.0),
                                'YoY Operating Margin Growth Standard Deviation': (None if std_om_g is None else std_om_g * 100.0),
                                'Median Operating Margin': med_om_disp,
                                'Operating Margin Standard Deviation': std_om_disp,
                            }
                        )
                    except Exception as e:
                        st.warning(f"Skipping company id={cid}: {e}")

                if rows_out:
                    out_df = pd.DataFrame(rows_out)

                    def fmt_pct(x):
                        return "—" if pd.isna(x) else f"{x:.2f}%"

                    show_df = out_df.copy()
                    show_df['Median Revenue Growth'] = show_df['Median Revenue Growth'].map(fmt_pct)
                    show_df['Revenue Growth Standard Deviation'] = show_df['Revenue Growth Standard Deviation'].map(fmt_pct)
                    show_df['Median Pretax Income Growth'] = show_df['Median Pretax Income Growth'].map(fmt_pct)
                    show_df['Pretax Income Growth Standard Deviation'] = show_df['Pretax Income Growth Standard Deviation'].map(fmt_pct)
                    show_df['Median Net Income Growth'] = show_df['Median Net Income Growth'].map(fmt_pct)
                    show_df['Net Income Growth Standard Deviation'] = show_df['Net Income Growth Standard Deviation'].map(fmt_pct)
                    show_df['Median NOPAT Growth'] = show_df['Median NOPAT Growth'].map(fmt_pct)
                    show_df['NOPAT Growth Standard Deviation'] = show_df['NOPAT Growth Standard Deviation'].map(fmt_pct)
                    show_df['Median YoY Operating Margin Growth'] = show_df['Median YoY Operating Margin Growth'].map(fmt_pct)
                    show_df['YoY Operating Margin Growth Standard Deviation'] = show_df['YoY Operating Margin Growth Standard Deviation'].map(fmt_pct)
                    show_df['Median Operating Margin'] = show_df['Median Operating Margin'].map(fmt_pct)
                    show_df['Operating Margin Standard Deviation'] = show_df['Operating Margin Standard Deviation'].map(fmt_pct)

                    st.subheader("Results")
                    st.dataframe(show_df, use_container_width=True)
                else:
                    st.info("No results to display yet.")

    st.markdown("---")

    with st.expander("3) P&L Score Dashboard", expanded=True):
        conn = get_conn()
        init_db(conn)
        companies_df = list_companies(conn)
        if companies_df.empty:
            st.info("No companies in the database yet. Upload a spreadsheet above.")
        else:
            yr_input_pl = st.text_input(
                "Year range for P&L Score (e.g., 'Recent - 2020' or '2023-2018')",
                value="Recent - 2020",
                key="pl_year_range",
            )
            stdev_mode_pl = st.radio(
                "Standard deviation mode for P&L Score",
                ['Sample (ddof=1)', 'Population (ddof=0)'],
                horizontal=True,
                key="pl_stdev_mode",
            )
            sample_pl = (stdev_mode_pl == 'Sample (ddof=1)')

            def parse_range_pl(s: str, available_years: List[int]) -> Tuple[int, int]:
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

            # Bucket-based company selection for P&L Score Dashboard
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
                    "Select one or more buckets for P&L Score computation",
                    options=list(group_name_to_id.keys()),
                    key="pl_score_bucket_select",
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
                st.info("Select at least one bucket to compute P&L scores.")

            compute_pl_scores = st.button(
                "Compute P&L Scores",
                type="primary",
                key="compute_pl_scores",
                disabled=(len(score_company_ids) == 0),
            )
            if compute_pl_scores:
                pl_rows: List[Dict[str, Optional[float]]] = []

                for cid in score_company_ids:
                    row = companies_df[companies_df["id"] == cid]
                    if row.empty:
                        continue
                    row = row.iloc[0]
                    name = row["name"]
                    ticker = row["ticker"]

                    ann_rev = get_annual_series(conn, cid)
                    if ann_rev.empty:
                        continue

                    try:
                        yr_start, yr_end = parse_range_pl(yr_input_pl, ann_rev['year'].tolist())
                        if yr_start < yr_end:
                            yr_start, yr_end = yr_end, yr_start

                        med_rev_g, std_rev_g = compute_growth_stats(ann_rev, yr_start, yr_end, stdev_sample=sample_pl)

                        # Pretax Income growth stats
                        ann_pt = get_annual_pretax_income_series(conn, cid)
                        med_pt_g, std_pt_g = (None, None)
                        if not ann_pt.empty:
                            med_pt_g, std_pt_g = compute_growth_stats(
                                ann_pt, yr_start, yr_end, stdev_sample=sample_pl, value_col="pretax_income", abs_denom=True
                            )

                        # Net Income growth stats
                        ann_ni = get_annual_net_income_series(conn, cid)
                        med_ni_g, std_ni_g = (None, None)
                        if not ann_ni.empty:
                            med_ni_g, std_ni_g = compute_growth_stats(
                                ann_ni, yr_start, yr_end, stdev_sample=sample_pl, value_col="net_income", abs_denom=True
                            )

                        # NOPAT growth stats
                        ann_nopat = get_annual_nopat_series(conn, cid)
                        med_nopat_g, std_nopat_g = (None, None)
                        if not ann_nopat.empty:
                            med_nopat_g, std_nopat_g = compute_growth_stats(
                                ann_nopat, yr_start, yr_end, stdev_sample=sample_pl, value_col="nopat", abs_denom=True
                            )

                        # Operating Margin stats
                        ann_om = get_annual_op_margin_series(conn, cid)
                        med_om, std_om, om_is_fraction = (None, None, True)
                        med_om_g, std_om_g = (None, None)
                        if not ann_om.empty:
                            med_om, std_om, om_is_fraction = compute_margin_stats(
                                ann_om, yr_start, yr_end, stdev_sample=sample_pl
                            )
                            med_om_g, std_om_g = compute_margin_growth_stats(
                                ann_om, yr_start, yr_end, stdev_sample=sample_pl
                            )

                        # Convert all metrics to percentage units where applicable
                        def to_pct_val(x: Optional[float]) -> Optional[float]:
                            if x is None:
                                return None
                            return x * 100.0

                        def to_pct_margin(x: Optional[float], assume_fraction: bool) -> Optional[float]:
                            if x is None:
                                return None
                            return x * 100.0 if assume_fraction else x

                        median_rev_pct = to_pct_val(med_rev_g) if med_rev_g is not None else None
                        std_rev_pct = to_pct_val(std_rev_g) if std_rev_g is not None else None

                        median_pt_pct = to_pct_val(med_pt_g) if med_pt_g is not None else None
                        std_pt_pct = to_pct_val(std_pt_g) if std_pt_g is not None else None

                        median_ni_pct = to_pct_val(med_ni_g) if med_ni_g is not None else None
                        std_ni_pct = to_pct_val(std_ni_g) if std_ni_g is not None else None

                        median_nopat_pct = to_pct_val(med_nopat_g) if med_nopat_g is not None else None
                        std_nopat_pct = to_pct_val(std_nopat_g) if std_nopat_g is not None else None

                        median_om_pct = to_pct_margin(med_om, om_is_fraction) if med_om is not None else None
                        std_om_pct = to_pct_margin(std_om, om_is_fraction) if std_om is not None else None

                        median_yoy_om_pct = to_pct_val(med_om_g) if med_om_g is not None else None
                        std_yoy_om_pct = to_pct_val(std_om_g) if std_om_g is not None else None

                        # Retrieve relevant weights
                        gw_rev = get_factor_weight(growth_weight_map, "Revenue Growth")
                        gw_pt = get_factor_weight(growth_weight_map, "Pretax Income Growth")
                        gw_ni = get_factor_weight(growth_weight_map, "Net Income Growth")
                        gw_nopat = get_factor_weight(growth_weight_map, "NOPAT Growth")
                        gw_om = get_factor_weight(growth_weight_map, "Operating Margin")
                        gw_yoy_om = get_factor_weight(growth_weight_map, "YoY Operating Margin Growth")

                        sw_rev = get_factor_weight(stddev_weight_map, "Revenue Growth")
                        sw_pt = get_factor_weight(stddev_weight_map, "Pretax Income Growth")
                        # Net Income Growth in stddev table may have a double space; try both.
                        sw_ni = get_factor_weight(stddev_weight_map, "Net Income Growth", "Net Income  Growth")
                        sw_nopat = get_factor_weight(stddev_weight_map, "NOPAT Growth")
                        sw_om = get_factor_weight(stddev_weight_map, "Operating Margin")
                        sw_yoy_om = get_factor_weight(stddev_weight_map, "YoY Operating Margin Growth")

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

                        growth_pairs = [
                            (median_rev_pct, gw_rev),
                            (median_pt_pct, gw_pt),
                            (median_ni_pct, gw_ni),
                            (median_nopat_pct, gw_nopat),
                            (median_om_pct, gw_om),
                            (median_yoy_om_pct, gw_yoy_om),
                        ]

                        stddev_pairs = [
                            (std_rev_pct, sw_rev),
                            (std_pt_pct, sw_pt),
                            (std_ni_pct, sw_ni),
                            (std_nopat_pct, sw_nopat),
                            (std_om_pct, sw_om),
                            (std_yoy_om_pct, sw_yoy_om),
                        ]

                        weighted_growth = weighted_score(growth_pairs)
                        weighted_stddev = weighted_score(stddev_pairs)
                        additive_adjusted = None
                        scaled_adjusted = None
                        if weighted_growth is not None and weighted_stddev is not None:
                            additive_adjusted = weighted_growth - weighted_stddev
                            scaled_adjusted = weighted_growth / (1.0 + weighted_stddev)

                        pl_rows.append(
                            {
                                "Company Name": name,
                                "Ticker": ticker,
                                "Weighted P&L Growth Score": weighted_growth,
                                "Weighted P&L Standard Deviation Score": weighted_stddev,
                                "Additive Volatility-Adjusted P&L Growth Score": additive_adjusted,
                                "Scaled Volatility-Adjusted P&L Growth Score": scaled_adjusted,
                            }
                        )

                    except Exception as e:
                        st.warning(f"Skipping company {name} ({ticker}) in P&L Score computation: {e}")

                if pl_rows:
                    pl_df = pd.DataFrame(pl_rows)

                    pl_df = pl_df.sort_values(
                        by="Additive Volatility-Adjusted P&L Growth Score",
                        ascending=False,
                        na_position="last",
                    )

                    def fmt_score(x: Optional[float]) -> str:
                        if x is None or pd.isna(x):
                            return "—"
                        return f"{x:.2f}"

                    display_df = pl_df.copy()
                    display_df["Weighted P&L Growth Score"] = display_df["Weighted P&L Growth Score"].map(fmt_score)
                    display_df["Weighted P&L Standard Deviation Score"] = display_df[
                        "Weighted P&L Standard Deviation Score"
                    ].map(fmt_score)
                    display_df["Additive Volatility-Adjusted P&L Growth Score"] = display_df[
                        "Additive Volatility-Adjusted P&L Growth Score"
                    ].map(fmt_score)
                    display_df["Scaled Volatility-Adjusted P&L Growth Score"] = display_df[
                        "Scaled Volatility-Adjusted P&L Growth Score"
                    ].map(fmt_score)

                    st.subheader("P&L Score — Ranked by Additive Volatility-Adjusted P&L Growth Score")
                    st.dataframe(
                        display_df,
                        use_container_width=True,
                        column_config={
                            "Additive Volatility-Adjusted P&L Growth Score": st.column_config.TextColumn(
                                help="Formula: Weighted P&L Growth Score − Weighted P&L Standard Deviation Score"
                            ),
                            "Scaled Volatility-Adjusted P&L Growth Score": st.column_config.TextColumn(
                                help="Formula: Weighted P&L Growth Score ÷ (1 + Weighted P&L Standard Deviation Score)"
                            ),
                        },
                    )
                else:
                    st.info("No P&L scores to display for the selected year range.")

