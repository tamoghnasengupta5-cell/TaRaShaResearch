import streamlit as st
from core import *  # noqa: F401,F403
from typing import List


# Metric tables that store per-company financial data and can be safely cleared
METRIC_TABLES_FOR_CLEANUP: List[str] = [
    "price_change_annual",
    "revenues_annual",
    "revenues_ttm",
    "op_margin_annual",
    "op_margin_ttm",
    "pretax_income_annual",
    "pretax_income_ttm",
    "net_income_annual",
    "net_income_ttm",
    "eff_tax_rate_annual",
    "eff_tax_rate_ttm",
    "ebit_annual",
    "ebit_ttm",
    "interest_expense_annual",
    "interest_expense_ttm",
    "research_and_development_expense_annual",
    "capital_expenditures_annual",
    "depreciation_amortization_annual",
    "operating_income_annual",
    "operating_income_ttm",
    "interest_coverage_annual",
    "interest_load_annual",
    "nopat_annual",
    "shareholders_equity_annual",
    "shareholders_equity_ttm",
    "retained_earnings_annual",
    "retained_earnings_ttm",
    "comprehensive_income_annual",
    "comprehensive_income_ttm",
    "accumulated_profit_annual",
    "total_equity_annual",
    "average_equity_annual",
    "roe_annual",
    "total_assets_annual",
    "total_assets_ttm",
    "total_current_assets_annual",
    "total_current_assets_ttm",
    "total_current_liabilities_annual",
    "total_current_liabilities_ttm",
    "total_long_term_liabilities_annual",
    "total_long_term_liabilities_ttm",
    "total_debt_annual",
    "total_debt_ttm",
    "current_debt_annual",
    "current_debt_ttm",
    "cash_and_cash_equivalents_annual",
    "cash_and_cash_equivalents_ttm",
    "long_term_investments_annual",
    "long_term_investments_ttm",
    "capital_employed_annual",
    "invested_capital_annual",
    "roce_annual",
    "non_cash_working_capital_annual",
    "revenue_yield_non_cash_working_capital_annual",    "fcff_annual",
    "reinvestment_rate_annual",
    "rd_spend_rate_annual",
    "net_debt_issued_paid_annual",
    "market_capitalization_annual",
    "roic_direct_upload_annual",
    "debt_equity_annual",
    "levered_beta_annual",
    "cost_of_equity_annual",
    "default_spread_annual",
    "pre_tax_cost_of_debt_annual",
    "wacc_annual",

]


def delete_company_metric_data(conn, company_ids: List[int]) -> None:
    """Delete all stored metric rows for the given companies.

    This clears data across all metric tables but does **not** delete the
    companies themselves or any bucket definitions.
    """
    if not company_ids:
        return

    cur = conn.cursor()
    placeholders = ",".join(["?"] * len(company_ids))
    params = list(company_ids)

    for table in METRIC_TABLES_FOR_CLEANUP:
        cur.execute(f"DELETE FROM {table} WHERE company_id IN ({placeholders})", params)

    conn.commit()
def render_admin_tab():
    st.title("Admin")

    st.write(
        "Configure metric weight factors and manage company buckets. "
        "All changes are stored in the same SQLite database (`app.db`)."
    )

    conn = get_conn()
    init_db(conn)

    tab_weights, tab_buckets, tab_rfr, tab_iapm, tab_erp, tab_mctr, tab_industry_beta, tab_cleanup, tab_formula = st.tabs(


        [


            "Weights",


            "Buckets",


            "Risk Free Rate",


            "Index Annual Price Movement",


            "Implied Equity Risk Premium",


            "Marginal Corporate Tax Rate",


            "Industry Beta",


            "Data Cleanup",


            "Formula",


        ]


    )


    # -----------------------------
    # Weights sub-tab
    # -----------------------------
    with tab_weights:
        st.subheader("Growth Weight Factors")

        growth_df = pd.read_sql_query(
            "SELECT id, factor, weight FROM growth_weight_factors ORDER BY id",
            conn,
        )

        with st.form("growth_weight_factors_form"):
            st.write("Adjust the weight for each growth metric (for example, 0–100).")
            new_growth_weights = {}
            for _, row in growth_df.iterrows():
                new_val = st.number_input(
                    label=row["factor"],
                    min_value=0.0,
                    max_value=1000.0,
                    value=float(row["weight"]),
                    step=1.0,
                    key=f"growth_weight_{int(row['id'])}",
                )
                new_growth_weights[int(row["id"])] = float(new_val)

            submitted_growth = st.form_submit_button("Save Growth Weights")
            if submitted_growth:
                cur = conn.cursor()
                for row_id, w in new_growth_weights.items():
                    cur.execute(
                        "UPDATE growth_weight_factors SET weight = ? WHERE id = ?",
                        (w, row_id),
                    )
                conn.commit()
                st.success("Growth weight factors updated successfully.")

        st.markdown("---")

        st.subheader("Standard Deviation Weight Factors")

        stddev_df = pd.read_sql_query(
            "SELECT id, factor, weight FROM stddev_weight_factors ORDER BY id",
            conn,
        )

        with st.form("stddev_weight_factors_form"):
            st.write("Adjust the weight for each standard deviation metric.")
            new_stddev_weights = {}
            for _, row in stddev_df.iterrows():
                new_val = st.number_input(
                    label=row["factor"],
                    min_value=0.0,
                    max_value=1000.0,
                    value=float(row["weight"]),
                    step=1.0,
                    key=f"stddev_weight_{int(row['id'])}",
                )
                new_stddev_weights[int(row["id"])] = float(new_val)

            submitted_stddev = st.form_submit_button("Save Standard Deviation Weights")
            if submitted_stddev:
                cur = conn.cursor()
                for row_id, w in new_stddev_weights.items():
                    cur.execute(
                        "UPDATE stddev_weight_factors SET weight = ? WHERE id = ?",
                        (w, row_id),
                    )
                conn.commit()
                st.success("Standard deviation weight factors updated successfully.")

    # -----------------------------
    # Buckets sub-tab
    # -----------------------------
    with tab_buckets:
        st.subheader("Bucket Management")

        # Summary of all buckets
        buckets_df = pd.read_sql_query(
            """
            SELECT g.id, g.name, COUNT(m.company_id) AS company_count
            FROM company_groups g
            LEFT JOIN company_group_members m ON g.id = m.group_id
            GROUP BY g.id, g.name
            ORDER BY g.name
            """,
            conn,
        )

        if buckets_df.empty:
            st.info(
                "No buckets defined yet. Buckets are created from the P&L or Balance Sheet tabs "
                "when you save a selection."
            )
            return

        summary_df = buckets_df.rename(
            columns={"name": "Bucket Name", "company_count": "Number of Companies"}
        )[["Bucket Name", "Number of Companies"]]

        st.markdown("**Existing buckets:**")
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

        bucket_names = buckets_df["name"].tolist()

        # Orphan companies: companies not assigned to any bucket
        orphan_df = pd.read_sql_query(
            """
            SELECT c.id, c.name, c.ticker
            FROM companies c
            LEFT JOIN company_group_members m ON c.id = m.company_id
            WHERE m.group_id IS NULL
            ORDER BY c.name
            """,
            conn,
        )

        st.markdown("**Orphan companies (not in any bucket):**")
        if orphan_df.empty:
            st.info("All companies are currently assigned to at least one bucket.")
        else:
            orphan_display = orphan_df.copy()
            orphan_display["Label"] = orphan_display.apply(
                lambda r: f"{r['name']} ({r['ticker']}) [id={r['id']}]",
                axis=1,
            )
            st.dataframe(
                orphan_display[["Label"]],
                use_container_width=True,
                hide_index=True,
            )

            with st.form("assign_orphans_to_bucket"):
                orphan_options = orphan_display["Label"].tolist()
                selected_orphan_labels = st.multiselect(
                    "Select orphan companies to assign to a bucket",
                    options=orphan_options,
                )
                target_bucket_name = st.selectbox(
                    "Assign selected orphan companies to bucket",
                    options=bucket_names,
                )
                submitted_assign = st.form_submit_button("Assign")
                if submitted_assign:
                    if not selected_orphan_labels:
                        st.warning("Please select at least one orphan company to assign.")
                    else:
                        ids = (
                            orphan_display.loc[
                                orphan_display["Label"].isin(selected_orphan_labels),
                                "id",
                            ]
                            .astype(int)
                            .tolist()
                        )
                        if ids:
                            cur = conn.cursor()
                            cur.execute(
                                "SELECT id FROM company_groups WHERE name = ?",
                                (target_bucket_name,),
                            )
                            row = cur.fetchone()
                            if row is not None:
                                target_gid = int(row[0])
                                cur.executemany(
                                    "INSERT OR IGNORE INTO company_group_members(group_id, company_id) VALUES(?, ?)",
                                    [(target_gid, cid) for cid in ids],
                                )
                                conn.commit()
                                st.success(
                                    f"Assigned {len(ids)} company(ies) to bucket '{target_bucket_name}'."
                                )
                                st.experimental_rerun()

        selected_bucket_name = st.selectbox(
            "Select a bucket to manage",
            options=bucket_names,
        )

        selected_row = buckets_df[buckets_df["name"] == selected_bucket_name].iloc[0]
        group_id = int(selected_row["id"])

        # Load companies in the selected bucket
        members_df = pd.read_sql_query(
            """
            SELECT c.id, c.name, c.ticker
            FROM company_group_members m
            JOIN companies c ON c.id = m.company_id
            WHERE m.group_id = ?
            ORDER BY c.name
            """,
            conn,
            params=(group_id,),
        )

        if members_df.empty:
            st.info("This bucket currently has no companies.")
        else:
            st.markdown("**Companies in this bucket:**")
            members_display = members_df.copy()
            members_display["Company"] = (
                members_display["name"] + " (" + members_display["ticker"] + ")"
            )
            st.dataframe(
                members_display[["Company"]],
                use_container_width=True,
                hide_index=True,
            )

            # Multiselect to remove one or more companies from the bucket
            labels = []
            label_to_id = {}
            for _, row in members_df.iterrows():
                label = f"{row['name']} ({row['ticker']})"
                labels.append(label)
                label_to_id[label] = int(row["id"])

            to_remove = st.multiselect(
                "Select one or more companies to remove from this bucket",
                options=labels,
            )

            if st.button(
                "Remove selected companies from bucket",
                disabled=len(to_remove) == 0,
            ):
                ids_to_delete = [label_to_id[lbl] for lbl in to_remove]
                cur = conn.cursor()
                placeholders = ",".join(["?"] * len(ids_to_delete))
                cur.execute(
                    f"DELETE FROM company_group_members "
                    f"WHERE group_id = ? AND company_id IN ({placeholders})",
                    [group_id, *ids_to_delete],
                )
                conn.commit()
                st.success(
                    f"Removed {len(ids_to_delete)} compan"
                    f"{'y' if len(ids_to_delete) == 1 else 'ies'} from bucket "
                    f"'{selected_bucket_name}'."
                )
                st.experimental_rerun()

        st.markdown("---")
        col_confirm, col_delete = st.columns([3, 1])
        with col_confirm:
            confirm_delete = st.checkbox(
                f"Confirm permanent deletion of bucket '{selected_bucket_name}'. "
                "This will delete the grouping but not the underlying companies.",
                key="confirm_delete_bucket",
            )
        with col_delete:
            if st.button(
                "Delete bucket",
                type="secondary",
                disabled=not confirm_delete,
            ):
                cur = conn.cursor()
                cur.execute("DELETE FROM company_groups WHERE id = ?", (group_id,))
                conn.commit()
                st.success(f"Bucket '{selected_bucket_name}' deleted.")
                st.experimental_rerun()
    
    # -----------------------------
    # Risk Free Rate sub-tab
    # -----------------------------
    with tab_rfr:
        st.subheader("Risk Free Rate (10-Year Government Bond Yield)")
        st.caption(
            "Values are stored as percentages. Only the latest year is editable. "
            "When you add the ensuing year, the previous year becomes read-only automatically."
        )

        rfr_df = pd.read_sql_query(
            """
            SELECT
                year,
                usa_rf   AS 'USA (Rf) %',
                india_rf AS 'India (Rf) %',
                china_rf AS 'China (Rf) %',
                japan_rf AS 'Japan (Rf) %'
            FROM risk_free_rates
            ORDER BY year DESC
            """,
            conn,
        )

        if rfr_df.empty:
            st.info("No risk-free rate rows found in the database yet.")
        else:
            latest_year = int(rfr_df["year"].max())

            # Display stored history
            display_df = rfr_df.copy()
            for c in ["USA (Rf) %", "India (Rf) %", "China (Rf) %", "Japan (Rf) %"]:
                display_df[c] = pd.to_numeric(display_df[c], errors="coerce").round(2)

            st.markdown("**Stored series:**")
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            # Edit the latest year only
            st.markdown("---")
            st.markdown(f"**Edit latest year ({latest_year}) only:**")

            latest_row = rfr_df[rfr_df["year"] == latest_year].iloc[0]

            with st.form("rfr_update_latest_form"):
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    usa_new = st.number_input(
                        "USA (Rf) %",
                        value=float(latest_row["USA (Rf) %"]),
                        step=0.01,
                        format="%.2f",
                        key=f"rfr_usa_edit_{latest_year}",
                    )
                with c2:
                    india_new = st.number_input(
                        "India (Rf) %",
                        value=float(latest_row["India (Rf) %"]),
                        step=0.01,
                        format="%.2f",
                        key=f"rfr_ind_edit_{latest_year}",
                    )
                with c3:
                    china_new = st.number_input(
                        "China (Rf) %",
                        value=float(latest_row["China (Rf) %"]),
                        step=0.01,
                        format="%.2f",
                        key=f"rfr_chn_edit_{latest_year}",
                    )
                with c4:
                    japan_new = st.number_input(
                        "Japan (Rf) %",
                        value=float(latest_row["Japan (Rf) %"]),
                        step=0.01,
                        format="%.2f",
                        key=f"rfr_jpn_edit_{latest_year}",
                    )

                save_latest = st.form_submit_button("Save latest-year rates")
                if save_latest:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE risk_free_rates
                        SET usa_rf = ?, india_rf = ?, china_rf = ?, japan_rf = ?, updated_at = ?
                        WHERE year = ?
                        """,
                        (
                            float(usa_new),
                            float(india_new),
                            float(china_new),
                            float(japan_new),
                            datetime.utcnow().isoformat(),
                            int(latest_year),
                        ),
                    )
                    conn.commit()
                    st.success(f"Saved risk-free rates for {latest_year}.")
                    st.experimental_rerun()

            # Add the ensuing year
            st.markdown("---")
            next_year = int(latest_year) + 1
            st.markdown(f"**Add ensuing year ({next_year}):**")

            with st.form("rfr_add_next_year_form"):
                new_year = st.number_input(
                    "Year",
                    min_value=next_year,
                    value=next_year,
                    step=1,
                    key="rfr_new_year",
                )
                c1n, c2n, c3n, c4n = st.columns(4)
                with c1n:
                    usa_add = st.number_input(
                        "USA (Rf) % (new year)",
                        value=float(latest_row["USA (Rf) %"]),
                        step=0.01,
                        format="%.2f",
                        key="rfr_usa_add",
                    )
                with c2n:
                    india_add = st.number_input(
                        "India (Rf) % (new year)",
                        value=float(latest_row["India (Rf) %"]),
                        step=0.01,
                        format="%.2f",
                        key="rfr_ind_add",
                    )
                with c3n:
                    china_add = st.number_input(
                        "China (Rf) % (new year)",
                        value=float(latest_row["China (Rf) %"]),
                        step=0.01,
                        format="%.2f",
                        key="rfr_chn_add",
                    )
                with c4n:
                    japan_add = st.number_input(
                        "Japan (Rf) % (new year)",
                        value=float(latest_row["Japan (Rf) %"]),
                        step=0.01,
                        format="%.2f",
                        key="rfr_jpn_add",
                    )

                add_row = st.form_submit_button(f"Add {next_year}")
                if add_row:
                    if int(new_year) != int(next_year):
                        st.error(f"Please add only the ensuing year: {next_year}.")
                    else:
                        try:
                            cur = conn.cursor()
                            cur.execute(
                                """
                                INSERT INTO risk_free_rates(year, usa_rf, india_rf, china_rf, japan_rf, updated_at)
                                VALUES(?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    int(next_year),
                                    float(usa_add),
                                    float(india_add),
                                    float(china_add),
                                    float(japan_add),
                                    datetime.utcnow().isoformat(),
                                ),
                            )
                            conn.commit()
                            st.success(f"Added risk-free rates for {next_year}.")
                            st.experimental_rerun()
                        except Exception as e:
                            st.error(f"Could not add {next_year}. Reason: {e}")
# -----------------------------
    
    # -----------------------------
    
    # -----------------------------
    # Index Annual Price Movement sub-tab
    # -----------------------------
    with tab_iapm:
        st.subheader("Index Annual Price Movement (YoY %)")
        st.caption(
            "Values are stored as percentages. Only the latest year is editable. "
            "When you add the ensuing year, the previous year becomes read-only automatically."
        )

        iapm_df = pd.read_sql_query(
            """
            SELECT
                year,
                nasdaq_composite AS 'NASDAQ Composite %',
                sp500            AS 'S&P 500 %'
            FROM index_annual_price_movement
            ORDER BY year DESC
            """,
            conn,
        )

        if iapm_df.empty:
            st.info("No index annual price-movement rows found in the database yet.")
        else:
            latest_year = int(iapm_df["year"].max())

            # Display stored history
            display_df = iapm_df.copy()
            for c in ["NASDAQ Composite %", "S&P 500 %"]:
                display_df[c] = pd.to_numeric(display_df[c], errors="coerce").round(2)

            st.markdown("**Stored series:**")
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            # Edit the latest year only
            st.markdown("---")
            st.markdown(f"**Edit latest year ({latest_year}) only:**")

            latest_row = iapm_df[iapm_df["year"] == latest_year].iloc[0]

            with st.form("iapm_update_latest_form"):
                c1, c2 = st.columns(2)
                with c1:
                    nas_new = st.number_input(
                        "NASDAQ Composite %",
                        value=float(latest_row["NASDAQ Composite %"]),
                        step=0.01,
                        format="%.2f",
                        key=f"iapm_nas_edit_{latest_year}",
                    )
                with c2:
                    sp_new = st.number_input(
                        "S&P 500 %",
                        value=float(latest_row["S&P 500 %"]),
                        step=0.01,
                        format="%.2f",
                        key=f"iapm_sp_edit_{latest_year}",
                    )

                save_latest = st.form_submit_button("Save latest-year values")
                if save_latest:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE index_annual_price_movement
                        SET nasdaq_composite = ?, sp500 = ?, updated_at = ?
                        WHERE year = ?
                        """,
                        (
                            float(nas_new),
                            float(sp_new),
                            datetime.utcnow().isoformat(),
                            int(latest_year),
                        ),
                    )
                    conn.commit()
                    st.success(f"Saved index values for {latest_year}.")
                    st.experimental_rerun()

            # Add the ensuing year
            st.markdown("---")
            next_year = int(latest_year) + 1
            st.markdown(f"**Add ensuing year ({next_year}):**")

            with st.form("iapm_add_next_year_form"):
                new_year = st.number_input(
                    "Year",
                    min_value=next_year,
                    value=next_year,
                    step=1,
                    key="iapm_new_year",
                )

                c1n, c2n = st.columns(2)
                with c1n:
                    nas_add = st.number_input(
                        "NASDAQ Composite % (new year)",
                        value=float(latest_row["NASDAQ Composite %"]),
                        step=0.01,
                        format="%.2f",
                        key="iapm_nas_add",
                    )
                with c2n:
                    sp_add = st.number_input(
                        "S&P 500 % (new year)",
                        value=float(latest_row["S&P 500 %"]),
                        step=0.01,
                        format="%.2f",
                        key="iapm_sp_add",
                    )

                add_row = st.form_submit_button(f"Add {next_year}")
                if add_row:
                    if int(new_year) != int(next_year):
                        st.error(f"Please add only the ensuing year: {next_year}.")
                    else:
                        try:
                            cur = conn.cursor()
                            cur.execute(
                                """
                                INSERT INTO index_annual_price_movement(year, nasdaq_composite, sp500, updated_at)
                                VALUES(?, ?, ?, ?)
                                """,
                                (
                                    int(next_year),
                                    float(nas_add),
                                    float(sp_add),
                                    datetime.utcnow().isoformat(),
                                ),
                            )
                            conn.commit()
                            st.success(f"Added index values for {next_year}.")
                            st.experimental_rerun()
                        except Exception as e:
                            st.error(f"Could not add {next_year}. Reason: {e}")


    # -----------------------------
    # Implied Equity Risk Premium sub-tab
    # -----------------------------
    with tab_erp:
        st.subheader("US Implied Equity Risk Premium")
        st.caption("Stored as % values in the database. Only the latest year is editable.")

        erp_df = pd.read_sql_query(
            """
            SELECT
                year AS year,
                implied_erp AS "US Implied Equity Risk Premium %",
                notes AS "Notes"
            FROM implied_equity_risk_premium_usa
            ORDER BY year DESC
            """,
            conn,
        )

        if erp_df.empty:
            st.info("No implied equity risk premium rows found yet.")
        else:
            # Display full series (read-only)
            display_df = erp_df.copy()
            display_df["US Implied Equity Risk Premium %"] = pd.to_numeric(
                display_df["US Implied Equity Risk Premium %"], errors="coerce"
            ).round(2)
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            latest_year = int(pd.to_numeric(erp_df["year"], errors="coerce").max())
            latest_row = erp_df.loc[erp_df["year"] == latest_year].iloc[0]
            current_erp = float(latest_row["US Implied Equity Risk Premium %"])

            st.markdown("---")
            st.markdown(f"**Edit latest year ({latest_year}) only**")

            with st.form("erp_update_latest_form"):
                new_erp = st.number_input(
                    "US Implied Equity Risk Premium %",
                    value=float(current_erp),
                    min_value=0.0,
                    step=0.01,
                    format="%.2f",
                    key=f"erp_{latest_year}",
                )
                save_btn = st.form_submit_button("Save")

                if save_btn:
                    try:
                        ts = datetime.utcnow().isoformat()
                        conn.execute(
                            "UPDATE implied_equity_risk_premium_usa SET implied_erp = ?, updated_at = ? WHERE year = ?",
                            (float(new_erp), ts, int(latest_year)),
                        )
                        conn.commit()
                        st.success(f"Saved implied equity risk premium for {latest_year}.")
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Could not save. Reason: {e}")

# Marginal Corporate Tax Rate sub-tab
    # -----------------------------
    with tab_mctr:
        st.subheader("Marginal Corporate Tax Rate (effective, for valuation)")
        st.caption(
            "Values are stored as percentages. Edit the rate and notes, add/remove countries, then click Save."
        )

        mctr_df = pd.read_sql_query(
            """
            SELECT
                country AS 'Country',
                effective_rate AS 'Effective Marginal Corporate Tax Rate',
                notes AS 'Breakdown & Notes'
            FROM marginal_corporate_tax_rates
            ORDER BY country
            """,
            conn,
        )

        if mctr_df.empty:
            st.info("No marginal corporate tax-rate rows found in the database yet.")
        else:
            # Display & edit
            display_df = mctr_df.copy()
            display_df["Effective Marginal Corporate Tax Rate"] = pd.to_numeric(
                display_df["Effective Marginal Corporate Tax Rate"], errors="coerce"
            ).round(2)

            edited_df = st.data_editor(
                display_df,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key="mctr_editor",
                column_config={
                    "Country": st.column_config.TextColumn(
                        "Country",
                        required=True,
                    ),
                    "Effective Marginal Corporate Tax Rate": st.column_config.NumberColumn(
                        "Effective Marginal Corporate Tax Rate",
                        help="Percent (e.g., 25.70 means 25.70%).",
                        step=0.01,
                        format="%.2f",
                        required=True,
                    ),
                    "Breakdown & Notes": st.column_config.TextColumn(
                        "Breakdown & Notes",
                    ),
                },
            )

            if st.button("Save marginal corporate tax rates", type="primary"):
                df = edited_df.copy()

                # Basic cleanup / validation
                df["Country"] = df["Country"].astype(str).str.strip()
                df = df[df["Country"].str.len() > 0].copy()

                if df.empty:
                    st.error("Please keep at least one country row (or add one) before saving.")
                elif df["Country"].duplicated().any():
                    st.error("Duplicate country names are not allowed. Please make each country unique.")
                else:
                    df["Effective Marginal Corporate Tax Rate"] = pd.to_numeric(
                        df["Effective Marginal Corporate Tax Rate"], errors="coerce"
                    )

                    if df["Effective Marginal Corporate Tax Rate"].isna().any():
                        st.error("All rows must have a valid numeric Effective Marginal Corporate Tax Rate.")
                    else:
                        ts = datetime.utcnow().isoformat()
                        rows = []
                        for _, r in df.iterrows():
                            notes = r.get("Breakdown & Notes")
                            notes_val = "" if pd.isna(notes) else str(notes)
                            rows.append(
                                (
                                    str(r["Country"]).strip(),
                                    float(r["Effective Marginal Corporate Tax Rate"]),
                                    notes_val,
                                    ts,
                                )
                            )

                        cur = conn.cursor()
                        # Rewrite the table to match the editor (supports add/edit/delete)
                        cur.execute("DELETE FROM marginal_corporate_tax_rates")
                        cur.executemany(
                            "INSERT INTO marginal_corporate_tax_rates(country, effective_rate, notes, updated_at) VALUES(?, ?, ?, ?)",
                            rows,
                        )
                        conn.commit()
                        st.success("Saved marginal corporate tax rates.")
                        st.experimental_rerun()

# Data Cleanup sub-tab
    # -----------------------------
    
    # -----------------------------
    # Industry Beta sub-tab
    # -----------------------------
    with tab_industry_beta:
        st.subheader("Industry Beta (Unlevered and Cash-Adjusted)")
        st.caption(
            "Store and override industry bucket betas used in valuation work. "
            "Edit existing rows or add new bucket/sector combinations, then click Save."
        )

        ib_df = pd.read_sql_query(
            """
            SELECT
                user_industry_bucket AS "User's Industry Bucket",
                mapped_sector        AS "Mapped Sector",
                unlevered_beta       AS "Unlevered Beta",
                cash_adjusted_beta   AS "Cash-Adjusted Beta"
            FROM industry_betas
            ORDER BY user_industry_bucket, mapped_sector
            """,
            conn,
        )

        if ib_df.empty:
            ib_df = pd.DataFrame(
                columns=[
                    "User's Industry Bucket",
                    "Mapped Sector",
                    "Unlevered Beta",
                    "Cash-Adjusted Beta",
                ]
            )

        display_df = ib_df.copy()
        for c in ["Unlevered Beta", "Cash-Adjusted Beta"]:
            display_df[c] = pd.to_numeric(display_df[c], errors="coerce").round(2)

        edited_df = st.data_editor(
            display_df,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            key="industry_beta_editor",
            column_config={
                "User's Industry Bucket": st.column_config.TextColumn(
                    "User's Industry Bucket",
                    required=True,
                ),
                "Mapped Sector": st.column_config.TextColumn(
                    "Mapped Sector",
                    required=True,
                ),
                "Unlevered Beta": st.column_config.NumberColumn(
                    "Unlevered Beta",
                    step=0.01,
                    format="%.2f",
                    required=True,
                ),
                "Cash-Adjusted Beta": st.column_config.NumberColumn(
                    "Cash-Adjusted Beta",
                    step=0.01,
                    format="%.2f",
                    required=True,
                ),
            },
        )

        if st.button("Save industry betas", type="primary"):
            df = edited_df.copy()

            # Clean/normalize
            for col in ["User's Industry Bucket", "Mapped Sector"]:
                df[col] = df[col].astype(str).str.strip()

            df = df[(df["User's Industry Bucket"] != "") & (df["Mapped Sector"] != "")].copy()

            df["Unlevered Beta"] = pd.to_numeric(df["Unlevered Beta"], errors="coerce")
            df["Cash-Adjusted Beta"] = pd.to_numeric(df["Cash-Adjusted Beta"], errors="coerce")

            if df.empty:
                st.error("Please provide at least one valid row (bucket + sector + betas).")
            elif df[["Unlevered Beta", "Cash-Adjusted Beta"]].isna().any().any():
                st.error("Both beta columns must be numeric for every row.")
            elif df.duplicated(subset=["User's Industry Bucket", "Mapped Sector"]).any():
                st.error("Duplicate bucket + sector combinations found. Please make them unique.")
            else:
                ts = datetime.utcnow().isoformat()
                cur = conn.cursor()
                cur.execute("DELETE FROM industry_betas")
                cur.executemany(
                    "INSERT INTO industry_betas(user_industry_bucket, mapped_sector, unlevered_beta, cash_adjusted_beta, updated_at) VALUES(?, ?, ?, ?, ?)",
                    [
                        (
                            str(r["User's Industry Bucket"]),
                            str(r["Mapped Sector"]),
                            float(r["Unlevered Beta"]),
                            float(r["Cash-Adjusted Beta"]),
                            ts,
                        )
                        for _, r in df.iterrows()
                    ],
                )
                conn.commit()
                st.success("Industry beta table saved.")
                st.experimental_rerun()

    with tab_cleanup:
        st.subheader("Data cleanup by bucket")

        st.warning(
            "These actions permanently delete stored metric data for the selected companies "
            "across all annual/TTM tables. Bucket definitions and the companies themselves are **not** deleted."
        )

        buckets_df = pd.read_sql_query(
            "SELECT id, name FROM company_groups ORDER BY name",
            conn,
        )

        if buckets_df.empty:
            st.info(
                "No buckets defined yet. Once you have at least one bucket, you can use it to clear data."
            )
        else:
            # Map bucket names to ids
            bucket_name_to_id = {
                str(row["name"]): int(row["id"]) for _, row in buckets_df.iterrows()
            }

            st.markdown("### A. Delete data for all companies in one or more buckets")
            selected_bucket_names = st.multiselect(
                "Select bucket(s) whose companies' stored metric data should be cleared",
                options=list(bucket_name_to_id.keys()),
                key="cleanup_bucket_multi",
            )

            confirm_multi = st.checkbox(
                "I understand this will permanently delete stored metric data for **all** companies "
                "in the selected bucket(s).",
                key="confirm_cleanup_multi",
            )

            if st.button(
                "Delete data for selected buckets",
                type="primary",
                disabled=(len(selected_bucket_names) == 0 or not confirm_multi),
                key="btn_cleanup_multi",
            ):
                group_ids = [bucket_name_to_id[name] for name in selected_bucket_names]
                placeholders = ",".join(["?"] * len(group_ids))
                bucket_members_df = pd.read_sql_query(
                    f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
                    conn,
                    params=group_ids,
                )

                company_ids = sorted(
                    {int(x) for x in bucket_members_df["company_id"].tolist()}
                ) if not bucket_members_df.empty else []

                if not company_ids:
                    st.info("No companies found in the selected bucket(s). Nothing to delete.")
                else:
                    delete_company_metric_data(conn, company_ids)
                    st.success(
                        f"Cleared stored metric data for {len(company_ids)} company(ies) "
                        f"across all metric tables."
                    )

            st.markdown("---")
            st.markdown("### B. Selectively delete data for companies within a single bucket")

            single_bucket_name = st.selectbox(
                "Select a bucket for selective cleanup",
                options=list(bucket_name_to_id.keys()),
                key="cleanup_bucket_single",
            )

            single_bucket_id = bucket_name_to_id.get(single_bucket_name)
            members_df = pd.read_sql_query(
                """
                SELECT DISTINCT c.id, c.name, c.ticker
                FROM company_group_members m
                JOIN companies c ON c.id = m.company_id
                WHERE m.group_id = ?
                ORDER BY c.name
                """,
                conn,
                params=(single_bucket_id,),
            )

            if members_df.empty:
                st.info("The selected bucket does not contain any companies.")
            else:
                company_labels = []
                label_to_id = {}
                for _, row in members_df.iterrows():
                    label = f"{row['name']} ({row['ticker']})"
                    cid = int(row["id"])
                    company_labels.append(label)
                    label_to_id[label] = cid

                selected_company_labels = st.multiselect(
                    "Select one or more companies whose stored metric data should be cleared",
                    options=company_labels,
                    key="cleanup_company_select",
                )

                selected_company_ids = [
                    label_to_id[label] for label in selected_company_labels
                ]

                confirm_single = st.checkbox(
                    "I understand this will permanently delete stored metric data for the selected companies.",
                    key="confirm_cleanup_single",
                )

                if st.button(
                    "Delete data for selected companies",
                    type="primary",
                    disabled=(len(selected_company_ids) == 0 or not confirm_single),
                    key="btn_cleanup_single",
                ):
                    delete_company_metric_data(conn, selected_company_ids)
                    st.success(
                        f"Cleared stored metric data for {len(selected_company_ids)} company(ies) "
                        f"across all metric tables."
                    )

    # -----------------------------
    # Formula sub-tab
    # -----------------------------
    with tab_formula:
        st.subheader("Formula Reference")
        st.caption(
            "These formulas match how the application computes the score columns in the "
            "P&L Metrics, Balance Sheet Metrics, and Combined Dashboard tabs."
        )

        st.markdown(
            """
### P&L (Profit and Loss) Score

**a) Weighted P&L (Profit and Loss) Growth Score**

Let **WeightedAvg(pairs)** be:

> WeightedAvg = Σ(valueᵢ × weightᵢ) / Σ(weightᵢ), using only rows where valueᵢ is present and weightᵢ > 0.

Then:

- **Weighted P&L Growth Score** = WeightedAvg of these (all in % units over the selected year-range):
  - Median Revenue Growth (%)
  - Median Pretax Income Growth (%) (Pretax = Profit Before Tax)
  - Median Net Income Growth (%)
  - Median NOPAT (Net Operating Profit After Tax) Growth (%)
  - Median Operating Margin (%)
  - Median YoY (Year-over-Year) Operating Margin Growth (%)

**b) Weighted P&L (Profit and Loss) Standard Deviation Score**

- **Weighted P&L Standard Deviation Score** = WeightedAvg of these (all in % units over the selected year-range):
  - Revenue Growth Standard Deviation
  - Pretax Income Growth Standard Deviation
  - Net Income Growth Standard Deviation
  - NOPAT (Net Operating Profit After Tax) Growth Standard Deviation
  - Operating Margin Standard Deviation
  - YoY (Year-over-Year) Operating Margin Growth Standard Deviation

**c) Additive Volatility-Adjusted P&L (Profit and Loss) Growth Score**

- **Additive Volatility-Adjusted P&L Growth Score** = (Weighted P&L Growth Score) − (Weighted P&L Standard Deviation Score)

**d) Scaled Volatility-Adjusted P&L (Profit and Loss) Growth Score**

- **Scaled Volatility-Adjusted P&L Growth Score** = (Weighted P&L Growth Score) ÷ (1 + Weighted P&L Standard Deviation Score)


### Balance Sheet Score

**e) Weighted Balance Sheet Strength Score**

- **Weighted Balance Sheet Strength Score** = WeightedAvg of these (all in % units over the selected year-range):
  - Median Accumulated Equity Growth (%)
  - Median Return on Equity (ROE) Growth (%)
  - Median Return on Capital Employed (ROCE) (%)

**f) Weighted Balance Sheet Standard Deviation Score**

- **Weighted Balance Sheet Standard Deviation Score** = WeightedAvg of these (all in % units over the selected year-range):
  - Accumulated Equity Growth Standard Deviation
  - Return on Equity (ROE) Standard Deviation
  - Return on Capital Employed (ROCE) Standard Deviation

**g) Additive Volatility-Adjusted Balance Sheet Strength Score**

- **Additive Volatility-Adjusted Balance Sheet Strength Score** = (Weighted Balance Sheet Strength Score) − (Weighted Balance Sheet Standard Deviation Score)

**h) Scaled Volatility-Adjusted Balance Sheet Strength Score**

- **Scaled Volatility-Adjusted Balance Sheet Strength Score** = (Weighted Balance Sheet Strength Score) ÷ (1 + Weighted Balance Sheet Standard Deviation Score)

**i) Debt-Adjusted Balance Sheet Strength Score**

- **Debt-Adjusted Balance Sheet Strength Score** = (Scaled Volatility-Adjusted Balance Sheet Strength Score) ÷ (1 + Median Interest Load % ÷ 100)

> Median Interest Load % is the median Interest Load percentage over the selected year-range (higher debt load reduces this score).


### Combined / Overall Scores

**j) Total Additive Volatility-Adjusted Score**

- **Total Additive Volatility-Adjusted Score** = (Additive Volatility-Adjusted Balance Sheet Strength Score) + (Additive Volatility-Adjusted P&L Growth Score)

**k) Total Scaled Volatility-Adjusted Score**

- **Total Scaled Volatility-Adjusted Score** = (Scaled Volatility-Adjusted Balance Sheet Strength Score) + (Scaled Volatility-Adjusted P&L Growth Score)

**l) Total Debt-Adjusted Scaled Volatility-Adjusted Score**

- **Total Debt-Adjusted Scaled Volatility-Adjusted Score** = (Debt-Adjusted Balance Sheet Strength Score) + (Scaled Volatility-Adjusted P&L Growth Score)
"""
        )
