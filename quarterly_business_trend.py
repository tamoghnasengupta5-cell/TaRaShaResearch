from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st

from core import *  # noqa: F401,F403
from ui_lazy_tabs import lazy_tab_bar
from ui_theme import company_label_map, render_dashboard_table


_PERCENT_DETAIL_COLUMNS = [
    "Weighted Median Revenue Growth",
    "Weighted Median Operating Margin",
    "Weighted Median Operating Margin Change",
    "Weighted Median Incremental Operating Margin",
    "Weighted Median Capex / OCF",
]


def _get_selected_company_ids(conn, companies_df: pd.DataFrame) -> List[int]:
    mode = st.radio(
        "Analyze by",
        ["Company", "Industry Bucket", "Category / Sub-Category"],
        horizontal=True,
        key="quarterly_business_trend_mode",
    )

    if mode == "Company":
        labels = company_label_map(companies_df)
        selected = st.multiselect(
            "Select one or more companies",
            options=list(labels.keys()),
            format_func=lambda company_id: labels.get(company_id, str(company_id)),
            key="quarterly_business_trend_companies",
        )
        return sorted({int(company_id) for company_id in selected})

    if mode == "Category / Sub-Category":
        categories_df = read_df(
            """
            SELECT
                c.name AS master_category,
                s.id AS subcategory_id,
                s.name AS subcategory
            FROM relative_valuation_categories c
            JOIN relative_valuation_subcategories s
                ON s.category_id = c.id
            ORDER BY c.name, s.name
            """,
            conn,
        )
        if categories_df is None or categories_df.empty:
            st.info("No categories found yet.")
            return []

        master_categories = sorted(categories_df["master_category"].dropna().astype(str).unique().tolist())
        selected_master = st.selectbox(
            "Select category",
            options=master_categories,
            key="quarterly_business_trend_category",
        )
        subcategory_rows = categories_df[categories_df["master_category"] == selected_master].copy()
        subcategory_id_to_name = {
            int(row["subcategory_id"]): str(row["subcategory"])
            for _, row in subcategory_rows.iterrows()
        }
        selected_subcategory_ids = st.multiselect(
            "Select one or more sub-categories",
            options=list(subcategory_id_to_name.keys()),
            format_func=lambda subcategory_id: subcategory_id_to_name.get(int(subcategory_id), str(subcategory_id)),
            key="quarterly_business_trend_subcategories",
        )
        if not selected_subcategory_ids:
            return []

        subcategory_ids = sorted({int(subcategory_id) for subcategory_id in selected_subcategory_ids})
        placeholders = ",".join(["?"] * len(subcategory_ids))
        members_df = read_df(
            f"""
            SELECT DISTINCT company_id
            FROM relative_valuation_company_assignments
            WHERE subcategory_id IN ({placeholders})
            """,
            conn,
            params=subcategory_ids,
        )
        if members_df is None or members_df.empty:
            return []
        return sorted({int(company_id) for company_id in members_df["company_id"].tolist()})

    groups_df = read_df(
        "SELECT id, name FROM company_groups ORDER BY name",
        conn,
    )
    if groups_df.empty:
        st.info("No industry buckets found yet.")
        return []

    group_name_to_id = {str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()}
    selected_buckets = st.multiselect(
        "Select one or more industry buckets",
        options=list(group_name_to_id.keys()),
        key="quarterly_business_trend_buckets",
    )
    if not selected_buckets:
        return []

    group_ids = [group_name_to_id[name] for name in selected_buckets if name in group_name_to_id]
    if not group_ids:
        return []

    placeholders = ",".join(["?"] * len(group_ids))
    members_df = read_df(
        f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
        conn,
        params=group_ids,
    )
    if members_df is None or members_df.empty:
        return []
    return sorted({int(company_id) for company_id in members_df["company_id"].tolist()})


def _get_company_buckets(conn, company_ids: List[int]) -> Dict[int, str]:
    if not company_ids:
        return {}
    placeholders = ",".join(["?"] * len(company_ids))
    bucket_df = read_df(
        f"""
        SELECT m.company_id, g.name
        FROM company_group_members m
        JOIN company_groups g ON g.id = m.group_id
        WHERE m.company_id IN ({placeholders})
        ORDER BY g.name
        """,
        conn,
        params=company_ids,
    )
    if bucket_df is None or bucket_df.empty:
        return {}

    bucket_names_by_company: Dict[int, List[str]] = {}
    for _, row in bucket_df.iterrows():
        company_id = int(row["company_id"])
        bucket_names_by_company.setdefault(company_id, []).append(str(row["name"]))
    return {
        company_id: ", ".join(sorted(set(bucket_names)))
        for company_id, bucket_names in bucket_names_by_company.items()
    }


def _quarter_range_input() -> int:
    quarter_range = int(
        st.number_input(
            "Quarter Range for Score Calculation",
            min_value=8,
            value=16,
            step=4,
            key="quarterly_business_trend_quarter_range",
        )
    )
    if quarter_range <= 4 or quarter_range % 4 != 0:
        st.error("Quarter range must be greater than 4 and a multiple of 4.")
        st.stop()
    return quarter_range


def _build_dashboard_rows(
    conn,
    companies_df: pd.DataFrame,
    company_ids: List[int],
    quarter_range: int,
    component_weights: Dict[str, float],
) -> pd.DataFrame:
    company_lookup = {int(row["id"]): row for _, row in companies_df.iterrows()}
    bucket_map = _get_company_buckets(conn, company_ids)

    rows: List[Dict[str, object]] = []
    progress = st.progress(0, text="Computing Quarterly Business Trend scores...")
    total = max(len(company_ids), 1)

    for idx, company_id in enumerate(company_ids, start=1):
        row = company_lookup.get(company_id)
        if row is None:
            continue

        details = calculate_business_quarter_trend_details(
            get_quarterly_business_trend_inputs(conn, company_id),
            quarter_range=quarter_range,
            component_weights=component_weights,
        )

        rows.append(
            {
                "Company": row["name"],
                "Ticker": row["ticker"],
                "Industry Bucket": bucket_map.get(company_id, ""),
                "Business Quarter Trend Score": details["business_quarter_trend_score"],
                "Weighted Median Revenue Growth": _pct(details["weighted_median_revenue_growth"]),
                "Weighted Median Operating Margin": _pct(details["weighted_median_operating_margin"]),
                "Weighted Median Operating Margin Change": _pct(details["weighted_median_operating_margin_change"]),
                "Weighted Median Incremental Operating Margin": _pct(details["weighted_median_incremental_operating_margin"]),
                "Weighted Median Bill-to-Revenue": details["weighted_median_bill_to_revenue"],
                "Weighted Median Days Sales Outstanding": details["weighted_median_days_sales_outstanding"],
                "Weighted Median Capex / OCF": _pct(details["weighted_median_capex_to_ocf"]),
            }
        )
        progress.progress(idx / total, text=f"Computed {idx} of {total} companies")

    progress.empty()
    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    return out.sort_values(
        by="Business Quarter Trend Score",
        ascending=False,
        na_position="last",
    ).reset_index(drop=True)


def _pct(value):
    return None if value is None else float(value) * 100.0


def render_quarterly_business_trend_dashboard_tab() -> None:
    conn = get_db()
    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies in the database yet. Upload a spreadsheet under Equity Research -> Data Upload.")
        return

    quarter_range = _quarter_range_input()
    company_ids = _get_selected_company_ids(conn, companies_df)
    if not company_ids:
        st.info("Select at least one company or industry bucket to compute scores.")
        return

    compute = st.button("Compute Score", type="primary", key="quarterly_business_trend_compute")
    if compute:
        st.session_state["quarterly_business_trend_has_run"] = True
    if not st.session_state.get("quarterly_business_trend_has_run", False):
        return

    component_weights = get_business_quarter_trend_weight_map(conn)
    dashboard_df = _build_dashboard_rows(
        conn,
        companies_df,
        sorted(set(company_ids)),
        quarter_range,
        component_weights,
    )
    if dashboard_df.empty:
        st.info("No quarterly trend data is available for the selected companies.")
        return

    column_config = {
        "Business Quarter Trend Score": st.column_config.NumberColumn(format="%.1f"),
        "Weighted Median Revenue Growth": st.column_config.NumberColumn(format="%.2f%%"),
        "Weighted Median Operating Margin": st.column_config.NumberColumn(format="%.2f%%"),
        "Weighted Median Operating Margin Change": st.column_config.NumberColumn(format="%.2f%%"),
        "Weighted Median Incremental Operating Margin": st.column_config.NumberColumn(format="%.2f%%"),
        "Weighted Median Bill-to-Revenue": st.column_config.NumberColumn(format="%.2fx"),
        "Weighted Median Days Sales Outstanding": st.column_config.NumberColumn(format="%.1f days"),
        "Weighted Median Capex / OCF": st.column_config.NumberColumn(format="%.2f%%"),
    }
    render_dashboard_table(
        dashboard_df,
        column_config=column_config,
        key="quarterly_business_trend_dashboard",
    )


def render_quarterly_business_trend_formula_tab() -> None:
    conn = get_db()
    weights_df = get_business_quarter_trend_weights(conn)

    st.subheader("Formula for Business Quarter Trend Score")
    st.markdown(
        """
        The model converts quarterly operating metrics into component scores, then combines them using the saved parameter weights.

        `Business Quarter Trend Score = ROUND(SUMPRODUCT(parameter_weight, component_score) / 100, 1)`

        Component score formulas:
        - `Revenue Growth Score = CLAMP((Weighted Median Revenue Growth + 10%) / 50% * 100)`
        - `Operating Margin Score = CLAMP(Weighted Median Operating Margin / 50% * 100)`
        - `Operating Margin Change Score = CLAMP((Weighted Median Operating Margin Change + 5%) / 10% * 100)`
        - `Incremental Operating Margin Score = CLAMP(Weighted Median Incremental Operating Margin / 70% * 100)`
        - `Bill-to-Revenue Score = CLAMP((Weighted Median Bill-to-Revenue - 0.90) / 0.30 * 100)`
        - `Days Sales Outstanding Score = CLAMP((90 - Weighted Median Days Sales Outstanding) / 60 * 100)`
        - `Capex / OCF Score = CLAMP((30% - Weighted Median Capex / OCF) / 30% * 100)`

        `CLAMP(x)` means `MAX(0, MIN(100, x))`.
        """
    )

    st.subheader("Weights of Parameters Used to Calculate the Business Quarter Trend Score")
    editable_df = weights_df[["parameter_key", "parameter", "weight", "sort_order"]].copy()
    edited_df = st.data_editor(
        editable_df,
        hide_index=True,
        use_container_width=True,
        disabled=["parameter_key", "parameter", "sort_order"],
        column_config={
            "parameter_key": st.column_config.TextColumn("Parameter Key"),
            "parameter": st.column_config.TextColumn("Parameter"),
            "weight": st.column_config.NumberColumn("Weight", min_value=0.0, step=0.5, format="%.2f"),
            "sort_order": st.column_config.NumberColumn("Sort Order"),
        },
        key="quarterly_business_trend_weights_editor",
    )

    weight_total = float(pd.to_numeric(edited_df["weight"], errors="coerce").fillna(0.0).sum())
    st.caption(f"Total weight: {weight_total:.2f}. Total must equal 100.00.")

    if st.button("Save weights", type="primary", key="quarterly_business_trend_save_weights"):
        if abs(weight_total - 100.0) > 1e-6:
            st.error(f"Total weight must equal 100.00. Current total: {weight_total:.2f}.")
            return

        weights = {
            str(row["parameter_key"]): float(row["weight"])
            for _, row in edited_df.iterrows()
        }
        try:
            update_business_quarter_trend_weights(conn, weights)
        except Exception as exc:
            st.error(f"Save failed: {exc}")
        else:
            st.success("Saved.")


def _category_editor_frame(categories_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["Master Category", "Sub-Category", "Assigned Companies"]
    if categories_df is None or categories_df.empty:
        return pd.DataFrame(columns=columns)

    out = categories_df[["master_category", "subcategory", "assigned_companies"]].copy()
    out = out.rename(
        columns={
            "master_category": "Master Category",
            "subcategory": "Sub-Category",
            "assigned_companies": "Assigned Companies",
        }
    )
    out = out[out["Sub-Category"].notna()].copy()
    if out.empty:
        return pd.DataFrame(columns=columns)
    out["Assigned Companies"] = pd.to_numeric(out["Assigned Companies"], errors="coerce").fillna(0).astype(int)
    return out[columns].reset_index(drop=True)


def render_quarterly_business_trend_master_category_tab() -> None:
    conn = get_db()
    categories_df = get_relative_valuation_categories(conn)
    editor_df = _category_editor_frame(categories_df)

    edited_df = st.data_editor(
        editor_df,
        hide_index=True,
        use_container_width=True,
        num_rows="dynamic",
        disabled=["Assigned Companies"],
        column_config={
            "Master Category": st.column_config.TextColumn("Master Category", required=True),
            "Sub-Category": st.column_config.TextColumn("Sub-Category", required=True),
            "Assigned Companies": st.column_config.NumberColumn("Assigned Companies", disabled=True),
        },
        key="relative_valuation_categories_editor",
    )

    if st.button("Save categories", type="primary", key="relative_valuation_categories_save"):
        rows = edited_df.where(pd.notna(edited_df), "").to_dict("records")
        saved_count = replace_relative_valuation_categories(conn, rows)
        st.success(f"Saved {saved_count} sub-categories.")


def _subcategory_options(categories_df: pd.DataFrame, master_category: str) -> Dict[int, str]:
    if categories_df is None or categories_df.empty:
        return {}
    subset = categories_df[
        (categories_df["master_category"] == master_category)
        & categories_df["subcategory_id"].notna()
    ]
    return {
        int(row["subcategory_id"]): str(row["subcategory"])
        for _, row in subset.sort_values("subcategory").iterrows()
    }


def _assignment_label(row: pd.Series) -> str:
    company = str(row.get("company", "") or "").strip()
    ticker = str(row.get("ticker", "") or "").strip()
    master = str(row.get("master_category", "") or "").strip()
    subcategory = str(row.get("subcategory", "") or "").strip()
    company_label = f"{company} {ticker}".strip()
    return f"{company_label} | {master} / {subcategory}"


def render_quarterly_business_trend_company_assignment_tab() -> None:
    conn = get_db()
    companies_df = list_companies(conn)
    categories_df = get_relative_valuation_categories(conn)
    subcategory_df = (
        categories_df[categories_df["subcategory_id"].notna()].copy()
        if categories_df is not None and not categories_df.empty
        else pd.DataFrame()
    )

    if companies_df.empty:
        st.info("No companies in the database yet.")
        return
    if subcategory_df.empty:
        st.info("Define at least one master category and sub-category first.")
        return

    master_categories = sorted(subcategory_df["master_category"].dropna().astype(str).unique().tolist())
    company_labels = company_label_map(companies_df)

    with st.form("relative_valuation_assignment_form"):
        selected_master = st.selectbox(
            "Master Category",
            options=master_categories,
            key="relative_valuation_assignment_master",
        )
        subcategory_options = _subcategory_options(subcategory_df, selected_master)
        selected_subcategory_id = st.selectbox(
            "Sub-Category",
            options=list(subcategory_options.keys()),
            format_func=lambda subcategory_id: subcategory_options.get(int(subcategory_id), str(subcategory_id)),
            key="relative_valuation_assignment_subcategory",
        )
        selected_company_ids = st.multiselect(
            "Companies",
            options=list(company_labels.keys()),
            format_func=lambda company_id: company_labels.get(int(company_id), str(company_id)),
            key="relative_valuation_assignment_companies",
        )
        submitted = st.form_submit_button("Assign companies", type="primary")

    if submitted:
        if not selected_company_ids:
            st.error("Select at least one company.")
        else:
            assigned_count = add_relative_valuation_company_assignments(
                conn,
                [int(company_id) for company_id in selected_company_ids],
                int(selected_subcategory_id),
            )
            st.success(f"Processed {assigned_count} company assignments.")

    assignments_df = get_relative_valuation_company_assignments(conn)
    if assignments_df.empty:
        st.info("No company assignments saved yet.")
        return

    display_df = assignments_df[["company", "ticker", "master_category", "subcategory"]].rename(
        columns={
            "company": "Company",
            "ticker": "Ticker",
            "master_category": "Master Category",
            "subcategory": "Sub-Category",
        }
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    assignment_options: List[Tuple[int, int]] = []
    assignment_labels: Dict[Tuple[int, int], str] = {}
    for _, row in assignments_df.iterrows():
        key = (int(row["company_id"]), int(row["subcategory_id"]))
        assignment_options.append(key)
        assignment_labels[key] = _assignment_label(row)

    selected_assignments = st.multiselect(
        "Remove assignments",
        options=assignment_options,
        format_func=lambda key: assignment_labels.get(key, str(key)),
        key="relative_valuation_remove_assignments",
    )
    if st.button("Remove selected assignments", key="relative_valuation_remove_selected"):
        removed_count = remove_relative_valuation_company_assignments(conn, selected_assignments)
        st.success(f"Removed {removed_count} assignments.")
        st.rerun()


def render_quarterly_business_trend_categories_tab() -> None:
    active_tab = lazy_tab_bar(
        ["Master Category", "Company Assignment"],
        key="quarterly_business_trend_categories_tabs",
        default="Master Category",
    )
    if active_tab == "Master Category":
        render_quarterly_business_trend_master_category_tab()
    elif active_tab == "Company Assignment":
        render_quarterly_business_trend_company_assignment_tab()


def render_quarterly_business_trend_score_tab() -> None:
    st.title("Quarterly Business Trend Score")
    active_tab = lazy_tab_bar(
        ["Dashboard", "Formula", "Categories"],
        key="quarterly_business_trend_tabs",
        default="Dashboard",
    )
    if active_tab == "Dashboard":
        render_quarterly_business_trend_dashboard_tab()
    elif active_tab == "Formula":
        render_quarterly_business_trend_formula_tab()
    elif active_tab == "Categories":
        render_quarterly_business_trend_categories_tab()
