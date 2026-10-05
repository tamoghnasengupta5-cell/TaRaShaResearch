"""Shared Category / Sub-Category selector for score dashboards."""

from collections import Counter

import streamlit as st

from core_backend import read_df


def _category_subcategory_options(categories_df, selected_categories):
    """Return synthetic all-options and the real IDs available under the selection."""
    selected = set(selected_categories)
    rows = categories_df[
        categories_df["master_category"].isin(selected)
        & categories_df["subcategory_id"].notna()
    ]
    by_category = {category: set() for category in selected_categories}
    names = {}
    for _, row in rows.iterrows():
        category = str(row["master_category"])
        subcategory_id = int(row["subcategory_id"])
        by_category[category].add(subcategory_id)
        names[subcategory_id] = (category, str(row["subcategory"]))

    all_options = [f"All - {category}" for category in selected_categories]
    name_counts = Counter(name for _, name in names.values())
    labels = {option: option for option in all_options}
    for subcategory_id, (category, name) in names.items():
        labels[subcategory_id] = (
            f"{category} / {name}" if name_counts[name] > 1 else name
        )
    individual_options = sorted(
        names, key=lambda subcategory_id: (*names[subcategory_id], subcategory_id)
    )
    return all_options + individual_options, labels, by_category


def resolve_subcategory_ids(categories_df, selected_categories, selected_options):
    """Expand selected All options into real subcategory IDs without duplicates."""
    options, _, by_category = _category_subcategory_options(
        categories_df, selected_categories
    )
    allowed = set(options)
    resolved = set()
    for option in selected_options:
        if option not in allowed:
            continue
        if isinstance(option, str):
            resolved.update(by_category.get(option.removeprefix("All - "), ()))
        else:
            resolved.add(int(option))
    return sorted(resolved)


def select_category_company_ids(
    conn, *, category_key, subcategory_key, subcategory_label
):
    """Return distinct company IDs; None means no taxonomy is configured."""
    categories_df = read_df(
        """
        SELECT c.name AS master_category, s.id AS subcategory_id,
               s.name AS subcategory
        FROM relative_valuation_categories c
        JOIN relative_valuation_subcategories s ON s.category_id = c.id
        ORDER BY c.name, s.name
        """,
        conn,
    )
    if categories_df is None or categories_df.empty:
        st.info("No categories found yet.")
        return None

    categories = sorted(
        categories_df["master_category"].dropna().astype(str).unique().tolist()
    )
    selected_categories = st.multiselect(
        "Select one or more categories", options=categories, key=category_key
    )
    options, labels, _ = _category_subcategory_options(
        categories_df, selected_categories
    )

    # Streamlit recreates a widget when its options change. Keep the still-valid
    # choices as its default, without also writing the widget's session state.
    previous = st.session_state.get(subcategory_key, [])
    valid_previous = [option for option in previous if option in options]

    selected_options = st.multiselect(
        subcategory_label,
        options=options,
        default=valid_previous,
        format_func=lambda option: labels[option],
        key=subcategory_key,
    )
    subcategory_ids = resolve_subcategory_ids(
        categories_df, selected_categories, selected_options
    )
    if not subcategory_ids:
        return []

    placeholders = ",".join("?" for _ in subcategory_ids)
    members_df = read_df(
        """
        SELECT DISTINCT company_id
        FROM relative_valuation_company_assignments
        WHERE subcategory_id IN (""" + placeholders + ")",
        conn,
        params=subcategory_ids,
    )
    if members_df is None or members_df.empty:
        return []
    return sorted({int(company_id) for company_id in members_df["company_id"]})
