import base64
import html
from pathlib import Path
from urllib.parse import quote

import streamlit as st
import streamlit.components.v1 as components

from admin import render_admin_tab
from bs_metrics import render_balance_sheet_metrics_tab
from cap_structure_cost import render_capital_structure_cost_of_capital_tab
from cash_flow_spread import render_cash_flow_and_spread_tab
from combined_dashboard import render_combined_dashboard_tab
from data_upload import render_data_upload_tab
from pl_metrics import render_pl_metrics_tab


# --- Branding assets (kept local to the repo) ---
_ASSETS_DIR = Path(__file__).parent / "assets"
_LOGO_PATH = _ASSETS_DIR / "tarasha_logo.png"
_HERO_BANNER_PATH = _ASSETS_DIR / "Hero_Banner.png"

# Articles live under ./assets/article/ (preferred) or ./assets/articles/ (fallback)
_ARTICLES_DIR_CANDIDATES = [
    _ASSETS_DIR / "article",
    _ASSETS_DIR / "articles",
]

# Streamlit requires set_page_config to be the first Streamlit command in the app.
st.set_page_config(
    page_title="TaRaSha Equity Research Tool",
    layout="wide",
    initial_sidebar_state="collapsed",
)


@st.cache_data(show_spinner=False)
def _bytes_to_b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")


@st.cache_data(show_spinner=False)
def _read_bytes(path: str) -> bytes:
    return Path(path).read_bytes()


@st.cache_data(show_spinner=False)
def _img_to_b64(path: str) -> str:
    return _bytes_to_b64(_read_bytes(path))


def _inject_shell_css() -> None:
    # Keep this minimal: we don't want to unintentionally change global styling.
    st.markdown(
        """
<style>
/* Featured Articles */
.tarasha-articles-strip{
  display:flex;
  gap:16px;
  overflow-x:auto;
  padding:6px 2px 14px 2px;
  scroll-snap-type:x mandatory;
}
.tarasha-article-card{
  flex:0 0 260px;
  scroll-snap-align:start;
  text-decoration:none;
  color:inherit;
  border:1px solid rgba(120,120,120,0.25);
  border-radius:14px;
  overflow:hidden;
  background: rgba(255,255,255,0.02);
}
.tarasha-article-thumb{
  width:100%;
  height:150px;
  object-fit:cover;
  display:block;
}
.tarasha-article-meta{
  padding:10px 12px 12px 12px;
}
.tarasha-article-title{
  font-weight:650;
  font-size:0.95rem;
  line-height:1.25rem;
  margin:0;
}
.tarasha-article-sub{
  margin:6px 0 0 0;
  font-size:0.8rem;
  opacity:0.75;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def _get_query_param(name: str) -> str | None:
    """
    Works across Streamlit versions:
      - newer: st.query_params
      - older: st.experimental_get_query_params
    """
    try:
        qp = st.query_params  # type: ignore[attr-defined]
        val = qp.get(name)
        if isinstance(val, list):
            return val[0] if val else None
        return val
    except Exception:
        qp = st.experimental_get_query_params()  # type: ignore[attr-defined]
        vals = qp.get(name, [])
        return vals[0] if vals else None


def _articles_dir() -> Path | None:
    for p in _ARTICLES_DIR_CANDIDATES:
        if p.exists() and p.is_dir():
            return p
    return None


def _guess_title_from_slug(slug: str) -> str:
    # human-ish title from file stem
    t = slug.replace("_", " ").replace("-", " ").strip()
    return " ".join(w.capitalize() if w else "" for w in t.split())


def _find_thumbnail_for_pdf(pdf_path: Path) -> Path | None:
    """
    Naming convention (supported):
      Preferred:
        <slug>.pdf
        <slug>.(png|jpg|jpeg|webp)
      Also accepted:
        <slug>_thumb.(...)
        <slug>_thumbnail.(...)
        <slug>-thumb.(...)
        thumb_<slug>.(...)
        thumbnail_<slug>.(...)
    """
    stem = pdf_path.stem
    exts = [".png", ".jpg", ".jpeg", ".webp"]

    candidates = []
    candidates += [pdf_path.with_suffix(ext) for ext in exts]
    candidates += [pdf_path.with_name(f"{stem}_thumb{ext}") for ext in exts]
    candidates += [pdf_path.with_name(f"{stem}_thumbnail{ext}") for ext in exts]
    candidates += [pdf_path.with_name(f"{stem}-thumb{ext}") for ext in exts]
    candidates += [pdf_path.with_name(f"thumb_{stem}{ext}") for ext in exts]
    candidates += [pdf_path.with_name(f"thumbnail_{stem}{ext}") for ext in exts]

    for c in candidates:
        if c.exists() and c.is_file():
            return c

    return None


def _discover_articles() -> list[dict]:
    """
    Returns list of:
      { slug, title, pdf_path, thumb_path, mtime }
    """
    adir = _articles_dir()
    if not adir:
        return []

    pdfs = sorted(adir.glob("*.pdf"), key=lambda p: p.stat().st_mtime, reverse=True)

    articles: list[dict] = []
    for pdf in pdfs:
        slug = pdf.stem
        title = _guess_title_from_slug(slug)
        thumb = _find_thumbnail_for_pdf(pdf)

        # Fallback thumbnail: logo if present, else nothing
        if thumb is None and _LOGO_PATH.exists():
            thumb = _LOGO_PATH

        articles.append(
            {
                "slug": slug,
                "title": title,
                "pdf_path": str(pdf),
                "thumb_path": str(thumb) if thumb else None,
                "mtime": pdf.stat().st_mtime,
            }
        )

    return articles


def _render_article_viewer() -> None:
    """
    Renders a dedicated article view when opened in a new tab via ?article=<slug>.
    """
    slug = _get_query_param("article")
    if not slug:
        return

    adir = _articles_dir()
    if not adir:
        st.error("No articles folder found. Expected ./assets/article/ or ./assets/articles/.")
        st.stop()

    pdf_path = (adir / f"{slug}.pdf").resolve()
    if not pdf_path.exists():
        st.error(f"Article not found: {pdf_path.name}")
        st.stop()

    title = _guess_title_from_slug(slug)

    st.title(title)
    st.caption(f"Source: {pdf_path.relative_to(Path(__file__).parent)}")

    pdf_bytes = _read_bytes(str(pdf_path))
    st.download_button(
        "Download PDF",
        data=pdf_bytes,
        file_name=pdf_path.name,
        mime="application/pdf",
        use_container_width=False,
    )

    pdf_b64 = _bytes_to_b64(pdf_bytes)
    # Embed in an iframe so the browser PDF viewer can handle it.
    components.html(
        f"""
        <iframe
            src="data:application/pdf;base64,{pdf_b64}"
            width="100%"
            height="900"
            style="border:none; border-radius: 10px;"
        ></iframe>
        """,
        height=930,
        scrolling=True,
    )

    # Stop here so we don't render the rest of the app in this tab.
    st.stop()


def _render_header() -> tuple:
    _inject_shell_css()

    # Keep header lightweight; the hero banner is the main "branding" element on Home.
    tab_home, tab_equity_research = st.tabs(["Home", "Equity Research"])
    return tab_home, tab_equity_research


def _render_hero_carousel(image_paths) -> None:
    """
    Render a responsive hero image carousel.
    Notes:
      - Designed as a carousel so you can add more hero images later.
      - Leaves a clean overlay layer available for future CTA (Call To Action) buttons.
    """
    max_width_px = 1250  # keeps the banner from feeling huge on ultrawides
    banner_ratio_w = 1802
    banner_ratio_h = 601
    hero_iframe_height = int((max_width_px * banner_ratio_h / banner_ratio_w) + 95)

    valid_paths = [Path(p) for p in image_paths if Path(p).exists()]
    if not valid_paths:
        st.warning("Hero banner image not found. Put 'Hero_Banner.png' under ./assets/.")
        return

    imgs_b64 = [_img_to_b64(str(p)) for p in valid_paths]
    slides_html = "".join(
        [
            f"""
            <div class="slide">
              <img src="data:image/png;base64,{b64}" />
            </div>
            """
            for b64 in imgs_b64
        ]
    )

    show_controls = "block" if len(imgs_b64) > 1 else "none"

    html_block = f"""
    <style>
      .hero-wrap {{
        max-width: {max_width_px}px;
        margin: 0 auto;
        position: relative;
      }}
      .hero {{
        border-radius: 18px;
        overflow: hidden;
        border: 1px solid rgba(120,120,120,0.25);
      }}
      .slide img {{
        width: 100%;
        height: auto;
        display: block;
      }}
      .controls {{
        position: absolute;
        inset: 0;
        display: {show_controls};
        pointer-events: none;
      }}
      .btn {{
        position: absolute;
        top: 50%;
        transform: translateY(-50%);
        pointer-events: auto;
        border: none;
        border-radius: 999px;
        width: 34px;
        height: 34px;
        cursor: pointer;
        opacity: 0.85;
      }}
      .btn.left {{ left: 10px; }}
      .btn.right {{ right: 10px; }}
    </style>

    <div class="hero-wrap">
      <div class="hero" id="hero">
        {slides_html}
      </div>

      <div class="controls">
        <button class="btn left" onclick="prev(event)">‹</button>
        <button class="btn right" onclick="next(event)">›</button>
      </div>
    </div>

    <script>
      const hero = document.getElementById("hero");
      const slides = Array.from(hero.querySelectorAll(".slide"));
      let idx = 0;
      function render() {{
        slides.forEach((s, i) => s.style.display = (i === idx ? "block" : "none"));
      }}
      function prev(e) {{ e.preventDefault(); idx = (idx - 1 + slides.length) % slides.length; render(); }}
      function next(e) {{ e.preventDefault(); idx = (idx + 1) % slides.length; render(); }}
      render();
    </script>
    """

    components.html(html_block, height=hero_iframe_height, scrolling=False)


def _render_featured_articles() -> None:
    st.markdown("### Featured Articles")

    articles = _discover_articles()
    if not articles:
        adir = _articles_dir()
        if adir is None:
            st.caption("Put your PDFs under `./assets/article/` (or `./assets/articles/`).")
        else:
            st.caption(f"No PDFs found under `{adir}`.")
        st.caption("Thumbnail naming: `<slug>.png` (same stem as the PDF) is the preferred convention.")
        return

    cards: list[str] = []
    for a in articles:
        slug = a["slug"]
        title = html.escape(a["title"])
        href = f"?article={quote(slug)}"

        thumb_path = a.get("thumb_path")
        if thumb_path:
            # Pick image type from extension for better browser decoding
            ext = Path(thumb_path).suffix.lower().lstrip(".") or "png"
            thumb_b64 = _img_to_b64(thumb_path)
            img_html = f'<img class="tarasha-article-thumb" src="data:image/{ext};base64,{thumb_b64}" />'
        else:
            img_html = '<div style="height:150px;"></div>'

        cards.append(
            f"""
            <a class="tarasha-article-card" href="{href}" target="_blank" rel="noopener noreferrer">
              {img_html}
              <div class="tarasha-article-meta">
                <p class="tarasha-article-title">{title}</p>
                <p class="tarasha-article-sub">Open PDF in new tab</p>
              </div>
            </a>
            """
        )

    st.markdown(f'<div class="tarasha-articles-strip">{"".join(cards)}</div>', unsafe_allow_html=True)


def _render_home_body() -> None:
    # Hero banner carousel (add more images here later)
    hero_images = [_HERO_BANNER_PATH]

    # Fallback: if the image isn't under assets yet, allow running from repo root.
    if not _HERO_BANNER_PATH.exists():
        alt = Path(__file__).parent / "Hero_Banner.png"
        if alt.exists():
            hero_images = [alt]

    _render_hero_carousel(hero_images)

    # Featured Articles (auto-discovered from assets/article)
    _render_featured_articles()


def _render_equity_research_body() -> None:
    tab_upload, tab_pl, tab_bs, tab_cs, tab_cf, tab_combined, tab_admin = st.tabs(
        [
            "Data Upload",
            "P&L Metrics and Dashboard",
            "Balance Sheet Metrics and Dashboard",
            "Capital Structure & Spread",
            "Cash Flow and Reinvestment",
            "Combined Dashboard",
            "Admin",
        ]
    )

    with tab_upload:
        render_data_upload_tab()
    with tab_pl:
        render_pl_metrics_tab()
    with tab_bs:
        render_balance_sheet_metrics_tab()
    with tab_cs:
        render_capital_structure_cost_of_capital_tab()
    with tab_cf:
        render_cash_flow_and_spread_tab()
    with tab_combined:
        render_combined_dashboard_tab()
    with tab_admin:
        render_admin_tab()


def _render_footer() -> None:
    # Intentionally empty for now (layout placeholder).
    return


# --- Router: if opened as ?article=<slug> in a new tab, show the PDF viewer only ---
_render_article_viewer()

tab_home, tab_equity_research = _render_header()

with tab_home:
    _render_home_body()

with tab_equity_research:
    _render_equity_research_body()

_render_footer()
