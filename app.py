from __future__ import annotations

import base64
from pathlib import Path
from typing import Dict, List, Optional

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

# Articles (Portable Document Format files)
_ARTICLES_DIR = _ASSETS_DIR / "articles"


# Streamlit requires set_page_config to be the first Streamlit command in the app.
st.set_page_config(
    page_title="TaRaSha Equity Research Tool",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# -----------------------------
# Small helpers
# -----------------------------
@st.cache_data(show_spinner=False)
def _read_bytes(path: str) -> bytes:
    return Path(path).read_bytes()


@st.cache_data(show_spinner=False)
def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")


@st.cache_data(show_spinner=False)
def _img_to_b64(path: str) -> str:
    return _b64(_read_bytes(path))


def _get_query_param(name: str) -> Optional[str]:
    """
    Compatible across Streamlit versions:
      - New API: st.query_params
      - Legacy: st.experimental_get_query_params
    """
    try:
        val = st.query_params.get(name)  # type: ignore[attr-defined]
        if val is None:
            return None
        if isinstance(val, list):
            return val[0] if val else None
        return str(val)
    except Exception:
        qp = st.experimental_get_query_params()
        vals = qp.get(name, [])
        return vals[0] if vals else None


def _inject_shell_css() -> None:
    # Keep this light—your app already uses Streamlit defaults.
    st.markdown(
        """
        <style>
          /* Slightly tighter tab spacing */
          div[data-baseweb="tab-list"] button { padding-top: 10px; padding-bottom: 10px; }

          /* Make horizontal carousels feel nicer */
          .tarasha-scroll::-webkit-scrollbar { height: 10px; }
          .tarasha-scroll::-webkit-scrollbar-thumb { border-radius: 999px; }
        </style>
        """,
        unsafe_allow_html=True,
    )


# -----------------------------
# Header / shell
# -----------------------------
def _render_header() -> tuple:
    _inject_shell_css()

    with st.container():
        # Logo + title row (simple + robust)
        logo_html = ""
        if _LOGO_PATH.exists():
            logo_b64 = _img_to_b64(str(_LOGO_PATH))
            logo_html = f"""
              <div style="display:flex; align-items:center; gap:12px; margin-top:6px;">
                <img src="data:image/png;base64,{logo_b64}" style="height:40px; width:auto;" />
                <div style="font-size:22px; font-weight:700;">TaRaSha Research</div>
              </div>
            """
        else:
            logo_html = '<div style="font-size:22px; font-weight:700; margin-top:6px;">TaRaSha Research</div>'

        st.markdown(logo_html, unsafe_allow_html=True)

    tab_home, tab_equity_research = st.tabs(["Home", "Equity Research"])
    return tab_home, tab_equity_research


# -----------------------------
# Hero banner carousel
# -----------------------------
def _render_hero_carousel(image_paths: List[Path]) -> None:
    """
    Render a responsive hero image carousel.
    Notes:
      - Designed as a carousel so you can add more hero images later.
      - Leaves a clean overlay layer available for future Call To Action buttons.
    """
    max_width_px = 1250
    banner_ratio_w = 1802
    banner_ratio_h = 601
    hero_iframe_height = int((max_width_px * banner_ratio_h / banner_ratio_w) + 95)

    valid_paths = [p for p in image_paths if p.exists()]
    if not valid_paths:
        st.warning("Hero banner image not found. Put 'Hero_Banner.png' under ./assets/.")
        return

    imgs_b64 = [_img_to_b64(str(p)) for p in valid_paths]
    slides_html = "".join(
        [
            f"""
            <div class="slide">
              <img src="data:image/png;base64,{b64}" />
              <div class="overlay"></div>
            </div>
            """
            for b64 in imgs_b64
        ]
    )
    show_controls = "true" if len(imgs_b64) > 1 else "false"

    html = f"""
    <style>
      .hero-wrap {{ max-width:{max_width_px}px; margin: 12px auto 8px auto; position:relative; }}
      .carousel {{ position:relative; overflow:hidden; border-radius: 18px; }}
      .track {{ display:flex; transition: transform 0.4s ease; }}
      .slide {{ min-width:100%; position:relative; }}
      .slide img {{ width:100%; height:auto; display:block; }}
      .overlay {{ position:absolute; inset:0; pointer-events:none; }}
      .btn {{ position:absolute; top:50%; transform: translateY(-50%); border:none; background:rgba(0,0,0,0.35);
              color:white; width:44px; height:44px; border-radius:999px; cursor:pointer; display:flex;
              align-items:center; justify-content:center; font-size:24px; }}
      .btn:hover {{ background:rgba(0,0,0,0.50);}}
      .btn.prev {{ left:12px; }}
      .btn.next {{ right:12px; }}
      .dots {{ display:flex; gap:8px; justify-content:center; margin-top:10px; }}
      .dot {{ width:10px; height:10px; border-radius:999px; background:rgba(0,0,0,0.25); cursor:pointer; }}
      .dot.active {{ background:rgba(0,0,0,0.55); }}
    </style>

    <div class="hero-wrap">
      <div class="carousel" id="car">
        <div class="track" id="track">
          {slides_html}
        </div>
        <button class="btn prev" id="prev" aria-label="Previous" style="display:{'flex' if len(imgs_b64)>1 else 'none'};">‹</button>
        <button class="btn next" id="next" aria-label="Next" style="display:{'flex' if len(imgs_b64)>1 else 'none'};">›</button>
      </div>
      <div class="dots" id="dots"></div>
    </div>

    <script>
      const showControls = {show_controls};
      const track = document.getElementById('track');
      const slides = Array.from(track.children);
      const dots = document.getElementById('dots');
      const prev = document.getElementById('prev');
      const next = document.getElementById('next');
      let idx = 0;

      function renderDots() {{
        dots.innerHTML = '';
        slides.forEach((_, i) => {{
          const d = document.createElement('div');
          d.className = 'dot' + (i === idx ? ' active' : '');
          d.onclick = () => go(i);
          dots.appendChild(d);
        }});
      }}

      function go(i) {{
        idx = (i + slides.length) % slides.length;
        track.style.transform = `translateX(${{-idx * 100}}%)`;
        renderDots();
      }}

      if (showControls) {{
        prev.onclick = () => go(idx - 1);
        next.onclick = () => go(idx + 1);
        renderDots();
      }}
    </script>
    """
    components.html(html, height=hero_iframe_height, scrolling=False)


# -----------------------------
# Featured articles (PDF viewer)
# -----------------------------
def _discover_articles() -> List[Dict[str, str]]:
    """
    Find PDFs under:
      1) ./assets/articles (preferred)
      2) anywhere under ./assets (fallback)
    Returns a list of {slug,title,pdf_path,thumb_path?}.
    """
    pdfs: List[Path] = []

    if _ARTICLES_DIR.exists():
        pdfs = sorted(_ARTICLES_DIR.rglob("*.pdf"))
    else:
        pdfs = sorted(_ASSETS_DIR.rglob("*.pdf"))

    articles: List[Dict[str, str]] = []
    for pdf in pdfs:
        slug = pdf.stem
        title = slug.replace("_", " ").replace("-", " ").strip().title()

        # Optional: look for a thumbnail image with the same stem next to the PDF.
        thumb = None
        for ext in (".png", ".jpg", ".jpeg", ".webp"):
            cand = pdf.with_suffix(ext)
            if cand.exists():
                thumb = cand
                break

        item: Dict[str, str] = {
            "slug": slug,
            "title": title or slug,
            "pdf_path": str(pdf),
        }
        if thumb is not None:
            item["thumb_path"] = str(thumb)
        articles.append(item)

    return articles


@st.cache_data(show_spinner=False)
def _pdf_first_page_png_b64(pdf_path: str) -> Optional[str]:
    """
    Best-effort thumbnail from the PDF's first page.
    If PyMuPDF is unavailable, return None.
    """
    try:
        import fitz  # PyMuPDF
    except Exception:
        return None

    try:
        doc = fitz.open(pdf_path)
        page = doc.load_page(0)
        pix = page.get_pixmap(matrix=fitz.Matrix(1.2, 1.2), alpha=False)
        img_bytes = pix.tobytes("png")
        return _b64(img_bytes)
    except Exception:
        return None
    finally:
        try:
            doc.close()  # type: ignore[name-defined]
        except Exception:
            pass


def _render_articles_carousel(articles: List[Dict[str, str]]) -> None:
    if not articles:
        st.info("No featured articles found yet. Add PDF files under ./assets/articles/.")
        return

    cards_html = ""
    for a in articles:
        slug = a["slug"]
        title = a["title"]

        thumb_b64 = None
        if "thumb_path" in a and Path(a["thumb_path"]).exists():
            thumb_b64 = _img_to_b64(a["thumb_path"])
        else:
            # Try rendering the first page
            thumb_b64 = _pdf_first_page_png_b64(a["pdf_path"])

        if thumb_b64:
            img_tag = f'<img src="data:image/png;base64,{thumb_b64}" />'
        else:
            # Simple placeholder (no external assets)
            img_tag = """
              <div style="height:160px; display:flex; align-items:center; justify-content:center;
                          background: rgba(0,0,0,0.04); font-size: 40px;">
                📄
              </div>
            """

        # Open in a new tab as a Streamlit-rendered viewer page (avoids broken static PDF links).
        href = f"?article={slug}"

        cards_html += f"""
          <div class="card">
            <div class="thumb">{img_tag}</div>
            <div class="body">
              <div class="title">{title}</div>
              <div class="meta">Portable Document Format • opens in a new tab</div>
              <a class="read" href="{href}" target="_blank" rel="noopener noreferrer">Open</a>
            </div>
          </div>
        """

    html = f"""
    <style>
      .tarasha-scroll {{ display:flex; gap:16px; overflow-x:auto; padding: 8px 4px 14px 4px; }}
      .card {{ flex: 0 0 340px; border-radius: 18px; border: 1px solid rgba(0,0,0,0.10);
              overflow:hidden; background: rgba(255,255,255,0.90); }}
      .thumb img {{ width:100%; height:160px; object-fit:cover; display:block; }}
      .body {{ padding: 12px 14px 14px 14px; }}
      .title {{ font-size: 16px; font-weight: 700; line-height: 1.2; margin-bottom: 6px; }}
      .meta {{ font-size: 12px; opacity: 0.70; margin-bottom: 10px; }}
      .read {{ display:inline-block; padding: 8px 14px; border-radius: 999px; text-decoration:none;
               border: 1px solid rgba(0,0,0,0.18); font-weight: 600; }}
      .read:hover {{ border-color: rgba(0,0,0,0.35); }}
    </style>
    <div class="tarasha-scroll">
      {cards_html}
    </div>
    """
    components.html(html, height=260, scrolling=False)


def _render_article_viewer(article: Dict[str, str]) -> None:
    title = article.get("title", "Featured Article")
    pdf_path = article.get("pdf_path", "")

    st.markdown(f"## {title}")
    st.markdown(
        '<a href="?" style="text-decoration:none;">← Back to Home</a>',
        unsafe_allow_html=True,
    )

    if not pdf_path or not Path(pdf_path).exists():
        st.error("PDF file not found on the server.")
        return

    pdf_bytes = _read_bytes(pdf_path)
    pdf_b64 = _b64(pdf_bytes)

    # Render in an iframe inside the new tab.
    iframe_html = f"""
      <iframe
        src="data:application/pdf;base64,{pdf_b64}"
        width="100%"
        height="900"
        style="border:none; border-radius: 14px;"
      ></iframe>
    """
    components.html(iframe_html, height=920, scrolling=True)

    # Always provide a download fallback.
    st.download_button(
        "Download PDF",
        data=pdf_bytes,
        file_name=Path(pdf_path).name,
        mime="application/pdf",
    )


# -----------------------------
# Home + Equity Research bodies
# -----------------------------
def _render_home_body(articles: List[Dict[str, str]]) -> None:
    # Hero banner carousel (add more images here later)
    hero_images = [_HERO_BANNER_PATH]

    # Fallback: if the image isn't under assets yet, allow running from repo root.
    if not _HERO_BANNER_PATH.exists():
        alt = Path(__file__).parent / "Hero_Banner.png"
        if alt.exists():
            hero_images = [alt]

    _render_hero_carousel(hero_images)

    st.markdown("### Featured Articles")
    _render_articles_carousel(articles)


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
    st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)


def main() -> None:
    articles = _discover_articles()

    # If user opened an article in a new tab, render the PDF viewer and stop.
    slug = _get_query_param("article")
    if slug:
        article = next((a for a in articles if a.get("slug") == slug), None)
        if article is None:
            st.error("Unknown article.")
        else:
            _render_article_viewer(article)
        return

    tab_home, tab_equity_research = _render_header()

    with tab_home:
        _render_home_body(articles)

    with tab_equity_research:
        _render_equity_research_body()

    _render_footer()


if __name__ == "__main__":
    main()
