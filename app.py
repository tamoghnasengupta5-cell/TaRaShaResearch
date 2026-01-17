import base64
import html as html_lib
import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, List

import streamlit as st
import streamlit.components.v1 as components

from admin import render_admin_tab
from bs_metrics import render_balance_sheet_metrics_tab
from cap_structure_cost import render_capital_structure_cost_of_capital_tab
from cash_flow_spread import render_cash_flow_and_spread_tab
from combined_dashboard import render_combined_dashboard_tab
from data_upload import render_data_upload_tab
from pl_metrics import render_pl_metrics_tab
from key_data import render_key_data_tab

# --- Branding assets (kept local to the repo) ---
_ASSETS_DIR = Path(__file__).parent / "assets"
_LOGO_PATH = _ASSETS_DIR / "tarasha_logo.png"
_HERO_BANNER_PATH = _ASSETS_DIR / "Hero_Banner.png"
_HERO_BANNER_2_PATH = _ASSETS_DIR / "Hero_Banner_2_Semiconductors.png"
_HERO_BANNER_3_PATH = _ASSETS_DIR / "Hero_Banner_3_YoY_Growth.png"

# --- Featured Articles assets ---
_ARTICLES_DIR = _ASSETS_DIR / "articles"

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
def _file_to_b64(path: Path) -> str:
    return _bytes_to_b64(path.read_bytes())

def _inject_shell_css() -> None:
    """
    Minimal CSS to keep the Home page clean.
    (We avoid over-customizing Streamlit so we don't break layout across upgrades.)
    """
    st.markdown(
        """
        <style>
          /* Full-bleed layout: remove Streamlit's default max-width + side padding */
          .block-container {
            max-width: 100% !important;
            padding-top: 1.0rem !important;
            padding-bottom: 2rem !important;
            padding-left: 10px !important;
            padding-right: 10px !important;
          }

          /* Some Streamlit versions add padding on these containers too */
          [data-testid="stAppViewContainer"],
          [data-testid="stAppViewContainer"] > .main,
          [data-testid="stMainBlockContainer"] {
            padding-left: 10px !important;
            padding-right: 10px !important;
          }

          /* Keep tabs readable while the page is full-bleed */
          [data-baseweb="tab-list"] {
            gap: 0.75rem;
            padding-left: 10px;
            padding-right: 10px;
          }


          /* Header logo: reduce vertical real estate */
          div[data-testid="stImage"] {
            margin-top: 0.0rem !important;
            margin-bottom: 0.15rem !important;
          }

          /* Nicer headers */
          h1, h2, h3 { letter-spacing: -0.2px; }
        </style>
        """,
        unsafe_allow_html=True,
    )

def _render_header() -> tuple:
    _inject_shell_css()

    with st.container():
        # Logo (optional; app runs fine even if the file is missing)
        if _LOGO_PATH.exists():
            st.image(str(_LOGO_PATH), width=200)

        tab_home, tab_key_data, tab_equity_research = st.tabs(["Home", "Key Data", "Equity Research"])
        return tab_home, tab_key_data, tab_equity_research

def _render_hero_carousel(image_paths: List[Path], slide_meta: List[Dict] | None = None) -> None:
    """
    Render a responsive hero image carousel.
    - Designed as a carousel so you can add more hero images later.
    - Keeps aspect ratio; avoids cropping/distortion.
    - Leaves a clean overlay layer available for future CTA buttons.
    """
    # Standard hero banner aspect ratio (prevents cropping/distortion)
    # Target size: 1802x601
    banner_ratio_w = 1802
    banner_ratio_h = 601

    # Starting height only — the iframe auto-resizes via JS (postMessage)
    hero_iframe_height = 720
    # "slide_meta" is a future-proof hook for adding CTA behavior per slide.
    # We intentionally do NOT implement CTA click behavior yet.
    if slide_meta is None:
        slide_meta = [None] * len(image_paths)
    # Keep ordering stable, but skip missing files.
    pairs = [(Path(p), m) for p, m in zip(image_paths, slide_meta)]
    pairs = [(p, m) for p, m in pairs if p.exists()]
    if not pairs:
        st.warning("Hero banner image not found. Put 'Hero_Banner.png' under ./assets/.")
        return

    imgs_b64 = [_file_to_b64(p) for p, _ in pairs]
    metas = [m for _, m in pairs]

    # Optional: a transparent text banner overlay on specific slides (e.g., the main Hero_Banner).
    slides_parts: List[str] = []
    for b64, meta in zip(imgs_b64, metas):
        overlay_text_html = ""
        if isinstance(meta, dict) and meta.get("overlay_text"):
            ov = meta.get("overlay_text") or {}
            line1 = html_lib.escape(str(ov.get("line1", "")))
            line2 = html_lib.escape(str(ov.get("line2", "")))
            cta_label = html_lib.escape(str(ov.get("button_label", "Explore Combined Dashboard")))
            overlay_text_html = f"""
                <div class=\"ta-hero-textbanner\" aria-label=\"Hero banner description\">
                  <div class=\"ta-hero-textbanner-line1\">{line1}</div>
                  <div class=\"ta-hero-textbanner-line2\">{line2}</div>
                  <button class=\"ta-hero-cta\" type=\"button\" onclick=\"navigateToCombinedDashboard(event)\">{cta_label}</button>
                </div>
            """

        slides_parts.append(
            f"""
            <div class=\"ta-hero-slide\" data-slide-meta='{json.dumps(meta) if meta else ""}'>
              <div class=\"ta-hero-frame\">
                <img src=\"data:image/png;base64,{b64}\" alt=\"Hero banner\"/>
                {overlay_text_html}
                <div class=\"ta-hero-overlay\" aria-hidden=\"true\"></div>
              </div>
            </div>
            """
        )

    slides_html = "".join(slides_parts)

    show_controls = "block" if len(imgs_b64) > 1 else "none"
    html = f"""
    <html>
    <head>
      <style>
        .ta-hero {{
          width: 100%;
          max-width: 100%;
          margin: 0 0 0.25rem 0;
          position: relative;
          user-select: none;
          -webkit-user-select: none;
        }}
        .ta-hero-slide {{ display: none; }}
        .ta-hero-slide.active {{ display: block; }}
        .ta-hero-frame {{
          width: 100%;
          /*aspect-ratio: {banner_ratio_w} / {banner_ratio_h};*/
          border-radius: 18px;
          overflow: hidden;
          box-shadow: 0 10px 30px rgba(0,0,0,0.08);
          position: relative;
          background: #ffffff;
        }}
        .ta-hero-frame img {{
          width: 100%;
          height: 100%;
          object-fit: contain; /* critical: no crop/distort */
          display: block;
        }}
        .ta-hero-overlay {{
          position: absolute;
          inset: 0;
          background: linear-gradient(180deg, rgba(255,255,255,0.00) 35%, rgba(0,0,0,0.04) 100%);
          pointer-events: none;
          z-index: 1;
        }}

        /* Transparent text banner (main Hero_Banner only) */
        .ta-hero-textbanner {{
          position: absolute;
          top: 65%;
          right: clamp(28px, 5.5vw, 120px);
          transform: translateY(-50%);
          width: min(720px, 56%);
          padding: 40px 22px 27px 22px;
          border-radius: 16px;
          background: rgba(255, 255, 255, 0.42);
          border: 1px solid rgba(255, 255, 255, 0.50);
          box-shadow: 0 10px 26px rgba(0,0,0,0.12);
          backdrop-filter: blur(10px);
          -webkit-backdrop-filter: blur(10px);
          color: rgba(15, 17, 20, 0.92);
          line-height: 1.38;
          font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", "Liberation Sans", sans-serif;
          -webkit-font-smoothing: antialiased;
          text-rendering: optimizeLegibility;
          z-index: 2;
          pointer-events: none;
        }}
        .ta-hero-textbanner-line1 {{
          font-size: 16px;
          font-weight: 600;
          letter-spacing: -0.15px;
        }}
        .ta-hero-textbanner-line2 {{
          margin-top: 10px;
          font-size: 15px;
          font-weight: 450;
          opacity: 0.92;
        }}
        .ta-hero-cta {{
          margin-top: 16px;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          padding: 10px 14px;
          border-radius: 999px;
          border: 1px solid rgba(15, 17, 20, 0.18);
          background: rgba(255, 255, 255, 0.72);
          color: rgba(15, 17, 20, 0.92);
          font-size: 14px;
          font-weight: 600;
          letter-spacing: -0.10px;
          cursor: pointer;
          pointer-events: auto;
          box-shadow: 0 8px 20px rgba(0,0,0,0.10);
          transition: transform 140ms ease, background 140ms ease, box-shadow 140ms ease;
        }}
        .ta-hero-cta:hover {{
          background: rgba(255, 255, 255, 0.88);
          transform: translateY(-1px);
          box-shadow: 0 10px 26px rgba(0,0,0,0.12);
        }}
        .ta-hero-cta:active {{
          transform: translateY(0px);
        }}
        .ta-hero-cta:focus {{
          outline: none;
        }}
        .ta-hero-cta:focus-visible {{
          outline: 2px solid rgba(15, 17, 20, 0.30);
          outline-offset: 2px;
        }}
        @media (max-width: 960px) {{
          .ta-hero-textbanner {{
            left: 14px;
            right: 14px;
            top: auto;
            bottom: 14px;
            transform: none;
            width: auto;
          }}
        }}

        .ta-hero-nav {{
          display: {show_controls};
          position: absolute;
          inset: 0;
          pointer-events: none;
        }}
        .ta-hero-btn {{
          pointer-events: auto;
          position: absolute;
          top: 50%;
          transform: translateY(-50%);
          width: 42px;
          height: 42px;
          border-radius: 999px;
          border: 1px solid rgba(0,0,0,0.08);
          background: rgba(255,255,255,0.85);
          color: rgba(0,0,0,0.75);
          display: flex;
          align-items: center;
          justify-content: center;
          cursor: pointer;
          font-size: 20px;
          box-shadow: 0 8px 20px rgba(0,0,0,0.10);
        }}
        .ta-hero-btn:hover {{
          background: rgba(255,255,255,0.95);
        }}
        .ta-hero-btn.prev {{ left: 10px; }}
        .ta-hero-btn.next {{ right: 10px; }}

        .ta-hero-dots {{
          display: {show_controls};
          text-align: center;
          margin-top: 10px;
        }}
        .ta-dot {{
          display: inline-block;
          width: 7px;
          height: 7px;
          margin: 0 4px;
          border-radius: 999px;
          background: rgba(0,0,0,0.20);
          cursor: pointer;
        }}
        .ta-dot.active {{
          background: rgba(0,0,0,0.55);
        }}
      </style>
    </head>
    <body>
      <div class="ta-hero">
        {slides_html}
        <div class="ta-hero-nav">
          <div class="ta-hero-btn prev" onclick="prevSlide()" aria-label="Previous slide">‹</div>
          <div class="ta-hero-btn next" onclick="nextSlide()" aria-label="Next slide">›</div>
        </div>
      </div>
      <div class="ta-hero-dots" id="dots"></div>

      <script>
        const slides = Array.from(document.querySelectorAll('.ta-hero-slide'));
        const dotsEl = document.getElementById('dots');
        let idx = 0;

        function _clickStreamlitTabByText(label) {{
          try {{
            const doc = window.parent.document;
            const tabs = Array.from(doc.querySelectorAll('button[role=\"tab\"]'));
            const target = tabs.find(b => ((b.innerText || '').trim() === label));
            if (target) {{
              target.click();
              return true;
            }}
          }} catch (e) {{
            console.warn('tab click failed', e);
          }}
          return false;
        }}

        function navigateToCombinedDashboard(evt) {{
          if (evt) {{
            evt.preventDefault();
            evt.stopPropagation();
          }}
          const clickedOuter = _clickStreamlitTabByText('Equity Research');
          const delay = clickedOuter ? 250 : 0;
          window.setTimeout(() => {{
            let tries = 0;
            const maxTries = 12;
            const timer = window.setInterval(() => {{
              tries += 1;
              const ok = _clickStreamlitTabByText('Combined Dashboard');
              if (ok || tries >= maxTries) {{
                window.clearInterval(timer);
              }}
            }}, 200);
          }}, delay);
        }}

        function renderDots() {{
          if (!dotsEl) return;
          dotsEl.innerHTML = '';
          slides.forEach((_, i) => {{
            const d = document.createElement('span');
            d.className = 'ta-dot' + (i === idx ? ' active' : '');
            d.onclick = () => goTo(i);
            dotsEl.appendChild(d);
          }});
        }}

        function show(i) {{
          slides.forEach((s, n) => s.classList.toggle('active', n === i));
          idx = i;
          renderDots();
        }}

        function nextSlide() {{
          show((idx + 1) % slides.length);
        }}

        function prevSlide() {{
          show((idx - 1 + slides.length) % slides.length);
        }}

        function goTo(i) {{
          show(i);
        }}

        // Init
        show(0);

        // Auto-rotate disabled: slides advance only via user navigation (arrows/dots)
                // Auto-resize Streamlit iframe height (prevents cropping on wide screens)
        (function () {{
          function sendHeight() {{
            const height = document.documentElement.scrollHeight;
            window.parent.postMessage(
              {{ isStreamlitMessage: true, type: "streamlit:setFrameHeight", height: height }},
              "*"
            );
          }}

          function start() {{
            sendHeight();
            if ("ResizeObserver" in window && document.body) {{
              const ro = new ResizeObserver(sendHeight);
              ro.observe(document.body);
            }} else {{
              // Fallback: ping a few times after load
              let n = 0;
              const t = setInterval(() => {{
                sendHeight();
                n += 1;
                if (n >= 12) clearInterval(t);
              }}, 250);
            }}
          }}

          window.addEventListener("load", start);
          window.addEventListener("resize", sendHeight);
        }})();
</script>
    </body>
    </html>
    """
    components.html(html, height=hero_iframe_height, scrolling=False)

def _slugify(value: str) -> str:
    """Convert an arbitrary filename/path into a stable, URL/DOM-safe id."""
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "article"

def _humanize_filename(stem: str) -> str:
    """Turn a filename stem into a readable title."""
    # Replace underscores with spaces, collapse repeated whitespace, and trim.
    s = stem.replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _find_thumbnail_for_pdf(pdf_path: Path) -> Path:
    """Find a matching thumbnail for a given PDF.

    Convention: <pdf_stem>_thumb.<ext>
    """
    stem = pdf_path.stem
    for ext in (".png", ".jpg", ".jpeg", ".webp"):
        cand = pdf_path.with_name(f"{stem}_thumb{ext}")
        if cand.exists():
            return cand
    # Fallback (still renders a card even if thumbnail is missing)
    return _LOGO_PATH

def _get_featured_articles() -> List[Dict]:
    """Discover featured articles automatically from the ./assets folder.

    Rules:
    - Any *.pdf under ./assets (including ./assets/articles) is treated as a featured article.
    - Thumbnail is resolved by naming convention: <pdf_name>_thumb.png (also supports jpg/jpeg/webp).
      Example: my_article.pdf -> my_article_thumb.png
    """
    articles: List[Dict] = []

    if not _ASSETS_DIR.exists():
        return articles

    pdf_paths = sorted(_ASSETS_DIR.rglob("*.pdf"), key=lambda p: p.name.lower())
    for pdf_path in pdf_paths:
        if not pdf_path.is_file():
            continue

        # Use a stable id derived from the asset-relative path to avoid collisions.
        try:
            rel = pdf_path.relative_to(_ASSETS_DIR).as_posix()
        except Exception:
            rel = pdf_path.name
        article_id = _slugify(rel)

        articles.append(
            {
                "id": article_id,
                "title": _humanize_filename(pdf_path.stem),
                "pdf_path": pdf_path,
                "thumb_path": _find_thumbnail_for_pdf(pdf_path),
            }
        )

    return articles


def _render_featured_articles_carousel(articles: List[Dict]) -> None:
    """
    Horizontal, scrollable carousel of article cards.
    Clicking a card opens the PDF in a new browser tab.

    Implementation detail:
    - We DO NOT navigate directly to a huge `data:application/pdf;base64,...` URL.
      Some browsers/hosts will open a blank tab for large data URLs.
    - Instead, we open a `blob:` URL created client-side from the base64 PDF bytes.
      This is much more reliable and keeps the address bar sane.
    """
    # Filter only those where assets exist (prevents runtime surprises)
    safe_articles: List[Dict] = []
    for a in articles:
        if not a["pdf_path"].exists() or not a["thumb_path"].exists():
            continue
        safe_articles.append(a)

    if not safe_articles:
        st.info("No featured articles found under ./assets/articles yet.")
        return

    # Build card HTML + a JS map of {article_id: pdf_base64}
    cards_html: List[str] = []
    pdf_map = {}
    for a in safe_articles:
        article_id = a["id"]
        title = a["title"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        thumb_b64 = _file_to_b64(a["thumb_path"])
        pdf_b64 = _file_to_b64(a["pdf_path"])

        # Store raw base64 (NO data: prefix) so JS can build a Blob.
        pdf_map[article_id] = pdf_b64

        cards_html.append(
            f"""
            <button type="button" class="ta-article-card" onclick="openPdf('{article_id}')">
              <img class="ta-article-thumb" src="data:image/png;base64,{thumb_b64}" alt="{title}"/>
              <div class="ta-article-title">{title}</div>
            </button>
            """
        )

    pdf_map_json = json.dumps(pdf_map)

    html = f"""
    <html>
    <head>
      <style>
        .ta-articles {{
          width: 100%;
          max-width: 100%;
          margin: 0.25rem 0 0 0;
          padding: 0;
        }}

        .ta-scroll {{
          display: flex;
          gap: 18px;
          overflow-x: auto;
          padding: 6px 2px 12px 2px;
          scroll-snap-type: x mandatory;
          -webkit-overflow-scrolling: touch;
        }}
        .ta-scroll::-webkit-scrollbar {{
          height: 10px;
        }}

        .ta-article-card {{
          flex: 0 0 auto;
          width: 320px;
          border-radius: 16px;
          overflow: hidden;
          border: 1px solid rgba(0,0,0,0.08);
          box-shadow: 0 10px 24px rgba(0,0,0,0.06);
          background: #fff;
          scroll-snap-align: start;
          transition: transform 0.12s ease, box-shadow 0.12s ease;
          cursor: pointer;

          /* button reset */
          padding: 0;
          text-align: left;
          border: 1px solid rgba(0,0,0,0.08);
          outline: none;
        }}
        .ta-article-card:hover {{
          transform: translateY(-2px);
          box-shadow: 0 14px 30px rgba(0,0,0,0.10);
        }}

        .ta-article-thumb {{
          width: 100%;
          height: 180px;
          object-fit: cover;
          display: block;
          background: #f3f4f6;
        }}

        .ta-article-title {{
          padding: 12px 14px 14px 14px;
          font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
          font-size: 15px;
          font-weight: 650;
          line-height: 1.25;
        }}
      </style>
      <script>
        const pdfMap = {pdf_map_json};

        function base64ToBlob(base64, contentType, sliceSize) {{
          contentType = contentType || "application/pdf";
          sliceSize = sliceSize || 1024;

          const byteCharacters = atob(base64);
          const byteArrays = [];

          for (let offset = 0; offset < byteCharacters.length; offset += sliceSize) {{
            const slice = byteCharacters.slice(offset, offset + sliceSize);
            const byteNumbers = new Array(slice.length);
            for (let i = 0; i < slice.length; i++) {{
              byteNumbers[i] = slice.charCodeAt(i);
            }}
            const byteArray = new Uint8Array(byteNumbers);
            byteArrays.push(byteArray);
          }}

          return new Blob(byteArrays, {{ type: contentType }});
        }}

        function openPdf(articleId) {{
          const b64 = pdfMap[articleId];
          if (!b64) {{
            alert("Sorry — that article could not be found.");
            return;
          }}

          const blob = base64ToBlob(b64, "application/pdf");
          const url = URL.createObjectURL(blob);

          // Open in a new tab.
          const w = window.open(url, "_blank", "noopener,noreferrer");
          if (!w) {{
            alert("Your browser blocked the popup. Please allow popups for this site and try again.");
          }}

          // Revoke after a bit (don't revoke immediately or the PDF may fail to load).
          setTimeout(() => URL.revokeObjectURL(url), 60 * 1000);
        }}
                // Auto-resize Streamlit iframe height (prevents cropping on wide screens)
        (function () {{
          function sendHeight() {{
            const height = document.documentElement.scrollHeight;
            window.parent.postMessage(
              {{ isStreamlitMessage: true, type: "streamlit:setFrameHeight", height: height }},
              "*"
            );
          }}

          function start() {{
            sendHeight();
            if ("ResizeObserver" in window && document.body) {{
              const ro = new ResizeObserver(sendHeight);
              ro.observe(document.body);
            }} else {{
              // Fallback: ping a few times after load
              let n = 0;
              const t = setInterval(() => {{
                sendHeight();
                n += 1;
                if (n >= 12) clearInterval(t);
              }}, 250);
            }}
          }}

          window.addEventListener("load", start);
          window.addEventListener("resize", sendHeight);
        }})();
</script>
    </head>
    <body>
      <div class="ta-articles">
        <div class="ta-scroll">
          {''.join(cards_html)}
        </div>
      </div>
    </body>
    </html>
    """
    components.html(html, height=260, scrolling=False)

def _render_home_body() -> None:
    # Hero banner carousel (3-slide carousel; CTA hooks reserved for future use)
    hero_images = [_HERO_BANNER_PATH, _HERO_BANNER_2_PATH, _HERO_BANNER_3_PATH]
    hero_meta: List[Dict] = [
        {
            "cta": None,
            "overlay_text": {
                "line1": "Select one or more company buckets and a year range to generate a volatility- and debt-aware composite score that blends P&L growth, balance-sheet strength, free-cash-flow momentum, and ROIC–WACC value creation.",
                "line2": "Get the ranking + the breakdown behind every score, with quick price-history sanity checks to spot the most consistent performers.",
            },
        },
        {
            "cta": {
                "kind": "navigate",
                "target": "combined_dashboard",
                "prepopulated_buckets": [
                    "Technology : Semiconductor",
                    "Technology : Semiconductor Equipment & Materials",
                ],
            }
        },
        {
            "cta": {
                "kind": "navigate",
                "target": "key_data",
                "subtab": "P&L Key Data",
            }
        },
    ]

    # Fallback: if the primary image isn't under assets yet, allow running from repo root.
    if not _HERO_BANNER_PATH.exists():
        alt = Path(__file__).parent / "Hero_Banner.png"
        if alt.exists():
            hero_images[0] = alt

    _render_hero_carousel(hero_images, slide_meta=hero_meta)

    st.markdown("### Featured Articles")
    st.caption("Click an article to open it in a new tab.")
    _render_featured_articles_carousel(_get_featured_articles())

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

def _render_key_data_body() -> None:
    render_key_data_tab()

def _render_footer() -> None:
    # Intentionally empty for now (layout placeholder).
    st.markdown("", unsafe_allow_html=True)

tab_home, tab_key_data, tab_equity_research = _render_header()

with tab_home:
    _render_home_body()

with tab_key_data:
    _render_key_data_body()

with tab_equity_research:
    _render_equity_research_body()

_render_footer()
