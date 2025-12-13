import base64
from pathlib import Path

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

# Streamlit requires set_page_config to be the first Streamlit command in the app.
st.set_page_config(
    page_title="TaRaSha Equity Research Tool",
    layout="wide",
    initial_sidebar_state="collapsed",
)

@st.cache_data(show_spinner=False)
def _img_to_b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("utf-8")


def _inject_shell_css() -> None:
    st.markdown(
        """
        <style>
          /* Give the app a simple "site layout" feel (header / body / footer) */
          .trs-header {
            padding: 0.75rem 0 0.25rem 0;
            border-bottom: 1px solid rgba(0,0,0,0.08);
            margin-bottom: 0.75rem;
          }
          .trs-brand-row {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            padding-bottom: 0.25rem;
          }
          .trs-brand-row img {
            height: 38px;
            width: auto;
            object-fit: contain;
            display: block;
          }
          .trs-footer {
            margin-top: 1.75rem;
            padding: 1.25rem 0 0.75rem 0;
            border-top: 1px solid rgba(0,0,0,0.08);
            min-height: 18px; /* "empty footer" but visible spacing */
          }
          /* Slightly tighten the first block spacing */
          div.block-container { padding-top: 1rem; }

          /* Remove left navigation pane (Streamlit sidebar) */
          [data-testid="stSidebar"],
          [data-testid="stSidebarNav"],
          [data-testid="collapsedControl"] {
            display: none !important;
          }
        
          /* Hide Streamlit chrome (Deploy button / menu / toolbar) */
          header[data-testid="stHeader"] { display: none !important; }
          [data-testid="stToolbar"] { display: none !important; }
          [data-testid="stDecoration"] { display: none !important; }
          .stDeployButton { display: none !important; }
          #MainMenu { visibility: hidden; }
          footer { visibility: hidden; }
</style>
        """,
        unsafe_allow_html=True,
    )


def _render_header() -> tuple:
    _inject_shell_css()

    with st.container():
        st.markdown('<div class="trs-header">', unsafe_allow_html=True)

        tab_home, tab_equity_research = st.tabs(["Home", "Equity Research"])



        st.markdown("</div>", unsafe_allow_html=True)

    return tab_home, tab_equity_research


def _render_hero_carousel(image_paths) -> None:
    """Render a responsive hero image carousel.

    Notes:
    - Designed as a carousel so you can add more hero images later.
    - Leaves a clean overlay layer available for future CTA (Call To Action) buttons.
    """

    # Keep the hero banner from feeling "too big" on large monitors.
    max_width_px = 1250

    # Standard hero banner aspect ratio (prevents cropping/distortion)
    # Target size: 1802x601
    banner_ratio_w = 1802
    banner_ratio_h = 601
    hero_iframe_height = int((max_width_px * banner_ratio_h / banner_ratio_w) + 95)

    valid_paths = [Path(p) for p in image_paths if Path(p).exists()]
    if not valid_paths:
        st.warning(
            "Hero banner image not found. Put 'Hero_Banner.png' under ./assets/."
        )
        return

    imgs_b64 = [_img_to_b64(p) for p in valid_paths]
    slides_html = "".join(
        [
            f"""
            <div class=\"trs-slide\">
              <img src=\"data:image/png;base64,{b64}\" alt=\"Hero banner\" />
              <div class=\"trs-overlay\"><!-- CTA buttons can be injected here later --></div>
            </div>
            """
            for b64 in imgs_b64
        ]
    )

    show_controls = "true" if len(imgs_b64) > 1 else "false"

    html = f"""
    <style>
      .trs-hero-outer {{
        width: 100%;
        display: flex;
        justify-content: center;
        margin: 0.25rem 0 1.25rem 0;
      }}
      .trs-hero-carousel {{
        position: relative;
        width: 100%;
        max-width: {max_width_px}px;
      }}
      .trs-slide {{
        display: none;
        position: relative;
        width: 100%;
        aspect-ratio: {banner_ratio_w} / {banner_ratio_h};
        border-radius: 16px;
        overflow: hidden;
        box-shadow: 0 12px 30px rgba(0,0,0,0.12);

        /* Fix "side bars" when object-fit: contain.
           We paint a blurred, full-bleed version of the same image behind the main image,
           so any leftover space blends with the banner instead of showing a flat color. */
        background: rgba(0,0,0,0.02);
        isolation: isolate;
      }}
      .trs-slide::before {{
        content: "";
        position: absolute;
        inset: -28px;            /* bleed outward so blur doesn't reveal edges */
        background-image: var(--trs-bg-image);
        background-size: cover;
        background-position: center;
        filter: blur(18px);
        transform: scale(1.06);
        opacity: 0.88;
        z-index: 0;
      }}
      .trs-slide::after {{
        content: "";
        position: absolute;
        inset: 0;
        background: rgba(255,255,255,0.08); /* tiny wash to keep it classy */
        z-index: 1;
      }}
      .trs-slide.is-active {{ display: block; }}
      .trs-slide img {{
        width: 100%;
        height: 100%;
        display: block;
        object-fit: contain; /* never crop or distort */
        position: relative;
        z-index: 2;
      }}
      .trs-overlay {{
        position: absolute;
        inset: 0;
        border-radius: inherit;
        pointer-events: none; /* future CTA layer can override per-button */
        z-index: 3;
      }}
      .trs-nav {{
        position: absolute;
        top: 50%;
        transform: translateY(-50%);
        width: 40px;
        height: 40px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.65);
        background: rgba(0,0,0,0.28);
        color: white;
        cursor: pointer;
        display: flex;
        align-items: center;
        justify-content: center;
        user-select: none;
        backdrop-filter: blur(6px);
      }}
      .trs-nav:hover {{ background: rgba(0,0,0,0.38); }}
      .trs-prev {{ left: 14px; }}
      .trs-next {{ right: 14px; }}
      .trs-dots {{
        display: flex;
        justify-content: center;
        gap: 8px;
        margin-top: 10px;
      }}
      .trs-dot {{
        width: 8px;
        height: 8px;
        border-radius: 999px;
        background: rgba(0,0,0,0.25);
      }}
      .trs-dot.is-active {{ background: rgba(0,0,0,0.55); }}
      .trs-controls-hidden {{ display: none; }}
    </style>

    <div class="trs-hero-outer">
      <div class="trs-hero-carousel" id="trsHero" data-show-controls="{show_controls}">
        {slides_html}

        <button class="trs-nav trs-prev" id="trsPrev" aria-label="Previous hero image">&#8249;</button>
        <button class="trs-nav trs-next" id="trsNext" aria-label="Next hero image">&#8250;</button>
        <div class="trs-dots" id="trsDots"></div>
      </div>
    </div>

    <script>
      (function() {{
        const root = document.getElementById('trsHero');
        if (!root) return;

        const slides = Array.from(root.querySelectorAll('.trs-slide'));

        // Use each slide's image as a blurred background so any side space blends in.
        slides.forEach((s) => {{
          const img = s.querySelector('img');
          if (img) {{
            s.style.setProperty('--trs-bg-image', `url("${{img.src}}")`);
          }}
        }});
        const prev = document.getElementById('trsPrev');
        const next = document.getElementById('trsNext');
        const dots = document.getElementById('trsDots');
        const showControls = root.getAttribute('data-show-controls') === 'true';

        if (!showControls) {{
          prev.classList.add('trs-controls-hidden');
          next.classList.add('trs-controls-hidden');
          dots.classList.add('trs-controls-hidden');
        }}

        let idx = 0;
        const renderDots = () => {{
          dots.innerHTML = '';
          slides.forEach((_, i) => {{
            const d = document.createElement('div');
            d.className = 'trs-dot' + (i === idx ? ' is-active' : '');
            d.addEventListener('click', () => show(i));
            dots.appendChild(d);
          }});
        }};
        const show = (i) => {{
          idx = (i + slides.length) % slides.length;
          slides.forEach((s, j) => s.classList.toggle('is-active', j === idx));
          if (showControls) renderDots();
        }};

        if (slides.length === 0) return;
        show(0);

        if (showControls) {{
          prev.addEventListener('click', () => show(idx - 1));
          next.addEventListener('click', () => show(idx + 1));

          // Gentle auto-advance (can be removed later if you want manual-only)
          setInterval(() => show(idx + 1), 9000);
        }}
      }})();
    </script>
    """

    # Height is tuned for the max-width hero banner and leaves room for dots.
    components.html(html, height=hero_iframe_height, scrolling=False)


def _render_home_body() -> None:
    # Hero banner carousel (add more images here later)
    hero_images = [_HERO_BANNER_PATH]

    # Fallback: if the image isn't under assets yet, allow running from repo root.
    if not _HERO_BANNER_PATH.exists():
        alt = Path(__file__).parent / "Hero_Banner.png"
        if alt.exists():
            hero_images = [alt]

    _render_hero_carousel(hero_images)

    # Placeholder for an articles carousel section (to be implemented later).
    # Keeping this section here ensures layout stability when you add it.
    st.markdown("### Featured Articles")
    st.caption("(Article carousel will appear here.)")
    st.empty()


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
    st.markdown('<div class="trs-footer"></div>', unsafe_allow_html=True)


tab_home, tab_equity_research = _render_header()

with tab_home:
    _render_home_body()

with tab_equity_research:
    _render_equity_research_body()

_render_footer()
