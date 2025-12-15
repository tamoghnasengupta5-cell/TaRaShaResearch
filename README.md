# TaRaSha Equity Research Tool (Streamlit)

## Run locally
1. Install Python 3.11+
2. Open a terminal in this folder and run:
   - `python -m venv .venv`
   - Windows: `.venv\Scripts\activate`
   - Mac/Linux: `source .venv/bin/activate`
   - `pip install -r requirements.txt`
   - `streamlit run app.py`

## Notes for Azure App Service
- The app stores its SQLite database at:
  - Local: `./app.db`
  - Azure App Service: `/home/app.db` (persistent storage)
- Put these images in `./assets/` (case-sensitive):
  - `Hero_Banner.png` (1802x601 recommended)
  - `tarasha_logo.png`
