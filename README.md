# TaRaSha on Azure App Service (Streamlit)

## What was fixed
- Avoids SQLite "database is locked" by using a single shared SQLite connection per app process.
- Uses Azure-persistent DB path (/home/app.db) when running on Azure App Service.

## Required Azure settings
1) Startup Command:
   python -m streamlit run app.py --server.address 0.0.0.0 --server.port 8000 --server.headless true

2) App setting:
   WEBSITES_PORT = 8000

## GitHub Actions deployment
This repo includes .github/workflows/main_tarasha.yml which deploys using a publish-profile secret.

Create a repo secret:
- Name: TARASHA_AZURE_WEBAPP_PUBLISH_PROFILE
- Value: paste the contents of your downloaded .PublishSettings file.
