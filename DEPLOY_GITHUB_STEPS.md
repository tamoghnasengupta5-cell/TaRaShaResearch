# Deploy (novice step-by-step) — GitHub -> Azure App Service

## Part A — Put this code into GitHub
1) Create a GitHub repo
- Go to github.com -> click your profile icon -> **Your repositories**
- Click **New**
- Repository name: `TaRaShaResearch` (or anything)
- Choose **Private** (recommended) or Public
- Click **Create repository**

2) Upload these files to the repo
Option 1 (easiest, no command line):
- Open the new repo in GitHub
- Click **Add file** -> **Upload files**
- Drag & drop **all files and folders** from this zip
  (including `assets/`, `requirements.txt`, `.github/`)
- Click **Commit changes**

Option 2 (Git command line):
- Install Git (if not installed)
- Open a terminal in the folder where these files are
- Run:
  - `git init`
  - `git add .`
  - `git commit -m "Initial commit"`
  - `git branch -M main`
  - `git remote add origin <your repo URL>`
  - `git push -u origin main`

## Part B — Add the Azure Publish Profile secret in GitHub
1) Go to your GitHub repo -> **Settings**
2) Left menu -> **Secrets and variables** -> **Actions**
3) Click **New repository secret**
4) Name: `TARASHA_AZURE_WEBAPP_PUBLISH_PROFILE`
5) Value: paste the full contents of your downloaded publish profile XML
6) Click **Add secret**

## Part C — Make sure Azure uses the Streamlit startup command
Azure portal -> your Web App **TaRaSha** -> **Settings** -> **Configuration**
- Find **Startup Command** and set to:

`python -m streamlit run app.py --server.port 8000 --server.address 0.0.0.0 --server.headless true`

(If you don’t see “Startup Command”, your app might not be Linux.)

## Part D — Trigger the deployment
1) GitHub repo -> **Actions**
2) Open workflow: **Build and deploy TaRaSha to Azure App Service**
3) Click **Run workflow** (or push any commit to main)

## Part E — Verify
Azure portal -> Web App -> **Overview** -> click the **Default domain** URL.

Troubleshooting:
- If the site shows “Please wait…” forever, enable WebSockets (Streamlit needs it).
  In Azure Cloud Shell (portal top bar > `>_`):
  `az webapp config set --name TaRaSha --resource-group TaRaShaResearch --web-sockets-enabled true`
