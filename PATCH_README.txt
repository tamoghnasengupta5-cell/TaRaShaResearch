This patch fixes the Featured Articles PDF opening issue by:
- Rendering articles as Streamlit viewer pages (opens in a new tab) instead of linking to a static PDF path.
- Embedding the PDF inside the page via an <iframe>, plus a download fallback.

How to apply:
1) Replace your repo's app.py with this app.py
2) Update requirements.txt (adds pymupdf for automatic PDF first-page thumbnails)
3) Put your PDFs under: assets/articles/
   - Optional: add a thumbnail image next to the PDF with the same name (e.g., My_Article.pdf + My_Article.png)
