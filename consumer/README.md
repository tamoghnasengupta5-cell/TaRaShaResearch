# TaRaSha Company Lens

Initial consumer-facing preview for the first ten invited users. It is intentionally separate from the Streamlit research application and runs entirely with fictional companies and illustrative financial data.

## Included

- Responsive landing page and navigation
- Alphabetical company discovery without recommendations or rankings
- Plain-language company pages organised around growth, profitability, cash and debt
- Side-by-side comparison for up to three companies
- Device-local watchlist
- Learning library and glossary
- Data-source/freshness treatment
- Explicit founding-user and illustrative-data labelling
- Supabase free-tier schema with invite, profile, watchlist, consent and row-level security foundations

## Run locally

Requires Node.js 22 or newer.

```bash
cd consumer
npm install
npm run dev
```

Open the local URL shown by Vite. No account, API key or external service is required for this preview.

## Validate

```bash
npm run check
```

## Zero-cost deployment

1. Create a Cloudflare Pages project connected to this repository and branch.
2. Set the root directory to `consumer`.
3. Use `npm run build` as the build command.
4. Use `dist` as the output directory.
5. Keep the generated `*.pages.dev` domain for the founding-user phase.

The included `_headers` file applies baseline browser security headers on Cloudflare Pages.

## Free-tier backend preparation

The preview currently stores its watchlist in the browser. For real invite-only accounts:

1. Create a new Supabase project dedicated to Company Lens.
2. Run `supabase/001_initial_schema.sql` in its SQL editor.
3. Insert up to ten lowercase email addresses into `beta_invites` from the protected SQL editor.
4. Add the public project URL and anonymous key to `.env.local` using `.env.example`.
5. Add the authentication screens in the next iteration; the schema already rejects uninvited users and an eleventh profile.

Never reuse the research application database or expose a Supabase service-role key in the browser.

## Product boundary

This preview provides educational explanations and fictional historical facts. It deliberately excludes scores, ranked lists, intrinsic values, price targets, upside/downside, buy/sell/hold language, portfolio suitability and live prices.
