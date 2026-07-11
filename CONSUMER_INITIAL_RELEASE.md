# Consumer initial release

## Product objective

Help a non-specialist understand what a company reports without turning the application into a recommendation or suitability service.

## Visual direction

The second design pass reuses the illuminated signature logo from the private research platform and establishes a gold, ivory, charcoal and burgundy visual system. The interface presents company research as a layered dossier: six connected lenses lead from the business model and operating engine through financial history, cash conversion, capital allocation and industry context.

## Branch isolation

The consumer application lives under `consumer/` on `agent/consumer-friendly-initial`. The branch was created in a separate Git worktree from committed `Local_Test` HEAD `c5b421c`. No uncommitted research files were copied, staged or modified.

## Review walkthrough

1. Start on Home and assess whether the value proposition is understandable without financial knowledge.
2. Open Discover, search for a company and choose a reporting-year range.
3. Pull research and confirm that the company card appears only on the transient research shelf.
4. Open a company, switch between derived trends, inspect the statement tables and open source-filing links.
5. Add active session companies to the watchlist, then open Watchlist.
6. Compare two or three pulled companies.
7. Open a learning card and the glossary.
8. Resize the browser to a mobile width and review the bottom navigation.

## Intentional limitations

- Fictional data in credential-free review mode; official SEC data after the zero-cost Cloudflare setup
- No real authentication yet
- Watchlist stored on the current device
- No payment collection
- No email notifications
- No market-price data
- No earnings-call transcripts because SEC EDGAR does not provide them
- No India filing extraction until a lawful zero-cost structured source is confirmed

## Suggested next review decisions

- Brand name: `Company Lens` or an alternative
- Visual direction and density
- Which country/market to cover first
- The first 20–30 real companies
- The exact founding-user invitation flow
- Which reported metrics belong on the first real company page
