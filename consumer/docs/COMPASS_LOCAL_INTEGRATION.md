# Local Compass entry from Discover

The unauthenticated Discover home page is now the shared TaRaSha entry. Its
header shows the approved plain TaRaSha logo. Login to Compass is royal blue
and appears left of the existing gold Log in to Discover button. On narrow
screens, Compass appears first and both buttons stay fixed near the bottom
of the screen while the page scrolls. Safe-area spacing keeps them clear of
the phone’s bottom edge, and extra landing-footer space keeps its content
reachable above the buttons. The existing hero image is unchanged.

Discover selection still calls the existing `navigate("login")` flow. Discover
login, registration, password recovery and authenticated application behavior
are unchanged, including their Discover branding.

## Run locally

1. Start the independent Compass application on `http://localhost:5174`, following
   the TaRaShaCompass repository README.
2. Start Discover using its existing local development instructions. The Compass
   link defaults to `http://localhost:5174/#/compass/login`.
3. To use a different address, add `VITE_COMPASS_URL` to Discover's local environment
   file using the full Compass login URL, then restart Vite. This value is public
   browser configuration and must never contain a password, token or other secret.

The link performs ordinary same-tab navigation to the separate application.
Compass owns separate accounts and server-saved business profiles. No Discover
credentials, sessions, company research or business data are sent with the link.
Use the same host spelling consistently for the local applications.

## Scope and checks

Only the initial shared entry, its logo, scoped presentation rules, public link
configuration and related tests are part of this integration. No production
hosting configuration or deployment is included. The new destination must be
configured explicitly when a later production release is authorized.

Targeted verification: `npm test -- src/compassEntry.test.tsx` and `npm run build`.
Browser verification should cover both entries, the unchanged Discover login,
and desktop/mobile button order.
