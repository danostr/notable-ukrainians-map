# Execution Roadmap: Data Perfection First (`T7`–`T27`)

## Phase 1: Data Perfection & Pipeline Freeze (Do All Data Work First)
1. **[T12] Multi-Source Data Enrichment & Birthplace/Nationality Precision**
   - *Goal:* Extract full `P569` dates (`birth_year`, `birth_month`, `birth_day`), `P570` (`death_year`), citizenships (`P27`), heritage (`P172`), short Wikipedia summaries, and life-path locations (`P69`, `P551`, `P937`, `P20`) in **one consolidated Wikidata pass** so we never re-scrape for later UI tickets.
2. **[T25] Wikipedia Portrait Backfill & Biographical Card Image Styling (ETL Part)**
   - *Goal:* Run the cached MediaWiki `pageimages` batch backfill for records missing Wikidata `P18` so `people_details.json` ships with high static portrait coverage.
3. **[T8] NLP Fallback for Uncategorized Biographies**
   - *Goal:* Classify remaining `Other / Unknown` records (~13.7%) using Wikipedia lead paragraphs so domain coverage is near-complete before running statistics or thresholding.
4. **[T13] Remarkability Thresholding & Dataset Noise Pruning** + **[T17] Prominence Tiers (Data Model)**
   - *Goal:* Prune only broken/spam records in `T13` (preserving legitimate village/hometown figures needed for `T21`) and tag `< 5` sitelink records as `honorable_mention` vs. `remarkable` in `03_categorized.parquet` and `people_details.json`.
5. **[T14] Statistical Data Analysis & Interactive Insights Dashboard (Offline EDA + `stats_summary.json`)**
   - *Goal:* Run statistical checks (`notebooks/eda_ukr_notables.ipynb`) right after `T12` + `T25` + `T8` + `T13/T17` as a final data-quality audit and export pre-aggregated `stats_summary.json`.
6. **[T20] & [T22] Trajectory & Contemporaries Data Export (`journeys.json` & Overlap Index)**
   - *Goal:* Export lazy-loaded multi-stop trajectories (`T20`) and shared-city contemporaries (`T22`) in `04_export_web_json.py`, and lock all new contracts in `tests/test_web_contracts.py` and `tests/test_etl_units.py`.

---

## Phase 2: Self-Contained Assets, Live Deployment & "Know Your Region" UX
7. **[T27] Self-Host Frontend Vendor Libraries (Leaflet & Supercluster)**
   - *Goal:* Vendor Leaflet (`1.9.4`) and Supercluster (`8.0.1`) locally under `web/vendor/` so the site has zero external CDN dependencies and zero browser tracking-prevention warnings.
8. **[T7] Deploy Live Site & Write Portfolio Documentation**
   - *Goal:* Deploy the clean, validated, enriched production dataset live (GitHub Pages / Cloudflare Pages) with a portfolio README.
9. **[T15] Oblast-Level Rollup & Region Cluster Aggregation**
   - *Goal:* Clicking an oblast marker (e.g., `"Lviv Oblast"`) aggregates everyone born across the entire oblast rather than only centroid fallbacks.
10. **[T21] "Explore Near Me" & Radius-Based Hometown Discovery**
    - *Goal:* Add `10 / 25 / 50 km` device geolocation and pin-drop radius rings so users can explore notable people born near their hometown.
11. **[T10] Brainstorm & Enrich Website with Deeper Biographical Content** + **[T17] Prominence Tiers (UI Toggle & Badges)**
    - *Goal:* Display lifespans (`1814–1861`), short bio summaries, sitelink counts, and the *Remarkable vs. Honorable Mention* filter in `ui.js`.
12. **[T23] City & Oblast Cultural Tapestry Header**
    - *Goal:* Render a compact era, signature-field, and cultural heritage summary header at the top of city and oblast drawers.

---

## Phase 3: Chronology, Geopolitical Context & Biographical Journeys
13. **[T16] Chronology & Historical Era Slider (Granular Years + Global & Ukrainian Eras)**
    - *Goal:* Filter markers by `1y` / `10y` steps and curated Ukrainian & Global historical era presets using `birth_year`.
14. **[T18] Historical Polities & Empires Layer (Territorial Context Overlay)**
    - *Goal:* Add a toggleable color-coded map layer and legend showing the historical state/empire each birthplace belonged to.
15. **[T19] Intense Historical Period Deep-Dive (Year-by-Year & Month-by-Month Explorer)**
    - *Goal:* Build a high-resolution year/month scrubber and context callouts for pivotal eras (e.g., 1648–1657 Khmelnytsky Uprising and 1917–1921 post-Russian Empire conflict).
16. **[T20] Biographical Life Path & Migration Trajectory Explorer (UI)** & **[T22] Contemporaries Strip (UI)**
    - *Goal:* Render lazy-loaded `journeys.json` polylines on Leaflet and interactive "Contemporaries in This City" chips inside `#person-detail-panel`.

---

## Phase 4: Integrated Dashboard, Shareability, Localization & Final Polish
17. **[T14] Integrated Website Statistics Dashboard (`web/stats.html`)**
    - *Goal:* Render interactive charts from `stats_summary.json` with direct "View on Map" links.
18. **[T24] Shareable Deep-Links & "Born Today in History" Pill**
    - *Goal:* Sync active filters, eras, and open panels (`?person=`, `?zone=`, `?era=`) to the URL and add a daily birthday filter pill.
19. **[T26] Full Trilingual Localization (`EN` / `UK` / `RU`) & Missing-Script Fallbacks**
    - *Goal:* Add an `EN | UA | RU` switcher translating all UI labels, categories, and eras while gracefully handling figures missing one or more language editions.
20. **[T11] Visual & UX Polish Pass**
    - *Goal:* Final responsive layout, typography, portrait framing (`T25` UI polish), and dark-mode contrast pass across every drawer and overlay.
21. **[T9] Comprehensive Technical & Data Documentation**
    - *Goal:* Finalize architecture diagrams, data dictionaries, and methodology docs once every v2 pipeline stage and frontend layer is complete.