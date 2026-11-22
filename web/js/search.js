/**
 * ============================================================================
 * search.js — Multilingual Autocomplete Search (Regions, Cities & People)
 * ----------------------------------------------------------------------------
 * Indexes geographic zones (oblasts/countries), specific birthplaces, and
 * trilingual person names. 
 * ============================================================================
 */

import { state } from './state.js';
import { map, renderMarkers } from './map.js';
import {
    escapeHTML,
    closeAllPanels,
    generatePopupHTML,
    resetPanelControls,
    getLocalizedName,
    getFlyToPadding,
    applyPanelFiltersAndRender
} from './ui.js';

/**
 * Typo-Forgiving Character Deduplication:
 * Collapses consecutive duplicate letters ("kyiiv" -> "kyiv", "shevvchenko" -> "shevchenko")
 * so minor double-keystroke typos still match cleanly.
 */
function squeeze(str) {
    return str.replace(/(.)\1+/g, '$1');
}

/**
 * Delays executing `fn` until `delayMs` milliseconds after the user stops typing.
 */
export function debounce(fn, delayMs = 120) {
    let timer = null;
    return function (...args) {
        clearTimeout(timer);
        timer = setTimeout(() => fn.apply(this, args), delayMs);
    };
}

export function initSearch() {
    const searchInput = document.getElementById('search-input');
    const searchResults = document.getElementById('search-results');

    // Build deduplicated map of searchable Regions / Countries
    const zonesMap = new Map();

    for (const zone of (state.explicitZones || [])) {
        if (zone && typeof zone === 'string') {
            zonesMap.set(zone.trim(), 'Region');
        }
    }

    for (const loc of state.locationsByKey.values()) {
        if (loc.region) {
            zonesMap.set(loc.region.trim(), 'Region');
        }
    }

    for (const person of state.allPeople) {
        if (person.geoTree.length > 0) {
            const country = String(person.geoTree[0]).trim();
            if (country) {
                zonesMap.set(country, 'Country');
            }
            if (person.geoTree.length >= 2) {
                const region = String(person.geoTree[1]).trim();
                if (region && region !== country) {
                    zonesMap.set(region, country);
                }
            }
        }
    }

    const zonesList = Array.from(zonesMap.keys());
    const locationsEntries = Array.from(state.locationsByKey.entries());

    function handleSearchQuery() {
        const query = searchInput.value.toLowerCase().trim();
        searchResults.innerHTML = '';

        // Reset active region filter when the user clears the search box
        if (query.length === 0 && state.activeZoneFilter !== 'all') {
            state.activeZoneFilter = 'all';
            renderMarkers();
            closeAllPanels(map);
            searchResults.style.display = 'none';
            return;
        }

        if (query.length < 2) {
            searchResults.style.display = 'none';
            return;
        }

        const queryWords = query.split(/\s+/);
        const squeezedQueryWords = queryWords.map(squeeze);

        // 1. Match Regions / Oblasts (top 1 match)
        const zoneMatches = zonesList.filter(z => {
            const zLow = z.toLowerCase();
            const zSqueezed = squeeze(zLow);
            return queryWords.every(w => zLow.includes(w)) || squeezedQueryWords.every(w => zSqueezed.includes(w));
        }).slice(0, 1);

        // 2. Match Specific Cities / Birthplaces (top 3 matches)
        const locationMatches = locationsEntries.filter(([, loc]) => {
            if (!loc.birthplace) return false;
            const bpLow = loc.birthplace.toLowerCase();
            const bpSqueezed = squeeze(bpLow);
            return queryWords.every(w => bpLow.includes(w)) || squeezedQueryWords.every(w => bpSqueezed.includes(w));
        }).slice(0, 3);

        // 3. Match Individual People (top 8 matches)
        const peopleMatches = state.searchPeopleIndex.filter(p => {
            if (!p.searchString) return false;
            const squeezedStr = squeeze(p.searchString);
            return queryWords.every(w => p.searchString.includes(w)) || squeezedQueryWords.every(w => squeezedStr.includes(w));
        }).slice(0, 8);

        if (zoneMatches.length === 0 && locationMatches.length === 0 && peopleMatches.length === 0) {
            searchResults.innerHTML = '<div class="search-result-item search-result-meta">No results found...</div>';
            searchResults.style.display = 'block';
            return;
        }

        const fragment = document.createDocumentFragment();

        // --- RENDER REGION MATCHES (🗺️) ---
        for (const zone of zoneMatches) {
            const zoneContext = zonesMap.get(zone) || 'Region';
            const div = document.createElement('div');
            div.className = 'search-result-item';
            div.innerHTML = `🗺️ <strong>${escapeHTML(zone)}</strong> <span class="search-result-meta">(${escapeHTML(zoneContext)})</span>`;

            div.addEventListener('click', () => {
                searchResults.style.display = 'none';
                searchInput.value = zone;
                closeAllPanels(map);
                state.activeZoneFilter = zone;
                renderMarkers();

                const peopleInZone = state.allPeople.filter(p => {
                    if (p.geoTree.includes(zone)) return true;
                    if (p.locKey) {
                        const loc = state.locationsByKey.get(p.locKey);
                        return loc && loc.region === zone;
                    }
                    return false;
                });

                if (peopleInZone.length === 0) return;

                const bounds = L.latLngBounds();
                peopleInZone.forEach(p => bounds.extend([p.lat, p.lon]));

                document.getElementById('panel-title').textContent = zone;
                document.getElementById('panel-subtitle').textContent = `${peopleInZone.length} People Here`;

                state.currentPanelPeople = peopleInZone.map(p => ({
                    id: p.id,
                    categories: p.categories,
                    lat: p.lat,
                    lon: p.lon
                }));

                resetPanelControls();
                applyPanelFiltersAndRender(map);
                document.getElementById('cluster-list-panel').classList.add('open');

                map.flyToBounds(bounds, {
                    paddingTopLeft: getFlyToPadding(),
                    paddingBottomRight: [20, 20],
                    duration: 1.2
                });
            });

            fragment.appendChild(div);
        }

        // --- RENDER CITY / LOCATION MATCHES (📍) ---
        for (const [locKey, loc] of locationMatches) {
            const div = document.createElement('div');
            div.className = 'search-result-item';
            const regionTag = loc.region ? ` <span class="search-result-meta">(${escapeHTML(loc.region)})</span>` : '';
            div.innerHTML = `📍 <strong>${escapeHTML(loc.birthplace)}</strong>${regionTag}`;

            div.addEventListener('click', () => {
                searchResults.style.display = 'none';
                searchInput.value = loc.birthplace;
                closeAllPanels(map);

                const peopleInLocation = state.searchPeopleIndex.filter(p => p.locKey === locKey);

                document.getElementById('panel-title').textContent = loc.birthplace;
                document.getElementById('panel-subtitle').textContent = `${peopleInLocation.length} People Here`;

                // O(1) lookup via state.peopleById instead of O(N) .find() loop
                state.currentPanelPeople = [];
                for (const pMatch of peopleInLocation) {
                    const personData = state.peopleById.get(pMatch.id);
                    if (personData) {
                        state.currentPanelPeople.push({
                            id: personData.id,
                            categories: personData.categories,
                            lat: personData.lat,
                            lon: personData.lon
                        });
                    }
                }

                resetPanelControls();
                applyPanelFiltersAndRender(map);
                document.getElementById('cluster-list-panel').classList.add('open');

                map.flyTo([loc.lat, loc.lon], 12, {
                    paddingTopLeft: getFlyToPadding(),
                    duration: 1.0
                });
            });

            fragment.appendChild(div);
        }

        // --- RENDER INDIVIDUAL PERSON MATCHES (👤) ---
        for (const personMatch of peopleMatches) {
            const details = state.peopleDetails[personMatch.id];
            const displayName = getLocalizedName(details, query, personMatch.defaultName);
            const displayLocation = (
                personMatch.birthplace === personMatch.country ||
                !personMatch.country ||
                personMatch.country === 'Unknown'
            )
                ? personMatch.birthplace
                : `${personMatch.birthplace}, ${personMatch.country}`;

            const div = document.createElement('div');
            div.className = 'search-result-item';
            div.innerHTML = `👤 <strong>${escapeHTML(displayName)}</strong> <span class="search-result-meta">(${escapeHTML(displayLocation)})</span>`;

            div.addEventListener('click', () => {
                searchResults.style.display = 'none';
                searchInput.value = displayName;

                // O(1) lookup via state.peopleById
                const personData = state.peopleById.get(personMatch.id);
                if (!personData) return;

                document.getElementById('detail-name').textContent = displayName;
                const detailContent = document.getElementById('detail-content');
                detailContent.innerHTML = generatePopupHTML({ id: personData.id, categories: personData.categories });
                detailContent.querySelector('.popup-title')?.remove();
                document.getElementById('person-detail-panel').classList.add('open');

                const targetZoom = Math.min(Math.max(map.getZoom(), 12), 13);
                map.flyTo([personData.lat, personData.lon], targetZoom, {
                    paddingTopLeft: getFlyToPadding({ includeDetailPanel: true }),
                    duration: 1.0
                });
            });

            fragment.appendChild(div);
        }

        searchResults.appendChild(fragment);
        searchResults.style.display = 'block';
    }

    searchInput.addEventListener('input', debounce(handleSearchQuery, 120));

    // Close autocomplete dropdown when clicking outside #search-container
    document.addEventListener('click', e => {
        if (!document.getElementById('search-container')?.contains(e.target)) {
            searchResults.style.display = 'none';
        }
    });
}