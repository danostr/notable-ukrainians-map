/**
 * ============================================================================
 * ui.js — DOM Rendering, Localization, XSS Escaping & Side Panel Pagination
 * ----------------------------------------------------------------------------
 * Responsible for building safe HTML cards, selecting English vs. Cyrillic
 * names based on user input, calculating responsive map camera offsets, and
 * paginating large location lists in 50-item DOM chunks.
 * ============================================================================
 */

import { state } from './state.js';

/**
 * Escapes HTML special characters so untrusted Wikipedia strings cannot break
 * DOM structure or inject markup when rendered via .innerHTML.
 */
export function escapeHTML(value) {
    if (value === null || value === undefined) return '';
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

/**
 * Ensures external Wikipedia URLs use safe http/https schemes before rendering
 * inside <a href="...">.
 */
function sanitizeWikiUrl(url) {
    if (typeof url !== 'string') return null;
    const trimmed = url.trim();
    return /^https?:\/\//i.test(trimmed) ? escapeHTML(trimmed) : null;
}

/**
 * Computes responsive `paddingTopLeft` offsets for Leaflet `flyTo` / `flyToBounds`
 * so target markers stay centered in the visible map area beside open drawers.
 * Returns [0, 0] on mobile viewports where drawers cover the full width.
 */
export function getFlyToPadding({ includeDetailPanel = false } = {}) {
    if (window.innerWidth <= 768) {
        return [0, 0];
    }
    const listPanelOpen = document.getElementById('cluster-list-panel')?.classList.contains('open');
    const detailPanelOpen = includeDetailPanel || document.getElementById('person-detail-panel')?.classList.contains('open');

    if (detailPanelOpen) return [790, 0]; // 350px list + 20px gap + 400px detail + 20px margin
    if (listPanelOpen) return [370, 0];   // 350px list + 20px margin
    return [0, 0];
}

/**
 * Automatic Cyrillic vs. Latin Script Detection:
 * If the user types in Cyrillic (Ukrainian/Russian alphabet), prioritize
 * `name_uk` -> `name_ru` -> `name_en`. Otherwise default to English first.
 */
export function getLocalizedName(details, query, fallback = 'Unknown Name') {
    if (!details) return fallback;
    const isCyrillic = /[а-яіїєґё]/i.test(query || '');
    if (isCyrillic) {
        return details.name_uk || details.name_ru || details.name_en || fallback;
    }
    return details.name_en || details.name_uk || details.name_ru || fallback;
}

/**
 * Generates the biography card HTML used both inside Leaflet map popups and
 * inside `#person-detail-panel`.
 */
export function generatePopupHTML(basicData) {
    const details = state.peopleDetails[basicData.id];
    if (!details) {
        return `<div class="popup-container"><p class="popup-body">Loading details...</p></div>`;
    }

    const wikiLinks = [
        { url: sanitizeWikiUrl(details.wiki_en), cls: 'btn-en', label: 'EN' },
        { url: sanitizeWikiUrl(details.wiki_uk), cls: 'btn-uk', label: 'UA' },
        { url: sanitizeWikiUrl(details.wiki_ru), cls: 'btn-ru', label: 'RU' }
    ].filter(item => item.url);

    const wikiButtonsHTML = wikiLinks.length > 0
        ? `<div class="wiki-button-container">${wikiLinks.map(w => `<a href="${w.url}" target="_blank" rel="noopener noreferrer" class="wiki-btn ${w.cls}">${w.label}</a>`).join('')}</div>`
        : '';

    const occupationsHTML = (Array.isArray(details.occupations) && details.occupations.length > 0)
        ? `<p class="popup-detail"><b>Occupation:</b> ${escapeHTML(details.occupations.join(', '))}</p>`
        : '';

    const rawName = details.name_en || details.name_uk || details.name_ru || 'Unknown Name';
    const displayName = escapeHTML(rawName);
    const birthplace = escapeHTML(details.birthplace || 'Unknown');

    const firstCategory = (Array.isArray(basicData.categories) && basicData.categories.length > 0)
        ? basicData.categories[0]
        : ['Other', 'Unknown'];
    const categoryText = firstCategory[1] && firstCategory[1] !== 'Unknown'
        ? `${firstCategory[0]} > ${firstCategory[1]}`
        : firstCategory[0];

    const initials = escapeHTML(getInitials(rawName));
    const portraitSrc = buildPortraitSrc(details.image);

    if (!portraitSrc) {
        queueMicrotask(() => hydratePortraitFromWikipedia(basicData.id));
    }

    const portraitHTML = `
        <div class="portrait-frame" data-person-id="${escapeHTML(basicData.id)}">
            <span class="portrait-initials" aria-hidden="true">${initials}</span>
            ${portraitSrc ? `
                <img
                    src="${escapeHTML(portraitSrc)}"
                    alt="${displayName}"
                    class="portrait-img"
                    onload="this.classList.add('is-loaded')"
                    onerror="this.remove()"
                >` : ''}
        </div>
    `;

    return `
        <div class="popup-container">
            <div class="popup-header">
                ${portraitHTML}
                <div class="popup-header__info">
                    <h3 class="popup-title">${displayName}</h3>
                    <p class="popup-detail"><b>Birthplace:</b> ${birthplace}</p>
                    <div><span class="popup-category">${escapeHTML(categoryText)}</span></div>
                </div>
            </div>
            <div class="popup-body">
                ${occupationsHTML}
                ${wikiButtonsHTML}
            </div>
        </div>
    `;
}

const resolvedWikiThumbs = new Map();

function getInitials(name) {
    const parts = String(name || '')
        .trim()
        .split(/\s+/)
        .filter(Boolean);
    if (parts.length === 0) return '?';
    if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
    return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}

function buildPortraitSrc(rawImage) {
    if (typeof rawImage !== 'string') return null;
    const trimmed = rawImage.trim();
    if (!trimmed) return null;
    if (/^https?:\/\//i.test(trimmed)) return trimmed;
    return `https://commons.wikimedia.org/wiki/Special:FilePath/${encodeURIComponent(trimmed)}?width=260`;
}

/**
 * Lazily queries Wikipedia's public REST summary API when Wikidata P18 is missing
 * and caches the thumbnail URL in `state.peopleDetails[personId].image`.
 */
async function hydratePortraitFromWikipedia(personId) {
    if (resolvedWikiThumbs.has(personId)) return;
    resolvedWikiThumbs.set(personId, true);

    const details = state.peopleDetails[personId];
    if (!details) return;

    const candidateUrls = [details.wiki_uk, details.wiki_en, details.wiki_ru].filter(Boolean);
    for (const wikiUrl of candidateUrls) {
        try {
            const parsed = new URL(wikiUrl);
            const title = parsed.pathname.replace(/^\/wiki\//, '');
            if (!title) continue;

            const restUrl = `${parsed.origin}/api/rest_v1/page/summary/${title}`;
            const res = await fetch(restUrl);
            if (!res.ok) continue;

            const summary = await res.json();
            const thumbSrc = summary?.thumbnail?.source;
            if (thumbSrc && /^https:\/\/upload\.wikimedia\.org\//i.test(thumbSrc)) {
                details.image = thumbSrc;
                document
                    .querySelectorAll(`.portrait-frame[data-person-id="${CSS.escape(personId)}"]`)
                    .forEach(frame => {
                        const existingImg = frame.querySelector('img');
                        if (existingImg) {
                            existingImg.src = thumbSrc;
                            existingImg.style.display = 'block';
                        } else {
                            const img = document.createElement('img');
                            img.className = 'portrait-img';
                            img.alt = details.name_en || details.name_uk || 'Portrait';
                            img.onload = () => img.classList.add('is-loaded');
                            img.onerror = () => img.remove();
                            img.src = thumbSrc;
                            frame.appendChild(img);
                        }
                    });
                return;
            }
        } catch {
            // Ignore network/CORS errors and try next language edition
        }
    }
}

export function closeAllPanels(mapInstance) {
    document.getElementById('person-detail-panel')?.classList.remove('open');
    document.getElementById('cluster-list-panel')?.classList.remove('open');
    document.querySelectorAll('.active-pulse').forEach(el => el.classList.remove('active-pulse'));
    if (mapInstance) mapInstance.closePopup();
}

export function handleEscape(mapInstance) {
    const detailPanel = document.getElementById('person-detail-panel');
    const listPanel = document.getElementById('cluster-list-panel');

    // Close the topmost panel first before closing the list panel or popup
    if (detailPanel && detailPanel.classList.contains('open')) {
        detailPanel.classList.remove('open');
        return;
    }

    if (listPanel && listPanel.classList.contains('open')) {
        listPanel.classList.remove('open');
        document.querySelectorAll('.active-pulse').forEach(el => el.classList.remove('active-pulse'));
        return;
    }

    if (mapInstance) mapInstance.closePopup();
}

/**
 * Counts unique people per Main Category and Sub-Category and populates
 * `#topic-filter` with formatted totals.
 */
export function updateDropdownCounts(mainFilter) {
    state.topicCounts = { main: {}, sub: {} };

    for (const person of state.allPeople) {
        const uniqueMains = new Set();
        const uniqueSubs = new Set();

        for (const [main, sub] of person.categories) {
            uniqueMains.add(main);
            uniqueSubs.add(`${main}|${sub}`);
        }

        for (const main of uniqueMains) {
            state.topicCounts.main[main] = (state.topicCounts.main[main] || 0) + 1;
        }
        for (const subKey of uniqueSubs) {
            state.topicCounts.sub[subKey] = (state.topicCounts.sub[subKey] || 0) + 1;
        }
    }

    mainFilter.innerHTML = '';
    mainFilter.add(new Option(`All People (${state.allPeople.length.toLocaleString()})`, 'all'));

    Object.keys(state.subTopicsMap).sort().forEach(topic => {
        if (topic === 'Other') return;
        const count = state.topicCounts.main[topic] || 0;
        if (count > 0) {
            mainFilter.add(new Option(`${topic} (${count.toLocaleString()})`, topic));
        }
    });

    if (state.topicCounts.main['Other']) {
        mainFilter.add(new Option(`Other / Unknown (${state.topicCounts.main['Other'].toLocaleString()})`, 'Other'));
    }
}

// ============================================================================
// PAGINATION & SIDE PANEL ENGINE
// ============================================================================

export function resetPanelControls() {
    const search = document.getElementById('panel-search');
    const sort = document.getElementById('panel-sort');
    if (search) search.value = '';
    if (sort) sort.value = 'remarkability';
}

export function applyPanelFiltersAndRender(mapInstance) {
    const query = (document.getElementById('panel-search')?.value || '').toLowerCase().trim();
    const sortType = document.getElementById('panel-sort')?.value || 'remarkability';

    // 1. Filter in memory across all 3 name scripts
    const filtered = state.currentPanelPeople.filter(person => {
        if (!query) return true;
        const details = state.peopleDetails[person.id] || {};
        return (
            (details.name_en || '').toLowerCase().includes(query) ||
            (details.name_uk || '').toLowerCase().includes(query) ||
            (details.name_ru || '').toLowerCase().includes(query)
        );
    });

    // 2. Sort in memory by Remarkability score (default) or Alphabetical
    filtered.sort((a, b) => {
        const detailsA = state.peopleDetails[a.id] || {};
        const detailsB = state.peopleDetails[b.id] || {};
        if (sortType === 'remarkability') {
            return (detailsB.remarkability || 0) - (detailsA.remarkability || 0);
        }
        const nameA = getLocalizedName(detailsA, query).toLowerCase();
        const nameB = getLocalizedName(detailsB, query).toLowerCase();
        return nameA.localeCompare(nameB);
    });

    // 3. Reset DOM pagination window to the first 50 records
    state.filteredPanelPeople = filtered;
    state.displayCount = state.ITEMS_PER_PAGE;
    renderPanelList(mapInstance, query);
}

/**
 * Client-Side DOM Pagination:
 * Renders only `state.displayCount` (initially 50) DOM elements at a time so
 * opening a city cluster with thousands of people never freezes the browser.
 */
export function renderPanelList(mapInstance, query = '') {
    const listContent = document.getElementById('list-content');
    listContent.innerHTML = '';

    const fragment = document.createDocumentFragment();
    const chunk = state.filteredPanelPeople.slice(0, state.displayCount);

    for (const person of chunk) {
        const details = state.peopleDetails[person.id] || {};
        const displayName = getLocalizedName(details, query);
        const primaryTopic = (person.categories && person.categories.length > 0)
            ? person.categories[0][0]
            : 'Other';

        const item = document.createElement('div');
        item.className = 'list-item';
        item.innerHTML = `
            <div><strong>${escapeHTML(displayName)}</strong></div>
            <div class="list-item__topic">${escapeHTML(primaryTopic)}</div>
        `;

        item.addEventListener('click', () => {
            document.getElementById('detail-name').textContent = displayName;
            const detailContent = document.getElementById('detail-content');
            detailContent.innerHTML = generatePopupHTML({ id: person.id, categories: person.categories });
            detailContent.querySelector('.popup-title')?.remove();
            document.getElementById('person-detail-panel').classList.add('open');

            if (mapInstance && Number.isFinite(person.lat) && Number.isFinite(person.lon)) {
                const cityZoom = Math.min(Math.max(mapInstance.getZoom(), 12), 13);
                mapInstance.flyTo([person.lat, person.lon], cityZoom, {
                    paddingTopLeft: getFlyToPadding({ includeDetailPanel: true }),
                    duration: 0.6
                });
            }
        });

        fragment.appendChild(item);
    }

    // Append "Load more" pagination button if more records remain in memory
    if (state.filteredPanelPeople.length > state.displayCount) {
        const remaining = state.filteredPanelPeople.length - state.displayCount;
        const nextAmount = Math.min(state.ITEMS_PER_PAGE, remaining);

        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'load-more-btn';
        btn.innerHTML = `Load ${nextAmount} more <span style="font-size: 0.8em; opacity: 0.7;">(${remaining} left)</span>`;

        btn.addEventListener('click', () => {
            state.displayCount += state.ITEMS_PER_PAGE;
            renderPanelList(mapInstance, query);
        });

        fragment.appendChild(btn);
    }

    listContent.appendChild(fragment);
}