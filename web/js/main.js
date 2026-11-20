/**
 * ============================================================================
 * main.js — ES Module Entry Point & UI Event Orchestrator
 * ----------------------------------------------------------------------------
 * Bootstraps the application using a two-stage progressive loading strategy
 * and binds global UI event listeners (category dropdowns, dark mode toggle,
 * Escape key handler, panel close buttons, and debounced panel search).
 * ============================================================================
 */

import { state } from './state.js';
import { loadInitialMapData, loadBackgroundData } from './dataLoader.js';
import { map, renderMarkers, setMapTheme } from './map.js';
import { initSearch, debounce } from './search.js';
import {
    updateDropdownCounts,
    closeAllPanels,
    handleEscape,
    applyPanelFiltersAndRender
} from './ui.js';

document.addEventListener('DOMContentLoaded', async () => {
    const mainFilter = document.getElementById('topic-filter');
    const subFilter = document.getElementById('sub-topic-filter');
    const subLabel = document.getElementById('sub-topic-label');

    // ========================================================================
    // TWO-STAGE PROGRESSIVE DATA BOOTSTRAP
    // Stage 1 loads lightweight coordinates (`map_lite.json`) so markers appear
    // on screen immediately. Stage 2 fetches full biographies and search indexes
    // in the background without delaying initial map interactivity.
    // ========================================================================
    try {
        await loadInitialMapData();
        updateDropdownCounts(mainFilter);
        await renderMarkers();

        // Stage 2: Background fetch for detailed popups & autocomplete search
        await loadBackgroundData();
        initSearch();
    } catch (err) {
        console.error('Application startup error:', err);
    }

    // ========================================================================
    // TWO-LEVEL CATEGORY FILTER CONTROLS
    // ========================================================================
    mainFilter.addEventListener('change', e => {
        const selectedMain = e.target.value;
        subFilter.innerHTML = '';

        if (selectedMain !== 'all' && state.subTopicsMap[selectedMain]) {
            subFilter.classList.remove('is-hidden');
            subLabel.classList.remove('is-hidden');

            const subTopics = state.subTopicsMap[selectedMain];

            if (subTopics.length === 2) {
                // Only "All" and 1 real sub-category exist: select and lock it
                const singleSubTopic = subTopics[1];
                const count = state.topicCounts.sub[`${selectedMain}|${singleSubTopic}`] || 0;
                subFilter.add(new Option(`${singleSubTopic} (${count.toLocaleString()})`, singleSubTopic));
                subFilter.disabled = true;
            } else {
                subFilter.disabled = false;
                for (const st of subTopics) {
                    const count = st === 'All'
                        ? (state.topicCounts.main[selectedMain] || 0)
                        : (state.topicCounts.sub[`${selectedMain}|${st}`] || 0);
                    subFilter.add(new Option(`${st} (${count.toLocaleString()})`, st));
                }
                subFilter.value = 'All';
            }
        } else {
            subFilter.classList.add('is-hidden');
            subLabel.classList.add('is-hidden');
            subFilter.disabled = false;
        }

        renderMarkers();
    });

    subFilter.addEventListener('change', () => renderMarkers());

    // ========================================================================
    // THEME TOGGLE (Swaps both CSS tokens and CARTO basemap tiles)
    // ========================================================================
    const themeToggle = document.getElementById('theme-toggle');
    if (themeToggle) {
        themeToggle.addEventListener('click', () => {
            const isDark = document.body.classList.toggle('dark-mode');
            setMapTheme(isDark);
            renderMarkers();
            themeToggle.textContent = isDark ? '☀️ Switch to Light Mode' : '🌙 Switch to Dark Mode';
        });
    }

    // ========================================================================
    // PANEL DISMISSAL (Escape Key & data-close-panel Buttons)
    // ========================================================================
    document.addEventListener('keydown', e => {
        if (e.key === 'Escape') handleEscape(map);
    });

    document.body.addEventListener('click', e => {
        const closeBtn = e.target.closest('[data-close-panel]');
        if (!closeBtn) return;

        const targetPanel = closeBtn.getAttribute('data-close-panel');
        if (targetPanel === 'detail') {
            document.getElementById('person-detail-panel')?.classList.remove('open');
        } else if (targetPanel === 'cluster') {
            closeAllPanels(map);
        }
    });

    // ========================================================================
    // IN-PANEL LIVE FILTER & SORT CONTROLS (Debounced for large clusters)
    // ========================================================================
    const panelSearch = document.getElementById('panel-search');
    const panelSort = document.getElementById('panel-sort');
    if (panelSearch && panelSort) {
        panelSearch.addEventListener('input', debounce(() => applyPanelFiltersAndRender(map), 120));
        panelSort.addEventListener('change', () => applyPanelFiltersAndRender(map));
    }
});