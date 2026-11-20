/**
 * ============================================================================
 * state.js — Centralized Application State Store
 * ----------------------------------------------------------------------------
 * Single source of truth shared across all ES modules.
 * ============================================================================
 */
export const state = {
    // --- Core Dataset (Populated by js/dataLoader.js) ---
    allPeople: [],              // Array of normalized Person objects from map_lite.json
    peopleById: new Map(),      // personId -> normalized Person object
    peopleDetails: {},          // Dictionary: personId -> full biography & wiki URLs
    locationsByKey: new Map(),  // locKey -> { lat, lon, region, birthplace }
    searchPeopleIndex: [],      // Pre-lowercased multilingual search tuples

    // --- Category Filter Taxonomy ---
    subTopicsMap: {},           // Map: mainTopic -> ["All", ...subTopics]
    topicCounts: {              // Precomputed counts shown inside filter <select> options
        main: {},
        sub: {}
    },
    activeZoneFilter: 'all',    // Currently selected region/oblast filter (or 'all')

    // --- Side Panel & Client-Side Pagination State ---
    currentPanelPeople: [],     // Raw list of people inside the currently open cluster/zone
    filteredPanelPeople: [],    // Subset after applying #panel-search and #panel-sort
    displayCount: 50,           // Number of DOM nodes currently rendered in #list-content
    ITEMS_PER_PAGE: 50          // Chunk size added each time "Load more" is clicked
};