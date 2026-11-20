/**
 * ============================================================================
 * dataLoader.js — ETL JSON Adapter & Index Builder
 * ----------------------------------------------------------------------------
 * Isolates all raw JSON tuple parsing (positional arrays from Python ETL) in
 * one place. If later change the JSON schema produced by ETL pipeline
 * ONLY need to update this file.
 * ============================================================================
 */

import { state } from './state.js';

/**
 * Converts a single raw tuple from `data/map_lite.json` into a clean object.
 * Expected tuple format: [id, lat, lon, categories, locKey, geoTree]
 */
function normalizeLitePerson(raw) {
    const id = String(raw[0]);
    const lat = Number(raw[1]);
    const lon = Number(raw[2]);

    let categories = [["Other", "Unknown"]];
    let locKey = null;
    let geoTree = [];

    if (typeof raw[3] === 'string') {
        // Legacy format fallback: [id, lat, lon, mainCat, subCat, locKey, geoTree]
        categories = [[raw[3], raw[4] || 'Unknown']];
        locKey = typeof raw[5] === 'string' ? raw[5] : null;
        geoTree = Array.isArray(raw[6]) ? raw[6] : [];
    } else {
        if (Array.isArray(raw[3]) && raw[3].length > 0) {
            categories = raw[3];
        }
        locKey = typeof raw[4] === 'string' ? raw[4] : null;
        geoTree = Array.isArray(raw[5]) ? raw[5] : [];
    }

    return { id, lat, lon, categories, locKey, geoTree };
}

/**
 * Stage 1 Fetch: Loads lightweight coordinates + category tree so the map can
 * render markers immediately without waiting for heavy biography texts.
 */
export async function loadInitialMapData() {
    const [liteRes, topicRes] = await Promise.all([
        fetch('data/map_lite.json'),
        fetch('data/topic_map.json')
    ]);

    if (!liteRes.ok || !topicRes.ok) {
        throw new Error(`Failed to fetch initial map data (${liteRes.status} / ${topicRes.status})`);
    }

    const [liteData, topicData] = await Promise.all([
        liteRes.json(),
        topicRes.json()
    ]);

    state.allPeople = [];
    state.peopleById.clear();

    for (const rawPerson of liteData) {
        const person = normalizeLitePerson(rawPerson);
        // Skip corrupt records lacking valid coordinates
        if (Number.isFinite(person.lat) && Number.isFinite(person.lon)) {
            state.allPeople.push(person);
            state.peopleById.set(person.id, person);
        }
    }

    state.subTopicsMap = {};
    for (const mainTopic of Object.keys(topicData)) {
        state.subTopicsMap[mainTopic] = ["All", ...Object.keys(topicData[mainTopic])];
    }
}

/**
 * Stage 2 Fetch: Loads detailed biographies and the pre-lowercased search index
 * asynchronously in the background after initial markers are already on screen.
 */
export async function loadBackgroundData() {
    const [detailsRes, searchRes] = await Promise.all([
        fetch('data/people_details.json'),
        fetch('data/search_index.json')
    ]);

    if (!detailsRes.ok || !searchRes.ok) {
        throw new Error(`Failed to fetch background data (${detailsRes.status} / ${searchRes.status})`);
    }

    const [detailsData, searchData] = await Promise.all([
        detailsRes.json(),
        searchRes.json()
    ]);

    state.peopleDetails = detailsData || {};
    state.locationsByKey.clear();

    // Normalize locations dictionary into an O(1) Map
    const rawLocations = (searchData && searchData.locations) ? searchData.locations : {};
    for (const [key, locTuple] of Object.entries(rawLocations)) {
        if (key === 'zones' || !Array.isArray(locTuple) || locTuple.length < 4) continue;
        state.locationsByKey.set(key, {
            lat: Number(locTuple[0]),
            lon: Number(locTuple[1]),
            region: locTuple[2] || '',
            birthplace: locTuple[3] || ''
        });
    }

    // Preserve explicit zone list if provided by ETL
    state.explicitZones = Array.isArray(rawLocations.zones)
        ? rawLocations.zones
        : (rawLocations.zones && typeof rawLocations.zones === 'object' ? Object.keys(rawLocations.zones) : []);

    // Normalize people search records: [id, searchString, defaultDisplayName, locKey, birthplace, country]
    const rawPeople = (searchData && Array.isArray(searchData.people)) ? searchData.people : [];
    state.searchPeopleIndex = rawPeople.map(tuple => ({
        id: String(tuple[0]),
        searchString: (tuple[1] || '').toLowerCase(),
        defaultName: tuple[2] || 'Unknown Name',
        locKey: tuple[3] || null,
        birthplace: tuple[4] || 'Unknown',
        country: tuple[5] || 'Unknown'
    }));
}