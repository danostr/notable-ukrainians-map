/**
 * ============================================================================
 * dataLoader.js — ETL JSON Adapter & Index Builder
 * ----------------------------------------------------------------------------
 * Isolates all raw JSON tuple parsing (positional arrays from Python ETL) in
 * one place. If `data/search_index.json` is not present on disk, it builds an
 * equivalent in-memory search and location index from `map_lite.json` and
 * `people_details.json` so search works seamlessly out of the box.
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

    let categories = [['Other', 'Unknown']];
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
        if (Number.isFinite(person.lat) && Number.isFinite(person.lon)) {
            state.allPeople.push(person);
            state.peopleById.set(person.id, person);
        }
    }

    state.subTopicsMap = {};
    for (const mainTopic of Object.keys(topicData)) {
        state.subTopicsMap[mainTopic] = ['All', ...Object.keys(topicData[mainTopic])];
    }
}

/**
 * Builds `state.locationsByKey` and `state.searchPeopleIndex` directly from
 * `state.allPeople` and `state.peopleDetails` when `search_index.json` is absent.
 */
function buildSearchIndexFromMemory() {
    state.locationsByKey.clear();
    state.explicitZones = [];
    state.searchPeopleIndex = [];

    for (const person of state.allPeople) {
        const details = state.peopleDetails[person.id] || {};
        const birthplace = (details.birthplace || '').trim();
        const country = person.geoTree.length > 0
            ? person.geoTree[person.geoTree.length - 1]
            : 'Unknown';
        const region = person.geoTree.length >= 2
            ? person.geoTree[person.geoTree.length - 2]
            : '';

        // Reuse existing locKey or synthesize one from rounded coordinates + birthplace
        let locKey = person.locKey;
        if (!locKey && birthplace) {
            locKey = `${person.lat.toFixed(4)}_${person.lon.toFixed(4)}_${birthplace}`;
            person.locKey = locKey;
        }

        if (locKey && birthplace && !state.locationsByKey.has(locKey)) {
            state.locationsByKey.set(locKey, {
                lat: person.lat,
                lon: person.lon,
                region,
                birthplace
            });
        }

        const searchString = [details.name_en, details.name_uk, details.name_ru]
            .filter(Boolean)
            .join(' ')
            .toLowerCase();

        const defaultName = details.name_en || details.name_uk || details.name_ru || 'Unknown Name';

        state.searchPeopleIndex.push({
            id: person.id,
            searchString,
            defaultName,
            locKey,
            birthplace: birthplace || 'Unknown',
            country
        });
    }
}

/**
 * Stage 2 Fetch: Loads `people_details.json` (required) and `search_index.json`
 * (optional—falls back to in-memory indexing if not found).
 */
export async function loadBackgroundData() {
    const detailsRes = await fetch('data/people_details.json');
    if (!detailsRes.ok) {
        throw new Error(`Failed to fetch people_details.json (${detailsRes.status})`);
    }
    state.peopleDetails = (await detailsRes.json()) || {};

    // Attempt to load pre-built search_index.json; synthesize in memory if 404
    try {
        const searchRes = await fetch('data/search_index.json');
        if (!searchRes.ok) {
            console.info('data/search_index.json not found — building search index in memory.');
            buildSearchIndexFromMemory();
            return;
        }

        const searchData = await searchRes.json();
        state.locationsByKey.clear();

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

        state.explicitZones = Array.isArray(rawLocations.zones)
            ? rawLocations.zones
            : (rawLocations.zones && typeof rawLocations.zones === 'object' ? Object.keys(rawLocations.zones) : []);

        const rawPeople = (searchData && Array.isArray(searchData.people)) ? searchData.people : [];
        state.searchPeopleIndex = rawPeople.map(tuple => ({
            id: String(tuple[0]),
            searchString: (tuple[1] || '').toLowerCase(),
            defaultName: tuple[2] || 'Unknown Name',
            locKey: tuple[3] || null,
            birthplace: tuple[4] || 'Unknown',
            country: tuple[5] || 'Unknown'
        }));
    } catch {
        console.info('Building search index in memory from people_details.json.');
        buildSearchIndexFromMemory();
    }
}