/**
 * ============================================================================
 * map.js — Leaflet Map, Tile Theme Switcher & Worker Bridge
 * ----------------------------------------------------------------------------
 * Manages the Leaflet map instance, switches CARTO basemap tiles natively for
 * Light/Dark mode, filters `state.allPeople` by active topic/zone, and syncs
 * visible markers with `js/worker.js`.
 * ============================================================================
 */

import { state } from './state.js';
import { generatePopupHTML, closeAllPanels, resetPanelControls, applyPanelFiltersAndRender } from './ui.js';

const OSM_TILE_URL = 'https://tile.openstreetmap.org/{z}/{x}/{y}.png';

// Prevent users from panning infinitely into empty gray void beyond world bounds
const globeBounds = L.latLngBounds(L.latLng(-75, -200), L.latLng(85, 200));

export const map = L.map('map', {
    preferCanvas: true,
    renderer: L.canvas({ padding: 0.5 }), // Matches getBounds().pad(0.5) so markers don't clip during flyTo
    center: [48.3794, 31.1656],
    zoom: 6,
    minZoom: 2.25,
    zoomDelta: 0.6,
    zoomSnap: 0.15,
    wheelPxPerZoomLevel: 60,
    maxBounds: globeBounds,
    maxBoundsViscosity: 1.0
});

L.tileLayer(OSM_TILE_URL, {
    maxZoom: 19,
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
}).addTo(map);

export const markersLayer = L.featureGroup().addTo(map);

/**
 * Kept as a hook for main.js; Dark Mode tile styling is handled via CSS
 * on `.leaflet-tile-pane` in `css/base.css` so only one tile set is needed.
 */
export function setMapTheme() { }

// ============================================================================
// WEB WORKER ASYNC REQUEST/RESPONSE BRIDGE
// ============================================================================
const clusterWorker = new Worker('js/worker.js');
let workerMessageId = 0;
const workerCallbacks = new Map();

clusterWorker.onmessage = function (e) {
    const { id, result, error } = e.data;
    const pending = workerCallbacks.get(id);
    if (!pending) return;

    workerCallbacks.delete(id);
    if (error) {
        pending.reject(new Error(error));
    } else {
        pending.resolve(result);
    }
};

clusterWorker.onerror = function (err) {
    console.error('Cluster Worker fatal error:', err);
    for (const [id, pending] of workerCallbacks.entries()) {
        pending.reject(err);
        workerCallbacks.delete(id);
    }
};

function askWorker(type, payload) {
    return new Promise((resolve, reject) => {
        const id = workerMessageId++;
        workerCallbacks.set(id, { resolve, reject });
        clusterWorker.postMessage({ type, payload, id });
    });
}

// ============================================================================
// FILTERING & CLUSTER RENDERING PIPELINE
// ============================================================================

/**
 * Filters `state.allPeople` by active Main Category, Sub-Category, and Zone,
 * loads the matching GeoJSON points into the Web Worker, and paints the screen.
 */
export async function renderMarkers() {
    const mainTopic = document.getElementById('topic-filter').value || 'all';
    const subTopic = document.getElementById('sub-topic-filter').value || 'All';
    const geoJsonData = [];

    for (const person of state.allPeople) {
        // 1. Category Filter
        let matchesTopic = false;
        if (mainTopic === 'all') {
            matchesTopic = true;
        } else if (subTopic && subTopic !== 'All') {
            matchesTopic = person.categories.some(c => c[0] === mainTopic && c[1] === subTopic);
        } else {
            matchesTopic = person.categories.some(c => c[0] === mainTopic);
        }
        if (!matchesTopic) continue;

        // 2. Region / Zone Filter
        if (state.activeZoneFilter !== 'all') {
            let inZone = person.geoTree.includes(state.activeZoneFilter);
            if (!inZone && person.locKey) {
                const loc = state.locationsByKey.get(person.locKey);
                if (loc && loc.region === state.activeZoneFilter) {
                    inZone = true;
                }
            }
            if (!inZone) continue;
        }

        geoJsonData.push({
            type: 'Feature',
            properties: {
                id: person.id,
                categories: person.categories
            },
            geometry: {
                type: 'Point',
                coordinates: [person.lon, person.lat] // GeoJSON uses [lon, lat] order
            }
        });
    }

    await askWorker('load', { geoJsonData });
    await updateScreen();
}

/**
 * Queries the Web Worker for clusters visible inside the padded viewport
 * and renders either cluster badges or individual Canvas circle markers.
 */
async function updateScreen() {
    const paddedBounds = map.getBounds().pad(0.5);
    const bbox = [
        Math.max(-180, paddedBounds.getWest()),
        Math.max(-90, paddedBounds.getSouth()),
        Math.min(180, paddedBounds.getEast()),
        Math.min(90, paddedBounds.getNorth())
    ];

    const visibleClusters = await askWorker('getClusters', { bbox, zoom: map.getZoom() });
    markersLayer.clearLayers();

    const isDark = document.body.classList.contains('dark-mode');

    for (const feature of visibleClusters) {
        const [lon, lat] = feature.geometry.coordinates;

        if (feature.properties.cluster) {
            const count = feature.properties.point_count;
            const sizeClass = count < 50 ? 'small' : count < 500 ? 'medium' : 'large';

            const clusterIcon = L.divIcon({
                html: `<div><span>${count}</span></div>`,
                className: `marker-cluster marker-cluster-${sizeClass}`,
                iconSize: L.point(40, 40)
            });

            const clusterMarker = L.marker([lat, lon], { icon: clusterIcon });
            clusterMarker.clusterId = feature.properties.cluster_id;
            markersLayer.addLayer(clusterMarker);
        } else {
            const personMarker = L.circleMarker([lat, lon], {
                radius: 6,
                fillColor: isDark ? '#fbbf24' : '#1e40af',
                color: isDark ? '#111827' : '#ffffff',
                weight: 2,
                fillOpacity: 1
            });

            personMarker.personData = feature.properties;
            personMarker.bindPopup(layer => generatePopupHTML(layer.personData));
            markersLayer.addLayer(personMarker);
        }
    }
}

// ============================================================================
// MAP INTERACTION LISTENERS
// ============================================================================

map.on('moveend', () => {
    updateScreen();
});

map.on('click', e => {
    if (!e.originalEvent.target.closest('.leaflet-marker-icon') && !e.originalEvent.target.closest('.leaflet-popup')) {
        closeAllPanels(map);
    }
});

/**
 * Dense-Location Handoff:
 * Clicking a cluster normally zooms in to split it. However, when dozens or
 * hundreds of people share the exact same city coordinates (e.g., Kyiv),
 * `expansionZoom` exceeds maxZoom (19). Instead of zooming into an empty
 * grid, we fetch all leaf records and open the paginated side panel.
 */
markersLayer.on('click', async function (e) {
    const marker = e.layer;
    if (!marker.clusterId) return;

    const clusterId = marker.clusterId;
    const expansionZoom = await askWorker('getClusterExpansionZoom', { clusterId });

    if (expansionZoom > 19) {
        const leaves = await askWorker('getLeaves', { clusterId, limit: Infinity });
        if (!leaves || leaves.length === 0) return;

        const panel = document.getElementById('cluster-list-panel');
        const firstDetails = state.peopleDetails[leaves[0].properties.id] || {};

        document.getElementById('panel-title').textContent = firstDetails.birthplace || 'Location';
        document.getElementById('panel-subtitle').textContent = `${leaves.length} People Here`;

        state.currentPanelPeople = leaves.map(leaf => ({
            id: leaf.properties.id,
            categories: leaf.properties.categories,
            lat: leaf.geometry.coordinates[1],
            lon: leaf.geometry.coordinates[0]
        }));

        resetPanelControls();
        applyPanelFiltersAndRender(map);
        panel.classList.add('open');
    } else {
        map.flyTo(marker.getLatLng(), expansionZoom, { duration: 0.5 });
    }
});