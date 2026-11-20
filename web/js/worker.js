/**
 * ============================================================================
 * worker.js — Off-Main-Thread Spatial Clustering Engine
 * ----------------------------------------------------------------------------
 * Runs inside a dedicated Web Worker thread so spatial R-tree indexing and
 * viewport clustering (Supercluster) never block UI scrolling or animations.
 *
 * Communication Protocol:
 *   Incoming message:  { id: number, type: string, payload: object }
 *   Outgoing message:  { id: number, result: any }
 * ============================================================================
 */

importScripts('https://unpkg.com/supercluster@8.0.1/dist/supercluster.min.js');

let index = null;

self.onmessage = function (e) {
    const { type, payload, id } = e.data;

    try {
        // 1. Build a Supercluster R-tree whenever filters change
        if (type === 'load') {
            index = new Supercluster({
                radius: 180,  // Grouping radius in pixels (keeps map visually clean)
                maxZoom: 13,  // Stop splitting at city level (13) and hand off to side panel
                minPoints: 2  // Minimum points required to form a cluster bubble
            });
            index.load(payload.geoJsonData || []);
            self.postMessage({ id, result: 'loaded' });
            return;
        }

        // Guard: Always reply with an empty fallback if queried before 'load' finishes
        if (!index) {
            const fallback = type === 'getClusterExpansionZoom' ? 13 : [];
            self.postMessage({ id, result: fallback });
            return;
        }

        // 2. Return visible clusters & single points for the current map bounding box
        if (type === 'getClusters') {
            const clusters = index.getClusters(payload.bbox, payload.zoom);
            self.postMessage({ id, result: clusters });
            return;
        }

        // 3. Calculate the zoom level needed to split a clicked cluster apart
        if (type === 'getClusterExpansionZoom') {
            const zoom = index.getClusterExpansionZoom(payload.clusterId);
            self.postMessage({ id, result: zoom });
            return;
        }

        // 4. Retrieve all individual people inside a dense city cluster
        if (type === 'getLeaves') {
            const leaves = index.getLeaves(payload.clusterId, payload.limit || Infinity);
            self.postMessage({ id, result: leaves });
            return;
        }

        // Unknown command fallback
        self.postMessage({ id, result: null });
    } catch (err) {
        self.postMessage({ id, error: err.message || 'Worker execution error' });
    }
};