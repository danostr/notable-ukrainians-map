### 1. Re-fetch all 4 Wikidata slices (born, citizen, heritage, encyclopedia) with new fields
python etl/01_extract.py --refresh

### 2. Apply historical gazetteer, swapped-coord repair, village ranking & refresh place cache
python etl/02_clean_geocode.py --refresh-places

### 3. Re-run occupation taxonomy mapping
python etl/03_categorize.py

### 4. Re-export web JSON bundles (map_lite.json, people_details.json, search_index.json, locations.json)
python etl/04_export_web_json.py

### 5. Validate referential integrity, coordinates, and schema contracts
python etl/05_validate.py