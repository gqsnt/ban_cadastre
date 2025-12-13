# Kepler.gl – Visualisation (optionnel)

L’export Kepler convertit les artefacts internes (WKB EPSG:2154 + matches) vers des CSV compatibles Kepler.gl :
- points en WGS84 (`lon`, `lat`),
- polygones en GeoJSON WGS84 (`geometry`),
- liens sous forme de segments A→B (colonnes `*_lon`, `*_lat`).

---

## 1) Prérequis

- DuckDB CLI accessible (ex: `duckdb`) avec l’extension `spatial` (installée automatiquement par les scripts).
- Un département `<DEP>` déjà traité par la commande `pipeline` (génère `staging/*` et `batch_results/*`).

Entrées utilisées :
- `data/ban_cadastre/batch_results/matches_<DEP>.parquet`
- `data/ban_cadastre/staging/parcelles_<DEP>.parquet`
- `data/ban_cadastre/staging/adresses_<DEP>.parquet`

---

## 2) Génération des fichiers Kepler

### Windows (PowerShell)

```powershell
# Depuis la racine du repo
powershell -ExecutionPolicy Bypass -File scripts/export_kepler.ps1 -Dept 69 -DataDir data/ban_cadastre -DuckdbExe duckdb
````

### Linux/macOS (Bash)

```bash
# Depuis la racine du repo
chmod +x scripts/export_kepler.sh
./scripts/export_kepler.sh --dept 69 --data-dir data/ban_cadastre --duckdb duckdb
```

Sorties générées dans `data/ban_cadastre/kepler/` :

1. `kepler_addresses_<DEP>.csv`
2. `kepler_parcels_<DEP>.csv`
3. `kepler_links_addr_<DEP>.csv`
4. `kepler_links_parcel_<DEP>.csv`

---

## 3) Schéma des exports

### 3.1 `kepler_addresses_<DEP>.csv` (Points)

Colonnes clés :

* `lon`, `lat`
* `class_match` ∈ {`UNMATCHED`, `Inside`, `0-5`, `5-15`, `15-50`, `>50`}
* `match_type`, `distance_m`, `confidence`

### 3.2 `kepler_parcels_<DEP>.csv` (Polygones)

Colonnes clés :

* `geometry` (GeoJSON)
* `parcel_class` ∈ {`UNMATCHED`, `Inside`, `0-100`, `100-250`, `250-500`, `500-1000`, `1000-1500`, `>1500`}
* `match_type`, `distance_m`, `confidence`

### 3.3 `kepler_links_addr_<DEP>.csv` (Lignes Adresse → Centroïde parcelle)

Colonnes clés :

* `addr_lon`, `addr_lat`
* `parc_lon`, `parc_lat`
* `class_match`, `match_type`, `distance_m`, `confidence`

### 3.4 `kepler_links_parcel_<DEP>.csv` (Lignes Centroïde parcelle → Adresse)

Colonnes clés :

* `parc_lon`, `parc_lat`
* `addr_lon`, `addr_lat`
* `class_match`, `match_type`, `distance_m`, `confidence`

---

## 4) Chargement dans Kepler.gl

1. `Add Data` → importer les 4 CSV.
2. Créer les couches dans l’ordre suivant.

### A) Parcelles (Polygon)

* Type : `Polygon`
* Source : `kepler_parcels_<DEP>`
* Geometry : colonne `geometry`
* Color : `parcel_class` (categorical)

### B) Liens Adresse → Parcelle (Line)

* Type : `Line`
* Source : `kepler_links_addr_<DEP>`
* Point A : `addr_lat` / `addr_lon`
* Point B : `parc_lat` / `parc_lon`
* Color : `class_match` (categorical)

### C) Liens Parcelle → Adresse (Line)

* Type : `Line`
* Source : `kepler_links_parcel_<DEP>`
* Point A : `parc_lat` / `parc_lon`
* Point B : `addr_lat` / `addr_lon`
* Color : `class_match` (categorical)

### D) Adresses (Point)

* Type : `Point`
* Source : `kepler_addresses_<DEP>`
* Lat/Lon : `lat` / `lon`
* Color : `class_match` (categorical)

