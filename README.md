# BAN–Cadastre Matcher

Outil en ligne de commande pour rapprocher des **adresses BAN** et des **parcelles cadastrales (Etalab)** à l’échelle d’un département, puis produire des artefacts **QA** et un rapport **national** d’alignement.

Sous-commandes :
- `pipeline` : exécute la chaîne complète (download → prepare → match → QA → aggregate).
- `link` : exécute le matching sur des Parquet déjà préparés (debug / one-shot).
- `analyze` : lit des résultats existants et produit un rapport national (CSV/JSON/Markdown).
- `status` : affiche l’état d’avancement du batch via `batch_state.json`.

---

## 1) Modèle de données et CRS

- CRS de travail : **EPSG:2154 (Lambert-93)**, en mètres.
- Sources :
  - BAN : CSV compressé `adresses-<DEP>.csv.gz`
  - Cadastre Etalab : GeoJSON compressé `cadastre-<DEP>-parcelles.json.gz`

Étape `prepare` :
- Parcelles :
  - lecture GeoJSON via DuckDB spatial
  - nettoyage : `ST_Force2D` → `ST_Transform(OGC:CRS84 → EPSG:2154)` → `ST_MakeValid` → extraction polygones (type 3)
  - export Parquet : `id`, `code_insee`, `geom` en **WKB**
- Adresses :
  - point EPSG:2154 via `x/y` (si présents) sinon reprojection depuis `lon/lat`
  - export Parquet : `id`, `code_insee`, `geom` en **WKB**, `existing_link` (issu de `cad_parcelles`)

Les loaders Rust attendent `geom` en WKB.

---

## 2) Sortie du matcher

Le moteur produit un ensemble de lignes :

`(id_ban, id_parcelle, match_type, distance_m, confidence)`

- `id_parcelle` peut être absent si aucune adresse n’est trouvée dans la limite Step 3 (dans ce cas, aucune ligne n’est produite pour la parcelle).
- Une parcelle peut avoir plusieurs adresses, et une adresse peut matcher une parcelle : le format est **multi-lignes** (many-to-many). Les modules QA/Analyse dérivent ensuite un “best-per-parcel” ou “best-per-address” via un ranking déterministe.

---

## 3) Algorithme de matching (3 étapes)

### Types de match

Priorité logique (utilisée pour “best-per-parcel” et “best-per-address”) :
1. `PreExisting` : liens explicites issus de la BAN (`cad_parcelles`), distance `0`.
2. `Inside` : point adresse inclus dans le polygone (ou sur la frontière), distance `0` (epsilon interne).
3. `BorderNear` : adresse associée à la parcelle la plus proche dans un rayon `address_max_distance_m` (défaut 50 m), avec `0 < d <= threshold`.
4. `FallbackNearest` : pour les parcelles restées sans match après Step 1 + Step 2, associe l’adresse la plus proche sous `fallback_max_distance_m` (défaut 1500 m).
5. `None` : valeur sentinelle utilisée dans certains exports/agrégations ; le matcher n’émet pas de lignes `None` en l’état.

### Step 1 — Parcelle-centric : `PreExisting` + `Inside`

Pour chaque parcelle :
- ajoute les liens `PreExisting` si `existing_link` référence une parcelle existante.
- cherche les adresses dont le point est dans l’enveloppe (R-Tree), puis teste `Inside` (distance au polygone <= epsilon).

Cette étape peut produire **plusieurs matches** pour une même parcelle.

### Step 2 — Address-centric : `BorderNear` (rescue “près de la frontière”)

Pour chaque adresse :
- parcourt les parcelles les plus proches via l’index (R-Tree sur AABB) et s’arrête dès que la distance AABB² dépasse `address_max_distance_m²`.
- calcule la distance exacte point→polygone.
- retient la meilleure parcelle avec la contrainte : `INSIDE_EPS < d <= address_max_distance_m`.

Cette étape peut produire plusieurs adresses vers une même parcelle.

### Step 3 — Parcelle-centric : `FallbackNearest` (uniquement parcelles sans match)

Appliquée uniquement aux parcelles n’ayant **aucun match** après Step 1 + Step 2.

Principe :
- élargissement progressif de l’AABB de la parcelle (`r`), interrogation de **toutes** les adresses dans cette fenêtre via R-Tree, évaluation distance exacte point→polygone.
- chaque adresse candidate n’est évaluée qu’une fois (`seen`).
- **pruning** : si `distance(point, AABB(parcel))` ne peut pas battre `best_dist`, on évite le calcul distance point→polygone.
- arrêt garanti : dès que `best_dist <= r`, aucune adresse hors de la fenêtre courante ne peut faire mieux.
- rejet dur : ignore toute adresse à `d > fallback_max_distance_m`.
- tie-break déterministe à égalité de distance : `addr.id` lexicographique.

Cas particulier :
- si Step 3 trouve `best_dist <= INSIDE_EPS`, le match est émis en `Inside` (distance 0), pas `FallbackNearest`.

---

## 4) Confidence (déterministe)

Score affecté au moment de l’émission de la ligne :
- `PreExisting` = 100
- `Inside` = 90
- `BorderNear` = 80 si `< 5 m`, sinon 70
- `FallbackNearest` = 50
- `None` = 0

---

## 5) Prérequis

- Rust (stable) pour compiler.
- DuckDB CLI (`duckdb`) disponible dans le `PATH` :
  - requis pour `prepare`, `qa`, `aggregate`
  - requis pour `export_kepler.ps1` (DuckDB + extension `spatial`)
- Accès réseau requis pour `pipeline` (téléchargements, et `INSTALL spatial` DuckDB si non déjà disponible).

Compilation :
```bash
cargo build --release
````

Binaire :

* `target/release/ban-cadastre`

---

## 6) Utilisation CLI

### 6.1 Pipeline complet

```bash
cargo run --release -- pipeline \
  --departments-file data/departements.csv \
  --departments 69 \
  --data-dir data/ban_cadastre
```

Options :

* `--resume` : reprend via `batch_state.json` (ignore les départements déjà complétés).
* `--force` : force download + régénération staging.
* `--quick-qa` : saute `match` si `matches_<DEP>.parquet` existe.
* `--filter-commune <CODE_INSEE>` : filtre adresses/parcelles (match uniquement).
* `--limit-addresses <N>` : tronque les adresses (debug/perf).
* `--strict` : code retour `2` en cas d’exécution partielle.

### 6.2 Link (one-shot sur Parquet préparés)

```bash
cargo run --release -- link \
  --addresses data/ban_cadastre/staging/adresses_69.parquet \
  --parcels   data/ban_cadastre/staging/parcelles_69.parquet \
  --output    data/ban_cadastre/batch_results/matches_69.parquet \
  --distance-threshold 50
```

Options :

* `--distance-threshold` : rayon Step 2 (`BorderNear`) en mètres.
* `--batch-size` : flush Parquet.
* `--filter-commune`, `--limit-addresses` : debug.

### 6.3 QA / Analyse nationale

QA départementale est exécutée par `pipeline` (étape `qa`) et produit les fichiers dans `output/`.

Analyse nationale :

```bash
cargo run --release -- analyze \
  --results-dir data/ban_cadastre \
  --departments-file data/departements.csv
```

Définitions (alignées code) :

* **Accepted coverage (QA-aligned)** : meilleur match par parcelle ∈ {`PreExisting`, `Inside`} **ou** `distance_m <= 1500`.
* **Best-match coverage** : existence d’un meilleur match par parcelle (hors `None`) sans seuil.
* **Delta** : Best-match – Accepted.

`--strict` : code retour `2` si inputs incomplets (matches/parcels manquants).

### 6.4 Status

```bash
cargo run --release -- status --data-dir data/ban_cadastre
```

---

## 7) Arborescence et artefacts

Arborescence typique dans `--data-dir` :

```text
data/ban_cadastre/
  raw/            # sources décompressées (.json / .csv) + archives .gz
  staging/        # Parquet EPSG:2154 (geom=WKB)
  batch_results/  # matches_<DEP>.parquet
  output/         # QA + agrégations
  batch_state.json
```

Artefacts QA par département (`output/`) :

* `parcelles_adresses_<DEP>.parquet`
* `parcelles_adresses_<DEP>.csv`
* `qa_distance_tiers_<DEP>.csv`
* `qa_precision_<DEP>.csv`
* `qa_worst_communes_<DEP>.csv`
* `qa_addresses_<DEP>.csv`

Artefacts nationaux (`output/`) si présents :

* `france_parcelles_adresses.parquet` et `france_parcelles_adresses.csv` (union des `parcelles_adresses_*.parquet`)
* `national_qa_distance_tiers.csv`
* `national_qa_precision.csv`
* `national_worst_communes_top100.csv`

---

## 8) Export Kepler (debug visuel)

Script : `export_kepler.ps1`

Entrées :

* `batch_results/matches_<DEP>.parquet`
* `staging/parcelles_<DEP>.parquet`
* `staging/adresses_<DEP>.parquet`

Sorties (dans `kepler/`) :

* `kepler_addresses_<DEP>.csv` (points)
* `kepler_parcels_<DEP>.csv` (polygones en GeoJSON)
* `kepler_links_addr_<DEP>.csv` (segments best-per-address)
* `kepler_links_parcel_<DEP>.csv` (segments best-per-parcel)

Classification :

* `kepler_links_addr_*` utilise `addr_band(...)`
* `kepler_links_parcel_*` utilise `parcel_band(...)` (distance bands orientés “parcelle”)

---

## 9) Codes retour

* `0` : succès.
* `1` : erreur bloquante (I/O, DuckDB, etc.).
* `2` : exécution partielle avec `--strict`

    * `pipeline` : au moins un département échoué ou agrégation partielle
    * `analyze` : données manquantes détectées

