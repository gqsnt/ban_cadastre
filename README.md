# BAN–Cadastre Matcher

Outil de rapprochement automatique à haute performance entre les adresses BAN et les parcelles cadastrales françaises.

Le projet :
- **Télécharge** les données publiques BAN et Cadastre (Etalab) par département.
- **Prépare** les géométries dans un système de coordonnées projeté cohérent (EPSG:2154 – Lambert-93).
- **Associe** chaque **parcelle** à au moins une **adresse** (via une stratégie en 3 étapes).
- **Produit** des fichiers d’analyse (CSV/Parquet) et des exports prêts pour **Kepler.gl**.

**Public visé :** Développeurs, Data Scientists en immobilier ou géomatique. Pas besoin de connaître Rust pour exploiter les résultats (CSV/Parquet).

---

## 1. Fonctionnement de l'Algorithme

Pour chaque couple adresse / parcelle, l’algorithme cherche le lien le plus pertinent selon cet ordre de priorité strict :

1.  **`PreExisting` (Priorité 0)**
    * Liens explicites déjà présents dans la BAN (colonne `cad_parcelles`).
2.  **`Inside` (Priorité 1)**
    * L'adresse est géométriquement incluse dans le polygone de la parcelle.
3.  **`BorderNear` (Priorité 2)**
    * L'adresse est située à moins de X mètres (défaut 50m) du bord de la parcelle.
    * *Optimisation :* Recherche via index spatial R-Tree.
4.  **`FallbackNearest` (Priorité 3)**
    * Pour les parcelles n'ayant **aucun** match précédent.
    * L'algorithme cherche les $k$ adresses les plus proches du centroïde, puis sélectionne celle qui est la plus proche de la **bordure réelle** du polygone.
    * *Avantage :* Gère correctement les parcelles en forme de L ou de U.

> **Note :** L'objectif est qu'aucune parcelle valide ne soit laissée sans adresse, afin de garantir une couverture de 100% pour les usages statistiques.

---

## 2. Le Pipeline de Données

Pour chaque département traité (ex: `69`), le pipeline exécute :

1.  **Download** : Récupération des `.csv.gz` (BAN) et `.json.gz` (Cadastre).
2.  **Prepare** : Nettoyage et reprojection (WGS84 -> Lambert-93) via DuckDB spatial. Stockage en Parquet.
3.  **Match** : Exécution du binaire Rust (parallélisé). Production de `matches_69.parquet`.
4.  **QA (Qualité)** : Génération des rapports d'erreurs et statistiques.

### Fichiers de sortie (`data/ban_cadastre/output/`)

Pour chaque département :
- `parcelles_adresses_XX.parquet` / `.csv` : La liste finale des liens.
- `qa_distance_tiers_XX.csv` : Taux de couverture par seuils (100m, ..., 1500m).
- `qa_precision_XX.csv` : Distribution fine des distances pour les matchs de bordure.
- `qa_worst_communes_XX.csv` : Top des communes les moins bien couvertes.
- `qa_addresses_XX.csv` : Synthèse pivotée par adresse.

---

## 3. Installation & Prérequis

### Logiciels
* **Rust** (Stable) : Pour compiler le moteur de matching.
    * `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
* **DuckDB** (CLI) : Doit être accessible dans le `PATH`.
    * Utilisé pour la préparation rapide des données géographiques.
* **Node.js** (Optionnel) : Uniquement pour l'interface de visualisation Kepler.gl.

### Compilation
```bash
cargo build --release
````

Le binaire sera disponible dans `target/release/ban-cadastre`.

-----

## 4\. Utilisation

### 4.1. Pipeline complet (Recommandé)

Traite un département de A à Z (téléchargement -\> matching -\> QA).

```bash
# Exemple pour le Rhône (69)
cargo run --release -- pipeline \
  --departments-file data/departements.csv \
  --departments 69 \
  --data-dir data/ban_cadastre
```

* `--resume` : Reprend là où le traitement s'est arrêté (évite de tout refaire en cas de crash).
* `--force` : Force le retéléchargement et le re-calcul complet.

### 4.2. Analyse Nationale

Une fois plusieurs départements traités, générez un rapport global :

```bash
cargo run --release -- analyze \
  --results-dir data/ban_cadastre \
  --departments-file data/departements.csv
```

Cela produit `analysis_report.md` (Markdown) contenant les taux de couverture par région et les indicateurs de confiance globaux.

-----

## 5\. Visualisation (Kepler.gl)

Le projet inclut un outillage complet pour visualiser les résultats sur une carte interactive.

1.  Générer les fichiers pour Kepler (via script PowerShell).
2.  Lancer l'interface web locale.

👉 **[Voir le guide complet dans kepler/README.md](https://github.com/gqsnt/ban_cadastre/blob/master/kepler/README.md)**

-----

## 6\. Structure des Dossiers

```text
data/
  ban_cadastre/
    raw/            # Fichiers sources (.gz)
    staging/        # Fichiers intermédiaires nettoyés (.parquet)
    batch_results/  # Résultats bruts du matching (.parquet)
    output/         # Exports finaux CSV/Parquet et fichiers QA
    kepler/         # Fichiers générés spécifiquement pour la viz
```
