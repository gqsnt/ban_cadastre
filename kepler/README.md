# Guide de Visualisation Kepler.gl

Ce guide explique comment charger et configurer les données de matching dans Kepler.gl pour valider visuellement la qualité des résultats.

## 1. Installation & Lancement

Ce dossier suppose que vous utilisez une instance locale de Kepler.gl (via un starter kit React ou autre).

### Pré-requis
* Node.js (v18+)
* Les données générées par `export_for_kepler.ps1`

### Commandes
Dans ce dossier `kepler/` :

```bash
# Installation des dépendances
npm install

# Lancement du serveur de développement
npm run dev
````

Ouvrir l’URL indiquée (ex: `http://localhost:5173`) dans votre navigateur.

-----

## 2\. Chargement des Données

Dans l’interface Kepler.gl, cliquez sur le bouton **Add Data** (en haut à gauche) et chargez les 4 fichiers situés dans `data/ban_cadastre/kepler/` :

1.  `kepler_addresses_<DEP>.csv`
2.  `kepler_parcels_<DEP>.csv`
3.  `kepler_links_addr_<DEP>.csv`
4.  `kepler_links_parcel_<DEP>.csv`

-----

## 3\. Configuration des Couches (Cheat Sheet)

Voici la configuration optimale. Créez les couches dans cet ordre pour une bonne superposition.

### A. Couche Parcelles (Fond de carte)

*Permet de voir la couverture cadastrale et la qualité du match par parcelle.*

| Paramètre | Valeur |
| :--- | :--- |
| **Type** | **Polygon** |
| **Source** | `kepler_parcels_<DEP>` |
| **Geometry** | (Détecté auto) `geometry` |
| **Fill Color** | Based on: `parcel_class` (Categorical) |
| **Palette** | Froid (0-100m) vers Chaud (\>1500m) |
| **Stroke** | Fin (0.5), couleur neutre (gris/bleu) |

### B. Couche Liens Adresses (Vérification BAN)

*Vue "Adresse" : Montre où chaque adresse se connecte.*

| Paramètre | Valeur |
| :--- | :--- |
| **Type** | **Line** |
| **Source** | `kepler_links_addr_<DEP>` |
| **Point A** | `addr_lat`, `addr_lon` |
| **Point B** | `parc_lat`, `parc_lon` |
| **Color** | Based on: `class_match` (Categorical) |
| **Opacity** | \~0.4 (faible pour ne pas surcharger) |

### C. Couche Liens Parcelles (Vérification Cadastre)

*Vue "Parcelle" : Montre quelle adresse a été choisie pour chaque parcelle.*

| Paramètre | Valeur |
| :--- | :--- |
| **Type** | **Line** |
| **Source** | `kepler_links_parcel_<DEP>` |
| **Point A** | `parc_lat`, `parc_lon` (Centroïde parcelle) |
| **Point B** | `addr_lat`, `addr_lon` (Adresse cible) |
| **Color** | Based on: `class_match` (Categorical) |
| **Stroke** | **2.0** (Plus épais pour bien voir les erreurs) |
| **Opacity** | 0.8 |

### D. Couche Adresses (Points)

*Les points d'entrée BAN.*

| Paramètre | Valeur |
| :--- | :--- |
| **Type** | **Point** |
| **Source** | `kepler_addresses_<DEP>` |
| **Lat/Lon** | `lat`, `lon` |
| **Color** | Based on: `class_match` (Categorical) |
| **Radius** | 4.0 |

-----
