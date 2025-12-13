use anyhow::{Context, Result};
use duckdb::{Config, Connection};
use std::path::Path;

#[allow(dead_code)]
pub struct QaSummary {
    pub total_parcels: i64,
    pub matched_parcels: i64,
    pub coverage_pct: f64,
    pub dist_tier_pcts: Vec<(f64, f64)>,
    pub avg_confidence: f64,
}

fn sql_path(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''")
}

pub fn step_qa(
    dept: &str,
    staging_dir: &Path,
    results_dir: &Path,
    output_dir: &Path,
) -> Result<QaSummary> {
    let matches_path = results_dir.join(format!("matches_{}.parquet", dept));
    let parcel_src = staging_dir.join(format!("parcelles_{}.parquet", dept));
    let address_src = staging_dir.join(format!("adresses_{}.parquet", dept));

    if !matches_path.exists() {
        return Err(anyhow::anyhow!("Matches file not found for {}", dept));
    }
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir)?;
    }

    let config = Config::default();
    let conn = Connection::open_in_memory_with_flags(config).context("Failed to open DuckDB QA")?;

    conn.execute(
        &format!(
            "CREATE VIEW matches AS SELECT * FROM read_parquet('{}')",
            sql_path(&matches_path)
        ),
        [],
    )
    .context("Create view matches")?;

    conn.execute(
        &format!(
            "CREATE VIEW parcels AS SELECT * FROM read_parquet('{}')",
            sql_path(&parcel_src)
        ),
        [],
    )
    .context("Create view parcels")?;

    conn.execute(
        &format!(
            "CREATE VIEW addresses AS SELECT * FROM read_parquet('{}')",
            sql_path(&address_src)
        ),
        [],
    )
    .context("Create view addresses")?;

    // 10.1 Export parcelles_adresses
    let pa_path = output_dir.join(format!("parcelles_adresses_{}.parquet", dept));
    let pa_csv = output_dir.join(format!("parcelles_adresses_{}.csv", dept));

    conn.execute(
        &format!(
            r#"
COPY (
  SELECT *
  FROM matches
  WHERE match_type IS NOT NULL
    AND match_type != 'None'
    AND id_parcelle IS NOT NULL
) TO '{}' (FORMAT 'PARQUET', CODEC 'SNAPPY')
"#,
            sql_path(&pa_path)
        ),
        [],
    )
    .context("Export PA Parquet")?;

    conn.execute(
        &format!(
            r#"
COPY (
  SELECT *
  FROM matches
  WHERE match_type IS NOT NULL
    AND match_type != 'None'
    AND id_parcelle IS NOT NULL
) TO '{}' (FORMAT 'CSV', HEADER)
"#,
            sql_path(&pa_csv)
        ),
        [],
    )
    .context("Export PA CSV")?;

    // 10.2 Distance tiers
    let total_parcels: i64 = conn.query_row("SELECT count(*) FROM parcels", [], |r| r.get(0))?;
    let threshold_tiers: [f64; 7] = [5.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 1500.0];

    conn.execute(
        "CREATE TABLE tiers_res (threshold_m DOUBLE, total_parcels BIGINT, matched_parcels BIGINT, coverage_pct DOUBLE)",
        [],
    )?;

    let mut dist_tier_pcts = Vec::new();
    let mut final_matched_parcels = 0i64;
    for t in threshold_tiers {
        conn.execute(
            &format!(
                r#"
INSERT INTO tiers_res
SELECT
  {t} as threshold_m,
  {tp} as total_parcels,
  count(DISTINCT id_parcelle) as matched_parcels,
  (count(DISTINCT id_parcelle)::DOUBLE / {tp}::DOUBLE * 100.0) as coverage_pct
FROM matches
WHERE id_parcelle IS NOT NULL
  AND (match_type IN ('PreExisting','Inside') OR distance_m <= {t})
"#,
                t = t,
                tp = total_parcels
            ),
            [],
        )
        .context("Tiers calc")?;

        let (matched, pct): (i64, f64) = conn.query_row(
            &format!(
                "SELECT matched_parcels, coverage_pct FROM tiers_res WHERE threshold_m = {}",
                t
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;

        dist_tier_pcts.push((t, pct));
        if t == 1500.0 {
            final_matched_parcels = matched;
        }
    }

    let tiers_csv = output_dir.join(format!("qa_distance_tiers_{}.csv", dept));
    conn.execute(
        &format!(
            "COPY tiers_res TO '{}' (FORMAT 'CSV', HEADER)",
            sql_path(&tiers_csv)
        ),
        [],
    )?;

    // 10.3 QA Precision (distance distribution on best-per-parcel, excluding PreExisting/Inside)
    // Schema: bin, count
    let prec_csv = output_dir.join(format!("qa_precision_{}.csv", dept));
    conn.execute(
        &format!(
            r#"
COPY (
WITH ranked AS (
  SELECT
    id_parcelle,
    id_ban,
    match_type,
    distance_m,
    CASE match_type
      WHEN 'PreExisting' THEN 0
      WHEN 'Inside' THEN 1
      WHEN 'BorderNear' THEN 2
      WHEN 'FallbackNearest' THEN 3
      ELSE 100
    END AS prio,
    ROW_NUMBER() OVER (
      PARTITION BY id_parcelle
      ORDER BY
        CASE match_type
          WHEN 'PreExisting' THEN 0
          WHEN 'Inside' THEN 1
          WHEN 'BorderNear' THEN 2
          WHEN 'FallbackNearest' THEN 3
          ELSE 100
        END ASC,
        distance_m ASC,
        id_ban ASC
    ) AS rn
  FROM matches
  WHERE id_parcelle IS NOT NULL
),
best AS (
  SELECT * FROM ranked WHERE rn = 1
),
binned AS (
  SELECT
    CASE
      WHEN match_type IN ('PreExisting','Inside') THEN NULL
      WHEN distance_m IS NULL THEN NULL
      WHEN distance_m <= 1    THEN '0-1'
      WHEN distance_m <= 2    THEN '1-2'
      WHEN distance_m <= 5    THEN '2-5'
      WHEN distance_m <= 10   THEN '5-10'
      WHEN distance_m <= 15   THEN '10-15'
      WHEN distance_m <= 25   THEN '15-25'
      WHEN distance_m <= 50   THEN '25-50'
      WHEN distance_m <= 100  THEN '50-100'
      WHEN distance_m <= 250  THEN '100-250'
      WHEN distance_m <= 500  THEN '250-500'
      WHEN distance_m <= 1000 THEN '500-1000'
      WHEN distance_m <= 1500 THEN '1000-1500'
      ELSE '>1500'
    END AS bin,
    CASE
      WHEN match_type IN ('PreExisting','Inside') THEN NULL
      WHEN distance_m IS NULL THEN NULL
      WHEN distance_m <= 1    THEN 1
      WHEN distance_m <= 2    THEN 2
      WHEN distance_m <= 5    THEN 3
      WHEN distance_m <= 10   THEN 4
      WHEN distance_m <= 15   THEN 5
      WHEN distance_m <= 25   THEN 6
      WHEN distance_m <= 50   THEN 7
      WHEN distance_m <= 100  THEN 8
      WHEN distance_m <= 250  THEN 9
      WHEN distance_m <= 500  THEN 10
      WHEN distance_m <= 1000 THEN 11
      WHEN distance_m <= 1500 THEN 12
      ELSE 13
    END AS ord
  FROM best
  WHERE match_type NOT IN ('PreExisting','Inside')
)
SELECT bin, COUNT(*) AS count
FROM binned
WHERE bin IS NOT NULL
GROUP BY bin, ord
ORDER BY ord
) TO '{}' (FORMAT 'CSV', HEADER)
"#,
            sql_path(&prec_csv)
        ),
        [],
    )
    .context("QA Precision export")?;

    // 10.4 QA Worst Communes (coverage by code_insee, accepted rule aligned with tiers @1500m)
    // Schema: code_insee, total_parcels, matched_parcels, coverage_pct
    let worst_csv = output_dir.join(format!("qa_worst_communes_{}.csv", dept));
    conn.execute(
        &format!(
            r#"
COPY (
WITH matched AS (
  SELECT DISTINCT id_parcelle
  FROM matches
  WHERE id_parcelle IS NOT NULL
    AND (match_type IN ('PreExisting','Inside') OR distance_m <= 1500.0)
),
tot AS (
  SELECT code_insee, COUNT(*) AS total_parcels
  FROM parcels
  GROUP BY code_insee
),
mat AS (
  SELECT p.code_insee, COUNT(*) AS matched_parcels
  FROM parcels p
  JOIN matched m ON m.id_parcelle = p.id
  GROUP BY p.code_insee
)
SELECT
  t.code_insee,
  t.total_parcels,
  COALESCE(m.matched_parcels, 0) AS matched_parcels,
  (COALESCE(m.matched_parcels, 0)::DOUBLE / t.total_parcels::DOUBLE * 100.0) AS coverage_pct
FROM tot t
LEFT JOIN mat m USING (code_insee)
ORDER BY coverage_pct ASC, total_parcels DESC
) TO '{}' (FORMAT 'CSV', HEADER)
"#,
            sql_path(&worst_csv)
        ),
        [],
    )
    .context("QA Worst communes export")?;

    // QA addresses (sentinel unified to 'None')
    let addr_csv = output_dir.join(format!("qa_addresses_{}.csv", dept));
    conn.execute(
        &format!(
            r#"
COPY (
WITH m_ranked AS (
  SELECT
    id_ban,
    match_type,
    distance_m,
    CASE match_type
      WHEN 'PreExisting' THEN 0
      WHEN 'Inside' THEN 1
      WHEN 'BorderNear' THEN 2
      WHEN 'FallbackNearest' THEN 3
      ELSE 100
    END as priority,
    ROW_NUMBER() OVER (PARTITION BY id_ban ORDER BY
      CASE match_type
        WHEN 'PreExisting' THEN 0
        WHEN 'Inside' THEN 1
        WHEN 'BorderNear' THEN 2
        WHEN 'FallbackNearest' THEN 3
        ELSE 100
      END ASC,
      distance_m ASC
    ) as rn
  FROM matches
),
best_m AS (SELECT * FROM m_ranked WHERE rn = 1),
joined AS (
  SELECT
    a.id as id_ban,
    COALESCE(bm.match_type, 'None') as res_type,
    bm.distance_m as dist
  FROM addresses a
  LEFT JOIN best_m bm ON a.id = bm.id_ban
)
SELECT
  count(*) as total_addresses,
  count(*) FILTER (WHERE res_type != 'None') as matched_addresses,
  count(*) FILTER (WHERE res_type = 'None') as unmatched_addresses,
  (count(*) FILTER (WHERE res_type != 'None')::DOUBLE / count(*)::DOUBLE * 100.0) as coverage_pct,
  count(*) FILTER (WHERE res_type = 'PreExisting') as res_pre,
  count(*) FILTER (WHERE res_type = 'Inside') as res_inside,
  count(*) FILTER (WHERE res_type = 'BorderNear') as res_border_near,
  count(*) FILTER (WHERE res_type = 'FallbackNearest') as res_fallback,
  count(*) FILTER (WHERE res_type = 'None') as res_none,
  count(*) FILTER (WHERE res_type != 'None' AND dist <= 5) as dist_0_5,
  count(*) FILTER (WHERE res_type != 'None' AND dist > 5 AND dist <= 15) as dist_5_15,
  count(*) FILTER (WHERE res_type != 'None' AND dist > 15 AND dist <= 50) as dist_15_50,
  count(*) FILTER (WHERE res_type != 'None' AND dist > 50) as dist_gt_50
FROM joined
) TO '{}' (FORMAT 'CSV', HEADER)
"#,
            sql_path(&addr_csv)
        ),
        [],
    )
    .context("QA Addresses calc")?;

    let avg_conf: f64 = conn
        .query_row(
            r#"
WITH ranked AS (
  SELECT
    id_ban,
    confidence,
    ROW_NUMBER() OVER (
      PARTITION BY id_ban
      ORDER BY
        CASE match_type
          WHEN 'PreExisting' THEN 0
          WHEN 'Inside' THEN 1
          WHEN 'BorderNear' THEN 2
          WHEN 'FallbackNearest' THEN 3
          ELSE 100
        END ASC,
        distance_m ASC
    ) as rn
  FROM matches
  WHERE match_type IS NOT NULL
    AND match_type != 'None'
)
SELECT COALESCE(AVG(confidence), 0.0)
FROM ranked
WHERE rn = 1
"#,
            [],
            |r| r.get(0),
        )
        .context("Average confidence (best-per-address) query")?;

    let coverage_pct = if total_parcels > 0 {
        (final_matched_parcels as f64 / total_parcels as f64) * 100.0
    } else {
        0.0
    };

    Ok(QaSummary {
        total_parcels,
        matched_parcels: final_matched_parcels,
        coverage_pct,
        dist_tier_pcts,
        avg_confidence: avg_conf,
    })
}
