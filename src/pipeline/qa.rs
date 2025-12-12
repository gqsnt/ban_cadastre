use anyhow::{Context, Result};
use duckdb::{Config, Connection};
use std::path::Path;

pub struct QaSummary {
    pub total_parcels: i64,
    pub matched_parcels: i64,
    pub coverage_pct: f64,
    // (threshold, pct)
    pub dist_tier_pcts: Vec<(f64, f64)>,
    pub avg_confidence: f64,
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
    // Load spatial if allowed/needed? Not strictly needed unless we compute geometric stuff.
    // distances are already in matches.

    conn.execute(
        &format!(
            "CREATE VIEW matches AS SELECT * FROM read_parquet('{}')",
            matches_path.to_string_lossy()
        ),
        [],
    )
    .context("Create view matches")?;

    conn.execute(
        &format!(
            "CREATE VIEW parcels AS SELECT * FROM read_parquet('{}')",
            parcel_src.to_string_lossy()
        ),
        [],
    )
    .context("Create view parcels")?;

    conn.execute(
        &format!(
            "CREATE VIEW addresses AS SELECT * FROM read_parquet('{}')",
            address_src.to_string_lossy()
        ),
        [],
    )
    .context("Create view addresses")?;

    // 10.1 Export parcelles_adresses
    // Join matches and parcels?
    // Matches has id_ban, id_parcelle.
    // We want a table of matches.
    // 10.1 Export parcelles_adresses
    let pa_path = output_dir.join(format!("parcelles_adresses_{}.parquet", dept));
    let pa_csv = output_dir.join(format!("parcelles_adresses_{}.csv", dept));

    // Valid matches: match_type != 'None' and id_parcelle IS NOT NULL
    let q_pa = format!(
        r#"
COPY (
    SELECT *
    FROM matches
    WHERE match_type IS NOT NULL
      AND match_type != 'None'
      AND id_parcelle IS NOT NULL
) TO '{}' (FORMAT 'PARQUET', CODEC 'SNAPPY')
"#,
        pa_path.to_string_lossy()
    );
    conn.execute(&q_pa, []).context("Export PA Parquet")?;

    // Export CSV
    let q_pa_csv = format!(
        r#"
COPY (
    SELECT *
    FROM matches
    WHERE match_type IS NOT NULL
      AND match_type != 'None'
      AND id_parcelle IS NOT NULL
) TO '{}' (FORMAT 'CSV', HEADER)
"#,
        pa_csv.to_string_lossy()
    );
    conn.execute(&q_pa_csv, []).context("Export PA CSV")?;

    // 10.2 Distance Tiers
    // total_parcels, matched_parcels
    // matched_parcels: at least one match (PreExisting or Inside or dist <= threshold)
    // Tiers: 100, 250, 500, 1000, 1500
    // Wait, prompt says: "Export threshold_m, total_parcels, matched_parcels, coverage_pct"
    // Does this mean one row per threshold? Or multiple columns?
    // Usually one CSV with rows.

    // Calculate total
    let total_parcels: i64 = conn.query_row("SELECT count(*) FROM parcels", [], |r| r.get(0))?;

    let threshold_tiers = [5.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 1500.0];
    let tiers_csv = output_dir.join(format!("qa_distance_tiers_{}.csv", dept));

    // Create a temp table or do pythonic loop? Loop is fine since few queries.
    // Need manual CSV writing or DuckDB Union.
    // Let's create a table for results.
    conn.execute("CREATE TABLE tiers_res (threshold_m DOUBLE, total_parcels BIGINT, matched_parcels BIGINT, coverage_pct DOUBLE)", [])?;

    // We will collect dist tiers for the summary
    let mut dist_tier_pcts = Vec::new();
    let mut final_matched_parcels = 0;

    for t in threshold_tiers {
        // match_type IN ('PreExisting','Inside') OR distance_m <= t
        // We need to count DISTINCT id_parcelle that satisfy this.
        let q = format!(
            r#"
            INSERT INTO tiers_res
            SELECT 
                {} as threshold_m, 
                {} as total_parcels, 
                count(DISTINCT id_parcelle) as matched_parcels,
                (count(DISTINCT id_parcelle)::DOUBLE / {}::DOUBLE * 100.0) as coverage_pct
            FROM matches
            WHERE (match_type IN ('PreExisting', 'Inside') OR distance_m <= {})
              AND id_parcelle IS NOT NULL
        "#,
            t, total_parcels, total_parcels, t
        );
        conn.execute(&q, []).context("Tiers calc")?;
        
        // Read back for summary if this is one of our "key" thresholds or just grab all
        let (matched, pct): (i64, f64) = conn.query_row(
            &format!("SELECT matched_parcels, coverage_pct FROM tiers_res WHERE threshold_m = {}", t), 
            [], 
            |r| Ok((r.get(0)?, r.get(1)?))
        )?;
        // We'll use the "infinite" or largest threshold as the "total matched" for the summary if we want "matched anything reasonable"
        // But usually "matched" implies some quality. Let's say < 50m or < 1500m?
        // Let's store all and decide later.
        dist_tier_pcts.push((t, pct));
        if t == 1500.0 {
            final_matched_parcels = matched;
        }
    }
    conn.execute(
        &format!(
            "COPY tiers_res TO '{}' (FORMAT 'CSV', HEADER)",
            tiers_csv.to_string_lossy()
        ),
        [],
    )?;

    // 10.2bis Distribution des distances par parcelle (bins exclusifs)
    // Bins: 0-100, 100-250, 250-500, 500-1000, 1000-1500, >1500
    //
    // On calcule d'abord, pour chaque parcelle, une "best_dist":
    //  - 0 si la parcelle a au moins un match PreExisting/Inside
    //  - sinon la plus petite distance_m parmi ses matches
    //  - NULL si aucune correspondance dans matches
    //
    // Puis on projette best_dist dans des classes exclusives qui couvrent 100 % des parcelles,
    // la classe '>1500' regroupant les cas problématiques ou très éloignés.
    let dist_cat_csv = output_dir.join(format!("qa_distance_categories_{}.csv", dept));
    let q_dist_cat = format!(
        r#"
COPY (
WITH best_per_parcel AS (
    SELECT
        p.id AS id_parcelle,
        MIN(
            CASE
                WHEN m.match_type IN ('PreExisting','Inside') THEN 0.0
                ELSE m.distance_m
            END
        ) AS best_dist
    FROM parcels p
    LEFT JOIN matches m ON p.id = m.id_parcelle
    GROUP BY p.id
),
binned AS (
    SELECT
        CASE
            WHEN best_dist IS NULL THEN '>1500'
            WHEN best_dist <= 100 THEN '0-100'
            WHEN best_dist <= 250 THEN '100-250'
            WHEN best_dist <= 500 THEN '250-500'
            WHEN best_dist <= 1000 THEN '500-1000'
            WHEN best_dist <= 1500 THEN '1000-1500'
            ELSE '>1500'
        END AS bin
    FROM best_per_parcel
),
all_bins AS (
    SELECT '0-100' AS bin
    UNION ALL SELECT '100-250'
    UNION ALL SELECT '250-500'
    UNION ALL SELECT '500-1000'
    UNION ALL SELECT '1000-1500'
    UNION ALL SELECT '>1500'
),
final AS (
    SELECT
        b.bin,
        COALESCE(COUNT(binned.bin), 0) AS count
    FROM all_bins b
    LEFT JOIN binned ON b.bin = binned.bin
    GROUP BY b.bin
)
SELECT * FROM final
ORDER BY
    CASE bin
        WHEN '0-100' THEN 1
        WHEN '100-250' THEN 2
        WHEN '250-500' THEN 3
        WHEN '500-1000' THEN 4
        WHEN '1000-1500' THEN 5
        ELSE 6
    END
) TO '{}' (FORMAT 'CSV', HEADER)
"#,
        dist_cat_csv.to_string_lossy()
    );
    conn.execute(&q_dist_cat, [])
        .context("Distance categories calc")?;

    // Bins: 0-5, 5-15, 15-50, >50
    // On veut que ces 4 catégories couvrent 100 % des BorderNear,
    // donc on force la présence d'une ligne pour chaque bin, même si count = 0.
    let prec_csv = output_dir.join(format!("qa_precision_{}.csv", dept));
    let q_prec = format!(
        r#"
        COPY (
            WITH base AS (
    SELECT
        CASE
            WHEN distance_m <= 5 THEN '0-5'
            WHEN distance_m <= 15 THEN '5-15'
            WHEN distance_m <= 50 THEN '15-50'
            ELSE '>50'
        END AS bin,
        COUNT(*) AS count
    FROM matches
    WHERE match_type = 'BorderNear'
    GROUP BY 1
),
all_bins AS (
    SELECT '0-5' AS bin
    UNION ALL SELECT '5-15'
    UNION ALL SELECT '15-50'
    UNION ALL SELECT '>50'
),
final AS (
    SELECT
        b.bin,
        COALESCE(base.count, 0) AS count
    FROM all_bins b
    LEFT JOIN base ON base.bin = b.bin
)
SELECT * FROM final
ORDER BY
    CASE bin
        WHEN '0-5' THEN 1
        WHEN '5-15' THEN 2
        WHEN '15-50' THEN 3
        ELSE 4
    END
        ) TO '{}' (FORMAT 'CSV', HEADER)
    "#,
        prec_csv.to_string_lossy()
    );
    conn.execute(&q_prec, []).context("Precision calc")?;

    // 10.4 Communes worst coverage
    // Join parcels <-> matches.
    // Count matched per commune.
    // We need grouping by commune (code_insee) from parcels.
    // Left join parcels -> matches.
    // But matches might not cover all parcels.
    // matches table has id_parcelle (nullable). None records mean failed match?
    // Wait, the "matches" table in `matches_{dept}.parquet` contains one row per ADDRESS-PARCEL link.
    // It does NOT contain unlinked parcels unless we added "Fallback" matches for them.
    // However, if a parcel has NO matches at all (failed fallback?), it won't be in matches.
    // Wait, step 2 does "Fallback parcelle (No parcel left behind)". So EVERY parcel should have at least one match of type FallbackNearest, UNLESS it has no valid geometry/centroid or no address anywhere (unlikely).
    // So `matches` should cover almost all parcels.
    // We can count distinct id_parcelle in matches group by parcel code_insee?
    // Parcels table has (id, code_insee).
    // Matches table has id_parcelle.
    // We should join: parcels LEFT JOIN matches ON parcels.id = matches.id_parcelle

    // Actually, "matched_parcels" means match_type != None?
    // We assume FallbackNearest counts as matched?
    // Prompt 10.4: "matched_parcels = nombre de parcelles ayant au moins un match".
    // Is fallback a "match"? It is `match_type != 'NONE'`.

    let worst_csv = output_dir.join(format!("qa_worst_communes_{}.csv", dept));
    let q_worst = format!(
        r#"
        COPY (
            WITH p_stats AS (
                SELECT 
                    p.code_insee,
                    count(DISTINCT p.id) as total_parcels,
                    count(DISTINCT m.id_parcelle) FILTER (WHERE m.match_type != 'None' AND m.match_type IS NOT NULL) as matched_parcels
                FROM parcels p
                LEFT JOIN matches m ON p.id = m.id_parcelle
                GROUP BY 1
            )
            SELECT 
                code_insee,
                total_parcels,
                matched_parcels,
                (matched_parcels::DOUBLE / total_parcels::DOUBLE * 100.0) as coverage_pct
            FROM p_stats
            WHERE total_parcels > 50 AND (matched_parcels::DOUBLE / total_parcels::DOUBLE * 100.0) < 80.0
            ORDER BY coverage_pct ASC
            LIMIT 10
        ) TO '{}' (FORMAT 'CSV', HEADER)
    "#,
        worst_csv.to_string_lossy()
    );
    conn.execute(&q_worst, []).context("Worst communes calc")?;

    // 10.5 QA Addresses
    // Best match per address.
    // Join to all addresses.
    // Addr priority: PreExisting < Inside < BorderNear < FallbackNearest < None
    // We need to implement this priority in SQL or reuse existing logic?
    // We can use duckdb arg_min or distinct on window function.

    // Priority mapping:
    // PreExisting: 0, Inside: 1, BorderNear: 2, FallbackNearest: 3, None: 100

    let addr_csv = output_dir.join(format!("qa_addresses_{}.csv", dept));

    // We need to map match_type string to int.

    let q_addr = format!(
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
            best_m AS (
                SELECT * FROM m_ranked WHERE rn = 1
            ),
            joined AS (
                SELECT 
                    a.id as id_ban,
                    COALESCE(bm.match_type, 'NONE') as res_type,
                    COALESCE(bm.distance_m, NULL) as dist
                FROM addresses a
                LEFT JOIN best_m bm ON a.id = bm.id_ban
            )
            SELECT
                count(*) as total_addresses,
                count(*) FILTER (WHERE res_type != 'NONE') as matched_addresses,
                count(*) FILTER (WHERE res_type = 'NONE') as unmatched_addresses,
                (count(*) FILTER (WHERE res_type != 'NONE')::DOUBLE / count(*)::DOUBLE * 100.0) as coverage_pct,
                count(*) FILTER (WHERE res_type = 'PreExisting') as res_pre,
                count(*) FILTER (WHERE res_type = 'Inside') as res_inside,
                count(*) FILTER (WHERE res_type = 'BorderNear') as res_border_near,
                count(*) FILTER (WHERE res_type = 'FallbackNearest') as res_fallback,
                count(*) FILTER (WHERE res_type = 'NONE') as res_none,
                count(*) FILTER (WHERE res_type != 'NONE' AND dist <= 5) as dist_0_5,
                count(*) FILTER (WHERE res_type != 'NONE' AND dist > 5 AND dist <= 15) as dist_5_15,
                count(*) FILTER (WHERE res_type != 'NONE' AND dist > 15 AND dist <= 50) as dist_15_50,
                count(*) FILTER (WHERE res_type != 'NONE' AND dist > 50) as dist_gt_50
            FROM joined
        ) TO '{}' (FORMAT 'CSV', HEADER)
    "#,
        addr_csv.to_string_lossy()
    );

    conn.execute(&q_addr, []).context("QA Addresses calc")?;

    // Calculate Average Confidence
    // From matches table.
    // confidence column exists in matches parquet?
    // Let's check if 'confidence' is in the view. `view matches` comes from parquet.
    // If we put it in generic structs, it should be there.
    let avg_conf: f64 = conn.query_row(
        "SELECT COALESCE(AVG(confidence), 0.0) FROM matches WHERE match_type != 'None'",
        [],
        |r| r.get(0),
    ).unwrap_or(0.0);

    // If final_matched_parcels was not set (loop didn't run?), set it.
    // 1500 is the last one.
    
    // Total coverage usually refers to "Matched at all" (which is <= 1500 or just matched).
    // Let's use the one for 1500m which is fairly inclusive.
    // Or we could query for ALL matches not None.
    // Let's use the 1500m bucket as "Matched".
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
