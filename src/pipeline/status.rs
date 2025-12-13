use crate::cli::StatusArgs;
use crate::pipeline::state::BatchState;
use anyhow::Result;
use tracing::{info, warn};

pub fn run_status(args: StatusArgs) -> Result<()> {
    let state_path = args.data_dir.join("batch_state.json");
    let state = BatchState::load(&state_path)?;

    info!(
        data_dir=?args.data_dir,
        state_path=?state_path,
        started_at=%state.started_at,
        completed_departments=state.completed.len(),
        failed_departments=state.failed.len(),
        "pipeline status"
    );

    if !state.failed.is_empty() {
        for f in &state.failed {
            warn!(dept=%f.dept, error=%f.error, "failed department");
        }
    }

    Ok(())
}
