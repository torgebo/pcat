//! Entry point functions
use anyhow::{Context, Result};
use indicatif::ParallelProgressIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rayon::prelude::*;
use std::path::{Path, PathBuf};

use parquet::errors::Result as ParquetResult; //, Result};

mod folders;

/// Run concatenation on all files in `base_in_dir` and write to `out_dir`.
/// If `recursive` is true, it will produce one output file for each subdirectory
/// Otherwise, it will act on the files (directly) within the given folder.
pub fn run_cat(base_in_dir: &Path, out_dir: &Path, recursive: bool) -> Result<()> {
    let process_paths: Vec<PathBuf> = if recursive {
        folders::collect_paths(base_in_dir)
            .with_context(|| format!("no such path {base_in_dir:?}"))?
    } else {
        vec![base_in_dir.to_path_buf().clone()]
    };

    let outdir_paths =
        folders::compute_outdir_paths(process_paths.len(), base_in_dir, out_dir, &process_paths)?;

    for outdir_path in outdir_paths.iter() {
        if !std::fs::exists(outdir_path)
            .with_context(|| format!("unable to check existence of {outdir_path:?}"))?
        {
            std::fs::create_dir_all(outdir_path)
                .with_context(|| format!("unable to recursively create dir {outdir_path:?}"))?;
        }
    }

    let p_in = process_paths.par_iter();
    let p_out = outdir_paths.par_iter();

    p_in.zip(p_out)
        .progress_count(process_paths.len() as u64)
        .map(|(dir_in, dir_out)| {
            let paths = folders::get_folder_paths(dir_in)?;
            let out_file = dir_out.join("out.parquet");
            folders::cat_file(out_file, paths)
        })
        .collect::<ParquetResult<Vec<_>, _>>()?;
    Ok(())
}
