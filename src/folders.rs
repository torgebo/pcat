use anyhow::{Context, Result};
use std::fs::File;
use std::path::{Path, PathBuf};

use parquet::column::writer::ColumnCloseResult;
use parquet::errors::ParquetError;
use parquet::errors::Result as ParquetResult; //, Result};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use std::sync::Arc;

/// Get all paths in folder
pub fn get_folder_paths(files_dir: &Path) -> ParquetResult<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(files_dir)?
        .map(|de| de.map(|p| p.path()))
        .filter(|resp| resp.as_ref().map_or(true, |p| p.is_file()))
        .collect::<ParquetResult<Vec<_>, _>>()?;
    paths.sort();

    Ok(paths)
}

pub fn cat_file(output: PathBuf, input: Vec<PathBuf>) -> ParquetResult<()> {
    if input.is_empty() {
        return Ok(());
    }

    let output = File::create(&output)?;

    let schema = {
        let inputs = input
            .iter()
            .map(|x| {
                let reader = File::open(x)?;
                let metadata = ParquetMetaDataReader::new().parse_and_finish(&reader)?;
                Ok(metadata)
            })
            .collect::<ParquetResult<Vec<_>>>()?;

        let expected = inputs[0].file_metadata().schema();
        for metadata in inputs.iter().skip(1) {
            let actual = metadata.file_metadata().schema();
            if expected != actual {
                return Err(ParquetError::General(format!(
                    "inputs must have the same schema, {expected:#?} vs {actual:#?}"
                )));
            }
        }

        let schema = inputs[0].file_metadata().schema_descr().root_schema_ptr();
        schema
    };
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(output, schema, props)?;

    input
        .iter()
        .map(|x| {
            let input = File::open(x)?;
            let metadata = ParquetMetaDataReader::new().parse_and_finish(&input)?;
            for rg in metadata.row_groups() {
                let mut rg_out = writer.next_row_group()?;
                for column in rg.columns() {
                    let result = ColumnCloseResult {
                        bytes_written: column.compressed_size() as _,
                        rows_written: rg.num_rows() as _,
                        metadata: column.clone(),
                        bloom_filter: None,
                        column_index: None,
                        offset_index: None,
                    };
                    rg_out.append_column(&input, result)?;
                }
                rg_out.close()?;
            }
            Ok(())
        })
        .collect::<ParquetResult<Vec<_>>>()?;

    writer.close()?;

    Ok(())
}

pub fn compute_outdir_paths(
    len_hint: usize,
    base_in_dir: &Path,
    base_out_dir: &Path,
    process_paths: &[PathBuf],
) -> Result<Vec<PathBuf>> {
    let mut outdir_paths: Vec<PathBuf> = Vec::<PathBuf>::with_capacity(len_hint);

    for path in process_paths.iter() {
        let rel_path = &path
            .strip_prefix(base_in_dir)
            .with_context(|| format!("unable to strip prefix {base_in_dir:?} of path {path:?}"))?;
        let outdir_path = base_out_dir.join(rel_path);
        outdir_paths.push(outdir_path);
    }

    Ok(outdir_paths)
}

pub fn collect_paths(in_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut to_process: Vec<PathBuf> = Vec::<_>::with_capacity(16);
    let mut to_expand: Vec<PathBuf> = Vec::<_>::with_capacity(16);
    to_expand.push(in_dir.to_path_buf());
    while let Some(cur_dir) = to_expand.pop() {
        let mut subdirs: Vec<PathBuf> = cur_dir
            .read_dir()
            .with_context(|| format!("unable to read {cur_dir:?}"))?
            .map(|rde| rde.map(|de| de.path()))
            .filter(|resp| resp.as_ref().map_or(true, |p| p.is_dir()))
            .collect::<Result<Vec<_>, _>>()?;
        subdirs.sort();
        to_expand.extend_from_slice(&subdirs);
        to_process.push(cur_dir);
    }

    Ok(to_process)
}
