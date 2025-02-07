use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[clap(author, version)]
/// Concatenates one or more parquet files in directories
struct Args {
    /// input directory
    in_dir: PathBuf,

    /// output directory
    out_dir: PathBuf,

    /// perform action on all subdirectories
    #[arg(short, long)]
    recursive: bool,
}

impl Args {
    fn run(&self) -> Result<()> {
        pcat::run_cat(&self.in_dir, &self.out_dir, self.recursive)
    }
}

fn main() -> Result<()> {
    Args::parse().run()
}
