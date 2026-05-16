//! Migration utilities for switching a deployment from per-node layout to the
//! shared-catalog / shared-inventory multi-node layout.
//!
//! Today the only migration implemented is `catalog --to-shared`: copy every
//! object under `<node_id>/catalogs/` to `_catalog/catalogs/` using
//! `PutMode::Create`. Existing objects under the destination are left alone,
//! which makes the operation idempotent and safe to re-run.

use anyhow::{Context, bail};
use futures::StreamExt;
use influxdb3_clap_blocks::object_store::ObjectStoreConfig;
use influxdb3_process::PROCESS_UUID_STR;
use object_store::path::Path as ObjPath;
use object_store::{PutMode, PutOptions};

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Copy a per-node catalog into the shared `_catalog/` prefix.
    Catalog(CatalogConfig),
}

#[derive(Debug, clap::Args)]
pub struct CatalogConfig {
    /// Object store options (s3, file, etc.) — same flags as `serve`.
    #[clap(flatten)]
    pub object_store_config: ObjectStoreConfig,

    /// Per-node prefix containing the legacy catalog (i.e. the value passed
    /// to the running server as `--node-id`). Catalog files live under
    /// `<source_node_id>/catalogs/` and will be copied to `_catalog/catalogs/`.
    #[clap(long = "from-node-id", env = "INFLUXDB3_MIGRATE_FROM_NODE_ID")]
    pub from_node_id: String,

    /// Run the migration without actually writing — log every object that
    /// would have been copied.
    #[clap(long, default_value_t = false, action)]
    pub dry_run: bool,
}

pub async fn command(config: Config) -> Result<(), anyhow::Error> {
    match config.cmd {
        SubCommand::Catalog(c) => migrate_catalog_to_shared(c).await,
    }
}

async fn migrate_catalog_to_shared(cfg: CatalogConfig) -> Result<(), anyhow::Error> {
    let object_store = cfg
        .object_store_config
        .make_object_store()
        .context("failed to construct object store from config")?;

    let source_dir = ObjPath::from(format!("{}/catalogs", cfg.from_node_id));
    let dest_prefix = influxdb3_catalog::catalog::SHARED_CATALOG_PREFIX;

    let mut listing = object_store.list(Some(&source_dir));
    let mut entries: Vec<ObjPath> = Vec::new();
    while let Some(item) = listing.next().await {
        entries.push(item?.location);
    }

    if entries.is_empty() {
        bail!(
            "no objects found under '{}'; refusing to claim a successful migration",
            source_dir
        );
    }

    let process_uuid = *PROCESS_UUID_STR;
    eprintln!(
        "migrating {} catalog objects from {} -> {}/catalogs/ (run-id={})",
        entries.len(),
        source_dir,
        dest_prefix,
        process_uuid
    );

    let mut copied = 0;
    let mut skipped_existing = 0;
    for src in entries {
        let leaf = src
            .filename()
            .with_context(|| format!("catalog object has no filename: {src}"))?;
        let dest = ObjPath::from(format!("{dest_prefix}/catalogs/{leaf}"));

        if cfg.dry_run {
            eprintln!("  [dry-run] {src} -> {dest}");
            continue;
        }

        let bytes = object_store.get(&src).await?.bytes().await?;
        match object_store
            .put_opts(
                &dest,
                bytes.into(),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => {
                copied += 1;
                eprintln!("  copied {src} -> {dest}");
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                skipped_existing += 1;
                eprintln!("  skipped (already exists) {dest}");
            }
            Err(e) => {
                return Err(e).with_context(|| format!("copy {src} -> {dest}"));
            }
        }
    }

    eprintln!(
        "migration done. copied={copied} skipped_existing={skipped_existing} dry_run={}",
        cfg.dry_run
    );
    Ok(())
}
