//! Operator-supplied permissions for scoped tokens, applied once during the
//! v2 → v3 migration.
//!
//! Neither v2 nor v3 can change a token's permissions in place, and the 3.9
//! v2 checkpoint writer stores every scoped token's actions as `u16::MAX`. A
//! token that was created read-only and then folded into a checkpoint would
//! migrate as `create,read,write`. This override sets the permissions the
//! migrated token gets, while it keeps the token's hash, so clients keep their
//! token string.
//!
//! Format: `<token name>=db:<*|db1,db2>:<read|write|create>[,…]`, entries
//! separated by `;`. Example: `ro=db:*:read;ingest=db:metrics:read,write`.
//!
//! The overrides take effect only in the migration run, that is, the first
//! start of a 3.12 node on a v2 catalog. After that they are ignored.

use std::sync::OnceLock;

use observability_deps::tracing::info;

use crate::snapshot::versions::v4::{
    ActionsSnapshot, CatalogSnapshot, DatabaseActionsSnapshot, PermissionSnapshot,
    ResourceIdentifierSnapshot, ResourceTypeSnapshot,
};

/// One parsed override entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TokenPermissionOverride {
    token_name: String,
    databases: DbScope,
    /// Actions in the fork's v2 bit layout (`read=1, write=2, create=4`), so
    /// the normal v2 → v3 remap applies to them.
    fork_v2_actions: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DbScope {
    All,
    Named(Vec<String>),
}

static OVERRIDES: OnceLock<Vec<TokenPermissionOverride>> = OnceLock::new();

/// Parse `spec` and register it for the migration in this process.
///
/// Call this once, before the catalog is opened. Returns an error for a
/// malformed spec, or when overrides were already registered.
pub fn set_migration_token_permissions(spec: &str) -> Result<usize, String> {
    let parsed = parse(spec)?;
    let n = parsed.len();
    OVERRIDES
        .set(parsed)
        .map_err(|_| "migration token permissions are already set".to_string())?;
    Ok(n)
}

pub(crate) fn registered() -> &'static [TokenPermissionOverride] {
    OVERRIDES.get().map(Vec::as_slice).unwrap_or(&[])
}

pub(crate) fn parse(spec: &str) -> Result<Vec<TokenPermissionOverride>, String> {
    let mut out: Vec<TokenPermissionOverride> = Vec::new();
    for entry in spec.split(';').map(str::trim).filter(|e| !e.is_empty()) {
        let (name, perm) = entry
            .split_once('=')
            .ok_or_else(|| format!("{entry:?}: expected <token name>=db:<dbs>:<actions>"))?;
        let name = name.trim();
        if name.is_empty() {
            return Err(format!("{entry:?}: empty token name"));
        }
        if out.iter().any(|o| o.token_name == name) {
            return Err(format!("token {name:?} is listed twice"));
        }
        let mut parts = perm.trim().splitn(3, ':');
        let (Some("db"), Some(dbs), Some(actions)) = (parts.next(), parts.next(), parts.next())
        else {
            return Err(format!(
                "{entry:?}: only database permissions are supported (db:<dbs>:<actions>)"
            ));
        };
        let databases = match dbs.trim() {
            "*" => DbScope::All,
            "" => return Err(format!("{entry:?}: empty database list")),
            list => DbScope::Named(list.split(',').map(|d| d.trim().to_string()).collect()),
        };
        let mut fork_v2_actions = 0u16;
        for a in actions.split(',').map(str::trim) {
            fork_v2_actions |= match a {
                "read" => 1,
                "write" => 1 << 1,
                "create" => 1 << 2,
                other => return Err(format!("{entry:?}: unknown action {other:?}")),
            };
        }
        out.push(TokenPermissionOverride {
            token_name: name.to_string(),
            databases,
            fork_v2_actions,
        });
    }
    Ok(out)
}

/// Replace the permissions of every token named in `overrides`.
///
/// Fails when a token does not exist, is an admin token, or names a database
/// that does not exist, so a typo stops the migration instead of leaving a
/// token with more access than intended.
pub(crate) fn apply(
    snapshot: &mut CatalogSnapshot,
    overrides: &[TokenPermissionOverride],
) -> Result<(), String> {
    for o in overrides {
        let identifier = match &o.databases {
            DbScope::All => ResourceIdentifierSnapshot::Wildcard,
            DbScope::Named(names) => {
                let mut ids = Vec::with_capacity(names.len());
                for n in names {
                    let id = snapshot
                        .databases
                        .repo
                        .values()
                        .find(|db| !db.deleted && db.name.as_ref() == n)
                        .map(|db| db.id)
                        .ok_or_else(|| {
                            format!("token {:?}: database {n:?} not found", o.token_name)
                        })?;
                    ids.push(id);
                }
                ResourceIdentifierSnapshot::Database(ids)
            }
        };
        let token = snapshot
            .tokens
            .repo
            .values_mut()
            .find(|t| t.name.as_ref() == o.token_name)
            .ok_or_else(|| format!("token {:?} not found in the v2 catalog", o.token_name))?;
        if token.permissions.iter().any(|p| {
            matches!(p.resource_type, ResourceTypeSnapshot::Wildcard)
                && matches!(p.actions, ActionsSnapshot::Wildcard)
        }) {
            return Err(format!(
                "token {:?} is an admin token; its permissions cannot be overridden",
                o.token_name
            ));
        }
        info!(
            token = %o.token_name,
            before = ?token.permissions.iter().map(|p| &p.actions).collect::<Vec<_>>(),
            "overriding token permissions in the v2 → v3 migration"
        );
        token.permissions = vec![PermissionSnapshot {
            resource_type: ResourceTypeSnapshot::Database,
            resource_identifier: identifier,
            actions: ActionsSnapshot::Database(DatabaseActionsSnapshot(o.fork_v2_actions)),
            resource_names: None,
        }];
    }
    Ok(())
}
