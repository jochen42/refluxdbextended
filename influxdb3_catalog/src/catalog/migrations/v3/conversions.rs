//! v2-snapshot → v3-wire `From` impls.

use crate::catalog::versions::v2::{
    DeletionScope as V2DeletionScope, FieldFamilyMode as V2FieldFamilyMode,
    FieldFamilyName as V2FieldFamilyName,
};
use crate::format::records::types::{
    Actions as WireActions, CacheSource as WireCacheSource, ColumnDefinition as WireColumnDef,
    DeletionScope as WireDeletionScope, ErrorBehavior as WireErrorBehavior,
    FieldColumn as WireFieldColumn, FieldDataType as WireFieldDataType,
    FieldFamilyDefinition as WireFieldFamilyDef, FieldFamilyMode as WireFieldFamilyMode,
    FieldFamilyName as WireFieldFamilyName, FieldIdentifier as WireFieldIdent,
    LastCacheValueColumnsDef as WireLvcDef, NodeMode as WireNodeMode, NodeSpec as WireNodeSpec,
    Permission as WirePermission, ResourceIdentifier as WireResourceIdent,
    ResourceNameEntry as WireResourceNameEntry, ResourceType as WireResourceType,
    RetentionPeriod as WireRetentionPeriod, StorageMode as WireStorageMode,
    TagColumn as WireTagColumn, TimestampColumn as WireTimestampColumn,
    TriggerSettings as WireTriggerSettings, TriggerSpec as WireTriggerSpec,
};
use crate::log::versions::v4::{
    CacheSource, ErrorBehavior, NodeMode, NodeSpec, StorageMode, TriggerSettings,
    TriggerSpecificationDefinition,
};
use crate::snapshot::versions::v4::{
    ActionsSnapshot, ColumnDefinitionSnapshot, FieldDataType, FieldFamilySnapshot,
    PermissionSnapshot, ResourceIdentifierSnapshot, ResourceTypeSnapshot, RetentionPeriodSnapshot,
};
use influxdb3_id::ColumnIdentifier;

impl From<StorageMode> for WireStorageMode {
    fn from(mode: StorageMode) -> Self {
        match mode {
            StorageMode::Parquet => Self::Parquet,
            StorageMode::PachaTree => Self::PachaTree,
            StorageMode::ParquetAndPachaTree => Self::ParquetAndPachaTree,
        }
    }
}

impl From<NodeMode> for WireNodeMode {
    fn from(mode: NodeMode) -> Self {
        match mode {
            NodeMode::Core => Self::Core,
            NodeMode::Query => Self::Query,
            NodeMode::Ingest => Self::Ingest,
            NodeMode::Compact => Self::Compact,
            NodeMode::Process => Self::Process,
            NodeMode::All => Self::All,
        }
    }
}

impl From<&Option<RetentionPeriodSnapshot>> for WireRetentionPeriod {
    fn from(period: &Option<RetentionPeriodSnapshot>) -> Self {
        match period {
            Some(RetentionPeriodSnapshot::Duration(d)) => Self::Duration {
                duration_secs: d.as_secs(),
            },
            Some(RetentionPeriodSnapshot::Indefinite) | None => Self::Indefinite,
        }
    }
}

impl From<&V2DeletionScope> for WireDeletionScope {
    fn from(scope: &V2DeletionScope) -> Self {
        match scope {
            V2DeletionScope::DataAndCatalog => Self::DataAndCatalog,
            V2DeletionScope::DataOnlyRemoveTables => Self::DataOnlyRemoveTables,
            V2DeletionScope::DataOnlyKeepResources => Self::DataOnlyKeepResources,
        }
    }
}

impl From<&NodeSpec> for WireNodeSpec {
    fn from(spec: &NodeSpec) -> Self {
        match spec {
            NodeSpec::All => Self::All,
            NodeSpec::Nodes(ids) => Self::Nodes(ids.iter().map(|id| id.get()).collect()),
        }
    }
}

impl From<&TriggerSpecificationDefinition> for WireTriggerSpec {
    fn from(spec: &TriggerSpecificationDefinition) -> Self {
        match spec {
            TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                Self::SingleTableWalWrite {
                    table_name: table_name.clone(),
                }
            }
            TriggerSpecificationDefinition::AllTablesWalWrite => Self::AllTablesWalWrite,
            TriggerSpecificationDefinition::Schedule { schedule } => Self::Schedule {
                schedule: schedule.clone(),
            },
            TriggerSpecificationDefinition::RequestPath { path } => {
                Self::RequestPath { path: path.clone() }
            }
            TriggerSpecificationDefinition::Every { duration } => Self::Every {
                duration_ns: u64::try_from(duration.as_nanos())
                    .expect("trigger Every duration overflows u64 nanoseconds"),
            },
        }
    }
}

impl From<TriggerSettings> for WireTriggerSettings {
    fn from(settings: TriggerSettings) -> Self {
        Self {
            run_async: settings.run_async,
            error_behavior: settings.error_behavior.into(),
        }
    }
}

impl From<ErrorBehavior> for WireErrorBehavior {
    fn from(behavior: ErrorBehavior) -> Self {
        match behavior {
            ErrorBehavior::Log => Self::Log,
            ErrorBehavior::Retry => Self::Retry,
            ErrorBehavior::Disable => Self::Disable,
        }
    }
}

impl From<V2FieldFamilyMode> for WireFieldFamilyMode {
    fn from(mode: V2FieldFamilyMode) -> Self {
        match mode {
            V2FieldFamilyMode::Aware => Self::Aware,
            V2FieldFamilyMode::Auto => Self::Auto,
        }
    }
}

impl From<&V2FieldFamilyName> for WireFieldFamilyName {
    fn from(name: &V2FieldFamilyName) -> Self {
        match name {
            V2FieldFamilyName::User(s) => Self::User(s.to_string()),
            V2FieldFamilyName::Auto(n) => Self::Auto(*n),
        }
    }
}

impl From<&FieldFamilySnapshot> for WireFieldFamilyDef {
    fn from(ff: &FieldFamilySnapshot) -> Self {
        Self {
            id: ff.id.get(),
            name: (&ff.name).into(),
        }
    }
}

impl From<FieldDataType> for WireFieldDataType {
    fn from(dt: FieldDataType) -> Self {
        match dt {
            FieldDataType::String => Self::String,
            FieldDataType::Integer => Self::Integer,
            FieldDataType::UInteger => Self::UInteger,
            FieldDataType::Float => Self::Float,
            FieldDataType::Boolean => Self::Boolean,
        }
    }
}

impl From<CacheSource> for WireCacheSource {
    fn from(source: CacheSource) -> Self {
        match source {
            CacheSource::User => Self::User,
            CacheSource::Auto => Self::Auto,
        }
    }
}

impl From<&Option<Vec<ColumnIdentifier>>> for WireLvcDef {
    fn from(vals: &Option<Vec<ColumnIdentifier>>) -> Self {
        match vals {
            Some(cols) => Self::Explicit(cols.iter().copied().map(Into::into).collect()),
            None => Self::AllNonKeyColumns,
        }
    }
}

impl From<&ResourceTypeSnapshot> for WireResourceType {
    fn from(rt: &ResourceTypeSnapshot) -> Self {
        match rt {
            ResourceTypeSnapshot::Database => Self::Database,
            ResourceTypeSnapshot::Token => Self::Token,
            ResourceTypeSnapshot::System => Self::System,
            ResourceTypeSnapshot::Wildcard => Self::Wildcard,
        }
    }
}

impl From<&ResourceIdentifierSnapshot> for WireResourceIdent {
    fn from(ri: &ResourceIdentifierSnapshot) -> Self {
        match ri {
            ResourceIdentifierSnapshot::Database(ids) => {
                Self::Database(ids.iter().map(|id| id.get()).collect())
            }
            ResourceIdentifierSnapshot::Token(ids) => {
                Self::Token(ids.iter().map(|id| id.get()).collect())
            }
            ResourceIdentifierSnapshot::System(ids) => {
                Self::System(ids.iter().map(|id| id.as_u16()).collect())
            }
            ResourceIdentifierSnapshot::Wildcard => Self::Wildcard,
        }
    }
}

impl From<&ActionsSnapshot> for WireActions {
    fn from(a: &ActionsSnapshot) -> Self {
        match a {
            ActionsSnapshot::Database(d) => Self::Database(d.0),
            ActionsSnapshot::Token(t) => Self::Token(t.0),
            ActionsSnapshot::System(s) => Self::System(s.0),
            ActionsSnapshot::Wildcard => Self::Wildcard,
        }
    }
}

impl From<&PermissionSnapshot> for WirePermission {
    fn from(p: &PermissionSnapshot) -> Self {
        let resource_names = p
            .resource_names
            .as_ref()
            .map(|map| {
                map.iter()
                    .map(|(resource_id, meta)| WireResourceNameEntry {
                        resource_id: resource_id.clone(),
                        name: meta.name.clone(),
                        deleted: meta.deleted,
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self {
            resource_type: (&p.resource_type).into(),
            resource_identifier: (&p.resource_identifier).into(),
            actions: (&p.actions).into(),
            resource_names,
        }
    }
}

impl From<&ColumnDefinitionSnapshot> for WireColumnDef {
    fn from(col: &ColumnDefinitionSnapshot) -> Self {
        match col {
            ColumnDefinitionSnapshot::Timestamp(ts) => Self::Timestamp(WireTimestampColumn {
                column_id: ts.column_id.map(|c| c.get()),
                name: ts.name.to_string(),
            }),
            ColumnDefinitionSnapshot::Tag(tag) => Self::Tag(WireTagColumn {
                id: tag.id.get(),
                column_id: tag.column_id.map(|c| c.get()),
                name: tag.name.to_string(),
            }),
            ColumnDefinitionSnapshot::Field(field) => Self::Field(WireFieldColumn {
                id: WireFieldIdent {
                    family_id: field.id.field_family_id().get(),
                    field_id: field.id.id().get(),
                },
                column_id: field.column_id.map(|c| c.get()),
                name: field.name.to_string(),
                data_type: field.data_type.into(),
            }),
        }
    }
}

/// Remap a fork-written v2 scoped-token permission to upstream's action bits.
///
/// The fork's v2 catalog (RefluxDB Extended up to 3.9.x) stored scoped-token
/// actions with its own bit layout — database `READ=1, WRITE=2, CREATE=4`,
/// token `READ=1, WRITE=2, CREATE=4, DELETE=8` — while upstream's v3 records
/// use `CREATE=1, DESCRIBE=2, …, READ=16, WRITE=32` (database) and
/// `CREATE=1, READ=2, UPDATE=4, DELETE=8` (token). Passing the raw bits
/// through would turn a fork read-only token into a create-only one.
///
/// Every non-admin token in a fork v2 catalog was created by the fork: Core
/// v2 had no resource-scoped tokens, so the remap applies unconditionally.
pub(crate) fn fork_v2_permission_to_wire(p: &PermissionSnapshot) -> WirePermission {
    let mut wire = WirePermission::from(p);
    wire.actions = match &p.actions {
        ActionsSnapshot::Database(d) => WireActions::Database(remap_fork_database_actions(d.0)),
        ActionsSnapshot::Token(t) => WireActions::Token(remap_fork_crud_actions(t.0)),
        ActionsSnapshot::System(_) | ActionsSnapshot::Wildcard => wire.actions,
    };
    wire
}

fn remap_bits(bits: u16, mapping: &[(u16, u16)]) -> u16 {
    mapping
        .iter()
        .filter(|(fork, _)| bits & fork == *fork)
        .fold(0, |acc, (_, upstream)| acc | upstream)
}

pub(crate) fn remap_fork_database_actions(bits: u16) -> u16 {
    use influxdb3_authz::DatabaseActions as A;
    remap_bits(bits, &[(1, A::READ), (1 << 1, A::WRITE), (1 << 2, A::CREATE)])
}

pub(crate) fn remap_fork_crud_actions(bits: u16) -> u16 {
    use influxdb3_authz::CrudActions as A;
    remap_bits(
        bits,
        &[(1, A::READ), (1 << 1, A::UPDATE), (1 << 2, A::CREATE), (1 << 3, A::DELETE)],
    )
}
