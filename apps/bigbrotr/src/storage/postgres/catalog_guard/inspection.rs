//! Inspect actual PostgreSQL objects, not the presence of a migration marker.
//! Every returned definition is bounded; structural hashes exclude data, ACLs
//! and cluster-specific OIDs so ordinary DML/grants do not invalidate the guard.
use super::{CatalogIdentity, GuardError, backend, body, checksum};
use sha2::{Digest, Sha256};
use tokio_postgres::{Transaction, types::ToSql};

const NATIVE: &str = include_str!("../../../../../../contracts/archive/catalog-native-v1.json");
const MAX_DEFINITION: usize = 8192;
const MAX_OBJECTS: usize = 1024;

pub(super) async fn require(
    tx: &Transaction<'_>,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
    error: GuardError,
) -> Result<(), GuardError> {
    if tx
        .query_one(sql, params)
        .await
        .map_err(backend)?
        .try_get::<_, Option<bool>>(0)
        .map_err(backend)?
        == Some(true)
    {
        Ok(())
    } else {
        Err(error)
    }
}

pub(super) async fn native(tx: &Transaction<'_>) -> Result<Vec<String>, GuardError> {
    let expected: serde_json::Value =
        serde_json::from_str(NATIVE).map_err(|_| GuardError::IncompatibleLayout)?;
    let expected = expected["tables"]
        .as_object()
        .ok_or(GuardError::IncompatibleLayout)?;
    // Unknown views, sequences, partitions and materialized views are refused.
    // Index relations are inspected through their owning table below.
    let rows = tx.query("SELECT c.relname::text, c.relkind='r' AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT c.relispartition AND NOT c.relhassubclass AND NOT EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=c.oid) AND NOT EXISTS(SELECT 1 FROM pg_policy WHERE polrelid=c.oid) AND c.relam=(SELECT oid FROM pg_am WHERE amname='heap') AND c.reloptions IS NULL FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind<>'i' ORDER BY c.relname LIMIT 29", &[]).await.map_err(backend)?;
    if rows.len() != expected.len() || expected.len() != 28 {
        return Err(GuardError::IncompatibleLayout);
    }
    let mut tables = Vec::with_capacity(28);
    for row in rows {
        let name: String = row.try_get(0).map_err(backend)?;
        let columns = expected.get(&name).ok_or(GuardError::IncompatibleLayout)?;
        if !row.try_get::<_, bool>(1).map_err(backend)? {
            return Err(GuardError::IncompatibleLayout);
        }
        // PostgreSQL names/type strings are bounded; no default expression or
        // arbitrary native metadata value is decoded by this shape inspection.
        let actual = tx.query("SELECT a.attname::text, format_type(a.atttypid,a.atttypmod), a.attnotnull, EXISTS(SELECT 1 FROM pg_constraint p WHERE p.conrelid=a.attrelid AND p.contype='p' AND a.attnum=ANY(p.conkey)), a.atthasdef OR a.attisdropped OR a.attidentity<>'' OR a.attgenerated<>'' OR a.attcollation<>(SELECT typcollation FROM pg_type WHERE oid=a.atttypid) FROM pg_attribute a WHERE a.attrelid=('public.'||$1::text)::regclass AND a.attnum>0 ORDER BY a.attnum LIMIT 33", &[&name]).await.map_err(backend)?;
        let mut found = Vec::with_capacity(actual.len());
        for column in actual {
            if column.try_get::<_, bool>(4).map_err(backend)? {
                return Err(GuardError::IncompatibleLayout);
            }
            found.push(
                serde_json::json!({"name":column.try_get::<_, &str>(0).map_err(backend)?,
                "type":column.try_get::<_, &str>(1).map_err(backend)?,
                "not_null":column.try_get::<_, bool>(2).map_err(backend)?,
                "primary_key":column.try_get::<_, bool>(3).map_err(backend)?}),
            );
        }
        if serde_json::Value::Array(found) != *columns {
            return Err(GuardError::IncompatibleLayout);
        }
        require(tx, "SELECT NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=('public.'||$1::text)::regclass AND (contype<>'p' OR condeferrable OR condeferred OR NOT convalidated)) AND NOT EXISTS(SELECT 1 FROM pg_index WHERE indrelid=('public.'||$1::text)::regclass AND (NOT indisprimary OR NOT indisvalid OR NOT indisready OR NOT indislive))", &[&name], GuardError::IncompatibleLayout).await?;
        tables.push(name);
    }
    Ok(tables)
}

pub(super) async fn metadata(
    tx: &Transaction<'_>,
    identity: &CatalogIdentity,
) -> Result<(), GuardError> {
    require(tx, "SELECT (SELECT count(*)=1 FROM public.ducklake_metadata WHERE key='version' AND value='1.0' AND scope IS NULL AND scope_id IS NULL) AND (SELECT count(*)=1 FROM public.ducklake_metadata WHERE key='data_path' AND value=$1::text AND scope IS NULL AND scope_id IS NULL) AND (SELECT count(*)=1 FROM public.ducklake_metadata WHERE key='encrypted' AND value='false' AND scope IS NULL AND scope_id IS NULL) AND NOT EXISTS(SELECT 1 FROM public.ducklake_metadata WHERE (key IN ('version','data_path','encrypted') AND (scope IS NOT NULL OR scope_id IS NOT NULL)) OR (key='data_inlining_row_limit' AND (value='0') IS NOT TRUE)) AND (SELECT count(*)=3 FROM public.ducklake_metadata WHERE key IN ('version','data_path','encrypted')) AND NOT EXISTS(SELECT 1 FROM public.ducklake_inlined_data_tables)", &[&identity.root], GuardError::IncompatibleLayout).await
}

pub(super) async fn fresh(
    tx: &Transaction<'_>,
    identity: &CatalogIdentity,
) -> Result<(), GuardError> {
    metadata(tx, identity).await?;
    // No adoption of an old scientific catalog, even if its current table list
    // happens to be empty. Native snapshot/schema identity must be pristine.
    for table in native(tx).await? {
        let predicate = match table.as_str() {
            "ducklake_snapshot" => {
                "count(*)=1 AND bool_and(snapshot_id=0 AND schema_version=0 AND next_catalog_id=1 AND next_file_id=0 AND snapshot_time IS NOT NULL)"
            }
            "ducklake_snapshot_changes" => {
                "count(*)=1 AND bool_and(snapshot_id=0 AND changes_made='created_schema:\"main\"' AND author IS NULL AND commit_message IS NULL AND commit_extra_info IS NULL)"
            }
            "ducklake_schema" => {
                "count(*)=1 AND bool_and(schema_id=0 AND schema_uuid IS NOT NULL AND begin_snapshot=0 AND end_snapshot IS NULL AND schema_name='main' AND path='main/' AND path_is_relative)"
            }
            "ducklake_metadata" => {
                "count(*)=4 AND count(*) FILTER(WHERE key='created_by' AND value='DuckDB d8cdaa33fd' AND scope IS NULL AND scope_id IS NULL)=1"
            }
            _ => "count(*)=0",
        };
        require(
            tx,
            &format!("SELECT {predicate} FROM public.{table}"),
            &[],
            GuardError::ExistingData,
        )
        .await?;
    }
    require(tx, "SELECT NOT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public') AND NOT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public')", &[], GuardError::IncompatibleLayout).await
}

// NOLOGIN reduces future connection authority but does not exclude an existing
// backend. It must not block inspection or successor activation. Launch readiness
// and credential exposure remain the separate parent/provisioning boundary.
// PG16 gives a non-superuser creator an implicit ADMIN-only membership in its
// new role. Permit that exact link to the catalog manager, never membership of
// the native writer in another role or an inheriting/SET/foreign member. Before
// guard installation the manager is the explicit installing current user.
async fn restricted(tx: &Transaction<'_>, login: &str) -> Result<(), GuardError> {
    require(tx, "SELECT NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls AND rolname<>current_user AND NOT EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.member=r.oid OR (m.roleid=r.oid AND NOT (m.member=COALESCE((SELECT nspowner FROM pg_namespace WHERE nspname='bigbrotr_archive'),(SELECT oid FROM pg_roles WHERE rolname=current_user)) AND m.admin_option AND NOT m.inherit_option AND NOT m.set_option))) AND NOT has_parameter_privilege(r.oid,'session_replication_role','SET') AND NOT has_database_privilege(r.oid,current_database(),'CREATE') FROM pg_roles r WHERE rolname=$1::name", &[&login], GuardError::UnsafeRole).await
}
pub(super) async fn bootstrap(tx: &Transaction<'_>, login: &str) -> Result<(), GuardError> {
    restricted(tx, login).await?;
    require(tx, "SELECT NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relowner<>(SELECT oid FROM pg_roles WHERE rolname=$1::name)) AND NOT EXISTS(SELECT 1 FROM pg_namespace WHERE nspowner=(SELECT oid FROM pg_roles WHERE rolname=$1::name)) AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datdba=(SELECT oid FROM pg_roles WHERE rolname=$1::name))", &[&login], GuardError::UnsafeRole).await
}
pub(super) async fn revoked(tx: &Transaction<'_>, login: &str) -> Result<(), GuardError> {
    writer(tx, login).await?;
    require(tx, "SELECT NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND (has_table_privilege($1::name,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER') OR has_any_column_privilege($1::name,c.oid,'INSERT,UPDATE')))", &[&login], GuardError::UnsafeRole).await
}
pub(super) async fn writer(tx: &Transaction<'_>, login: &str) -> Result<(), GuardError> {
    restricted(tx, login).await?;
    require(tx, "SELECT NOT has_schema_privilege($1::name,'public','CREATE') AND NOT has_schema_privilege($1::name,'bigbrotr_archive','USAGE,CREATE') AND NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('public','bigbrotr_archive') AND (c.relowner=(SELECT oid FROM pg_roles WHERE rolname=$1::name) OR has_table_privilege($1::name,c.oid,'TRIGGER,TRUNCATE') OR (n.nspname='bigbrotr_archive' AND has_table_privilege($1::name,c.oid,'SELECT,INSERT,UPDATE,DELETE')))) AND NOT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname IN ('public','bigbrotr_archive') AND p.proowner=(SELECT oid FROM pg_roles WHERE rolname=$1::name)) AND NOT EXISTS(SELECT 1 FROM pg_namespace WHERE nspowner=(SELECT oid FROM pg_roles WHERE rolname=$1::name)) AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datdba=(SELECT oid FROM pg_roles WHERE rolname=$1::name))", &[&login], GuardError::UnsafeRole).await
}

/// Hash length-framed, ordered structural definitions. Each SQL arm returns a
/// closed class of objects and truncates before network transfer; overflow is a
/// refusal, never a hash of a silently truncated definition. Privileged external
/// DDL remains an operator boundary; runtime logins cannot perform such changes.
pub(super) async fn layout(tx: &Transaction<'_>) -> Result<[u8; 32], GuardError> {
    let sql = r#"WITH objects(kind,ns,name,definition) AS (
SELECT 'table',n.nspname::text,c.relname::text,ROW(c.relkind,c.relpersistence,c.relrowsecurity,c.relforcerowsecurity,c.relispartition,c.reloptions)::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind<>'i'
UNION ALL SELECT 'column',n.nspname,c.relname||'.'||a.attnum,ROW(a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attisdropped,a.attidentity,a.attgenerated,a.attcollation::regcollation::text,a.attstorage,a.attcompression,pg_get_expr(d.adbin,d.adrelid))::text FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attnum>0
UNION ALL SELECT 'constraint',n.nspname,c.relname||'.'||p.conname,pg_get_constraintdef(p.oid,false) FROM pg_constraint p JOIN pg_class c ON c.oid=p.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
UNION ALL SELECT 'index',n.nspname,c.relname,ROW(pg_get_indexdef(i.indexrelid),i.indisvalid,i.indisready,i.indislive)::text FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace
UNION ALL SELECT 'trigger',n.nspname,c.relname||'.'||t.tgname,ROW(pg_get_triggerdef(t.oid,false),t.tgenabled,t.tgisinternal)::text FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
UNION ALL SELECT 'function',n.nspname,p.proname||'('||pg_get_function_identity_arguments(p.oid)||')',pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.prokind IN ('f','p')
UNION ALL SELECT 'rule',n.nspname,c.relname||'.'||r.rulename,pg_get_ruledef(r.oid,false) FROM pg_rewrite r JOIN pg_class c ON c.oid=r.ev_class JOIN pg_namespace n ON n.oid=c.relnamespace
) SELECT kind,ns,name,CASE WHEN octet_length(definition)<=8192 THEN definition ELSE repeat('x',8193) END FROM objects WHERE ns IN ('public','bigbrotr_archive') ORDER BY kind COLLATE "C",ns COLLATE "C",name COLLATE "C" LIMIT 1025"#;
    let rows = tx.query(sql, &[]).await.map_err(backend)?;
    if rows.len() > MAX_OBJECTS {
        return Err(GuardError::ResourceLimit);
    }
    let mut hash = Sha256::new();
    hash.update(b"bigbrotr/catalog-guard-layout/1\0");
    for row in rows {
        for column in 0..4 {
            let text: &str = row.try_get(column).map_err(backend)?;
            if text.len() > MAX_DEFINITION {
                return Err(GuardError::ResourceLimit);
            }
            hash.update((text.len() as u64).to_be_bytes());
            hash.update(text.as_bytes());
        }
    }
    Ok(hash.finalize().into())
}

pub(super) async fn installed(
    tx: &Transaction<'_>,
    identity: &CatalogIdentity,
) -> Result<(), GuardError> {
    native(tx).await?;
    metadata(tx, identity).await?;
    super::scientific::installed(tx, identity).await?;
    let actual = layout(tx).await?;
    require(tx, "SELECT count(*)=1 AND bool_and(singleton AND dataset_id=$1::bytea AND catalog_id=$2::bytea AND data_root=$3::text AND protocol=1 AND sql_checksum=$4::bytea AND layout_checksum=$5::bytea) FROM bigbrotr_archive.installation", &[&&identity.dataset.as_bytes()[..], &&identity.catalog.as_bytes()[..], &identity.root, &&checksum()[..], &&actual[..]], GuardError::WrongCatalog).await?;
    // Independent semantic checks: a stored checksum alone does not establish
    // which role or trigger enforces the fence on this actual server.
    require(tx, "SELECT count(*)=28 AND bool_and(t.tgname='bigbrotr_writer_guard' AND t.tgfoid='bigbrotr_archive.require_writer()'::regprocedure AND t.tgtype=62 AND t.tgenabled='O' AND NOT t.tgisinternal AND t.tgnargs=0 AND t.tgqual IS NULL) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'", &[], GuardError::IncompatibleLayout).await?;
    for (name, delimiter, definer) in [
        ("require_writer", super::WRITER_BODY, true),
        ("retain_history", "history", false),
    ] {
        require(tx, "SELECT p.prosrc=$2::text AND p.prosecdef=$3::boolean AND p.proconfig=ARRAY['search_path=pg_catalog']::text[] AND p.pronargs=0 AND p.prorettype='trigger'::regtype AND l.lanname='plpgsql' AND p.proowner=n.nspowner FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname='bigbrotr_archive' AND p.proname=$1::name", &[&name,&body(delimiter),&definer], GuardError::IncompatibleLayout).await?;
    }
    require(tx, "SELECT (SELECT count(*)=2 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname IN ('public','bigbrotr_archive')) AND NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('public','bigbrotr_archive') AND c.relowner<>(SELECT nspowner FROM pg_namespace WHERE nspname='bigbrotr_archive')) AND (SELECT count(*)=1 FROM bigbrotr_archive.owner) AND NOT EXISTS(SELECT 1 FROM bigbrotr_archive.owner o WHERE o.ordinal>0 AND NOT EXISTS(SELECT 1 FROM bigbrotr_archive.attempts a WHERE a.ordinal=o.ordinal))", &[], GuardError::IncompatibleLayout).await?;
    let rows = tx.query("SELECT a.login_name::text FROM bigbrotr_archive.owner o JOIN bigbrotr_archive.attempts a ON a.ordinal=o.ordinal WHERE o.enabled LIMIT 1", &[]).await.map_err(backend)?;
    for row in rows {
        writer(tx, row.try_get(0).map_err(backend)?).await?;
    }
    Ok(())
}
