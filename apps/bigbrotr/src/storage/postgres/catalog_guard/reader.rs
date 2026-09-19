//! Full manager inspection plus effective privileges of the dedicated reader.
use super::{CatalogIdentity, GuardError, backend, begin, inspection, lock_inspection};
use tokio_postgres::Client;

/// Observes the exact installed catalog and an operator-provisioned SELECT-only
/// login under the catalog inspection lock. No grants or credential changes.
/// Privileged external DDL/role changes remain the trusted operator boundary.
pub async fn inspect_reader(
    client: &mut Client,
    identity: &CatalogIdentity,
    login: &str,
) -> Result<(), GuardError> {
    super::identifier(login)?;
    let tx = begin(client, false).await?;
    lock_inspection(&tx).await?;
    inspection::installed(&tx, identity).await?;
    inspection::restricted(&tx, login).await?;
    // Test privileges separately: PostgreSQL comma lists are OR, not AND.
    inspection::require(&tx, r#"
SELECT rolcanlogin
    AND NOT has_database_privilege(r.oid,current_database(),'CREATE,TEMP,CONNECT WITH GRANT OPTION')
    AND has_database_privilege(r.oid,current_database(),'CONNECT')
    AND has_schema_privilege(r.oid,'public','USAGE')
    AND has_schema_privilege(r.oid,'bigbrotr_archive','USAGE')
    AND NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspowner=r.oid OR
        (n.nspname IN ('public','bigbrotr_archive') AND has_schema_privilege(r.oid,n.oid,'CREATE,USAGE WITH GRANT OPTION')))
    AND NOT EXISTS (SELECT 1 FROM pg_database WHERE datdba=r.oid)
    AND NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname IN ('public','bigbrotr_archive') AND c.relkind='r' AND (
            c.relowner=r.oid OR has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,SELECT WITH GRANT OPTION')
            OR has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES,SELECT WITH GRANT OPTION')
            OR CASE WHEN n.nspname='public' OR c.relname IN ('installation','scientific_schema')
                THEN NOT has_table_privilege(r.oid,c.oid,'SELECT')
                ELSE has_any_column_privilege(r.oid,c.oid,'SELECT') END
        )
    )
    AND NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname IN ('public','bigbrotr_archive') AND (p.proowner=r.oid OR has_function_privilege(r.oid,p.oid,'EXECUTE')))
FROM pg_roles r WHERE rolname=$1::name
"#, &[&login], GuardError::UnsafeRole).await?;
    tx.rollback().await.map_err(backend)
}
