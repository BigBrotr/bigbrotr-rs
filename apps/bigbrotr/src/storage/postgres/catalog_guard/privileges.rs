//! Privileges are semantic state, deliberately separate from the existing
//! structural checksum. Inspect server-side predicates, never an unbounded ACL
//! list. Cluster/database administrators remain a trusted operator boundary.
use super::{GuardError, inspection::require};
use tokio_postgres::Transaction;

pub(super) async fn fresh(tx: &Transaction<'_>) -> Result<(), GuardError> {
    // New guard objects use the current creator's global defaults. Defaults IN
    // SCHEMA public do not apply to the newly created bigbrotr_archive schema.
    // Only PUBLIC's ordinary function EXECUTE default is expected; the compiled
    // migration revokes it before commit. Other foreign defaults refuse.
    require(
        tx,
        r#"
SELECT NOT EXISTS (
    SELECT 1 FROM pg_default_acl d CROSS JOIN LATERAL aclexplode(d.defaclacl) a
    WHERE d.defaclrole=current_user::regrole::oid AND d.defaclnamespace=0
      AND d.defaclobjtype IN ('r','f','n')
      AND a.grantee<>d.defaclrole
      AND NOT (a.grantee=0 AND d.defaclobjtype='f'
               AND a.privilege_type='EXECUTE' AND NOT a.is_grantable)
) AND NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
    WHERE n.nspname='public' AND c.relkind='r' AND a.grantee<>c.relowner
) AND NOT EXISTS (
    SELECT 1 FROM pg_attribute col JOIN pg_class c ON c.oid=col.attrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace
    CROSS JOIN LATERAL aclexplode(COALESCE(col.attacl,acldefault('c',c.relowner))) a
    WHERE n.nspname='public' AND c.relkind='r' AND a.grantee<>c.relowner
)"#,
        &[],
        GuardError::UnsafeRole,
    )
    .await
}

pub(super) async fn installed(tx: &Transaction<'_>) -> Result<(), GuardError> {
    // ACL NULL means the built-in default, not an empty privilege set. Keep
    // grantee=0 (PUBLIC); joining only pg_roles would silently lose that entry.
    require(
        tx,
        r#"
SELECT NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
    WHERE n.nspname IN ('public','bigbrotr_archive') AND c.relkind='r'
      AND a.grantee<>c.relowner AND (
        a.grantee=0 OR a.is_grantable OR NOT (
            (n.nspname='public' AND a.privilege_type IN ('SELECT','INSERT','UPDATE','DELETE'))
            OR (n.nspname='bigbrotr_archive' AND c.relname IN ('installation','scientific_schema')
                AND a.privilege_type='SELECT')
        )
      )
) AND NOT EXISTS (
    SELECT 1 FROM pg_attribute col JOIN pg_class c ON c.oid=col.attrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace
    CROSS JOIN LATERAL aclexplode(COALESCE(col.attacl,acldefault('c',c.relowner))) a
    WHERE n.nspname IN ('public','bigbrotr_archive') AND c.relkind='r'
      AND a.grantee<>c.relowner AND (
        a.grantee=0 OR a.is_grantable OR NOT (
            (n.nspname='public' AND a.privilege_type IN ('SELECT','INSERT','UPDATE'))
            OR (n.nspname='bigbrotr_archive' AND c.relname IN ('installation','scientific_schema')
                AND a.privilege_type='SELECT')
        )
      )
) AND NOT EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a
    WHERE n.nspname='bigbrotr_archive' AND a.grantee<>p.proowner
) AND NOT EXISTS (
    SELECT 1 FROM pg_namespace n
    CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
    WHERE n.nspname='bigbrotr_archive' AND a.grantee<>n.nspowner
      AND (a.grantee=0 OR a.is_grantable OR a.privilege_type<>'USAGE')
) AND NOT EXISTS (
    SELECT 1 FROM pg_namespace n
    CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
    WHERE n.nspname='public' AND (a.privilege_type<>'USAGE' OR a.is_grantable)
      AND a.grantee NOT IN (n.nspowner,
          (SELECT datdba FROM pg_database WHERE datname=current_database()),
          (SELECT nspowner FROM pg_namespace WHERE nspname='bigbrotr_archive'))
)"#,
        &[],
        GuardError::UnsafeRole,
    )
    .await?;

    // Owners can revoke ordinary privileges from themselves. A superuser
    // initializer must not mistake its own successful read for manager fitness.
    // Each has_* call checks one privilege: comma lists mean OR, not AND.
    require(tx, r#"
WITH manager AS (SELECT nspowner AS oid FROM pg_namespace WHERE nspname='bigbrotr_archive')
SELECT has_database_privilege(m.oid,current_database(),'CONNECT')
   AND NOT EXISTS (SELECT 1 FROM pg_auth_members a WHERE a.roleid=m.oid
       OR (a.member=m.oid AND (NOT a.admin_option OR a.inherit_option OR a.set_option)))
   AND has_schema_privilege(m.oid,'public','USAGE')
   AND has_schema_privilege(m.oid,'bigbrotr_archive','USAGE')
   AND has_schema_privilege(m.oid,'bigbrotr_archive','CREATE')
   AND NOT EXISTS (
       SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       CROSS JOIN unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) p(privilege)
       WHERE n.nspname IN ('public','bigbrotr_archive') AND c.relkind='r'
         AND NOT has_table_privilege(m.oid,c.oid,p.privilege)
   ) AND NOT EXISTS (
       SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
       WHERE n.nspname='bigbrotr_archive' AND NOT has_function_privilege(m.oid,p.oid,'EXECUTE')
   )
FROM manager m"#, &[], GuardError::UnsafeRole).await
}
