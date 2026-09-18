-- Isolated dependency experiment only. Not a product migration or selected fence.
-- Requires the fresh pinned DuckLake catalog initialized by catalog_fence.rs.
-- No native metadata values/columns are rewritten; this experiment deliberately
-- adds PG statement triggers, whose compatibility must be qualified separately.
CREATE SCHEMA bb_fence_guard;
REVOKE ALL ON SCHEMA bb_fence_guard FROM PUBLIC;
CREATE TABLE bb_fence_guard.owner (
    singleton boolean PRIMARY KEY CHECK (singleton),
    login name
);
INSERT INTO bb_fence_guard.owner VALUES (true, NULL);
CREATE FUNCTION bb_fence_guard.require_owner() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE allowed name;
BEGIN
    -- Lock is held through the actual metadata transaction's COMMIT/ROLLBACK.
    -- A fixed older snapshot must either lock its unchanged row or fail with a
    -- serialization error; a stale nonlocking read would not be a fence.
    SELECT login INTO STRICT allowed FROM bb_fence_guard.owner
        WHERE singleton FOR SHARE;
    IF allowed IS NULL OR allowed <> session_user THEN
        RAISE EXCEPTION 'catalog writer excluded' USING ERRCODE = '55000';
    END IF;
    RETURN NULL;
END;
$$;
REVOKE ALL ON FUNCTION bb_fence_guard.require_owner() FROM PUBLIC;
DO $$
DECLARE relation text;
expected text[] := ARRAY[
    'ducklake_column', 'ducklake_column_mapping', 'ducklake_column_tag',
    'ducklake_data_file', 'ducklake_delete_file', 'ducklake_file_column_stats',
    'ducklake_file_partition_value', 'ducklake_file_variant_stats',
    'ducklake_files_scheduled_for_deletion', 'ducklake_inlined_data_tables',
    'ducklake_macro', 'ducklake_macro_impl', 'ducklake_macro_parameters',
    'ducklake_metadata', 'ducklake_name_mapping', 'ducklake_partition_column',
    'ducklake_partition_info', 'ducklake_schema', 'ducklake_schema_versions',
    'ducklake_snapshot', 'ducklake_snapshot_changes', 'ducklake_sort_expression',
    'ducklake_sort_info', 'ducklake_table', 'ducklake_table_column_stats',
    'ducklake_table_stats', 'ducklake_tag', 'ducklake_view'];
BEGIN
    IF (SELECT array_agg(tablename::text ORDER BY tablename)
        FROM pg_catalog.pg_tables WHERE schemaname = 'public') IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'unexpected native catalog relations';
    END IF;
    FOREACH relation IN ARRAY expected LOOP
        EXECUTE format('CREATE TRIGGER bigbrotr_writer_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.%I FOR EACH STATEMENT EXECUTE FUNCTION bb_fence_guard.require_owner()', relation);
    END LOOP;
END;
$$;
-- Ordinary writers cannot create/drop/alter catalog objects or bypass triggers.
REVOKE ALL ON SCHEMA public FROM bb_fence_writer;
GRANT USAGE ON SCHEMA public TO bb_fence_writer;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM bb_fence_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO bb_fence_writer;
