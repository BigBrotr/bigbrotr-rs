SELECT
NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname NOT IN ('information_schema','public'))
AND NOT EXISTS (SELECT 1 FROM pg_depend d JOIN pg_namespace n ON d.refclassid='pg_namespace'::regclass AND d.refobjid=n.oid WHERE n.nspname='public')
AND NOT EXISTS (SELECT 1 FROM pg_largeobject_metadata)
AND NOT EXISTS (SELECT 1 FROM pg_event_trigger)
AND NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname<>'plpgsql')
AND NOT EXISTS (SELECT 1 FROM pg_foreign_data_wrapper)
AND NOT EXISTS (SELECT 1 FROM pg_foreign_server)
AND NOT EXISTS (SELECT 1 FROM pg_publication)
AND NOT EXISTS (SELECT 1 FROM pg_subscription)
AS empty
