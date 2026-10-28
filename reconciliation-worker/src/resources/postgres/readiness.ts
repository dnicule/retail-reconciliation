/**
 * @file Defines the worker's read-only PostgreSQL schema and permission inspection statement.
 * The readiness adapter executes this static query; it neither initializes nor alters database objects.
 */

/** Inspect existing columns, constraints, indexes, triggers, migration receipt, and required privileges. */
export const readinessSql = `WITH expected(table_name,name,type_name,required) AS (VALUES
  ('products','product_id','text',true),('products','name','text',true),('products','description','text',true),
  ('products','category','text',true),('products','source_revision','integer',true),
  ('products','applied_at','timestamp with time zone',true),('products','internal_note','text',false),
  ('reconciliation_jobs','job_id','uuid',true),('reconciliation_jobs','kind','text',true),
  ('reconciliation_jobs','product_id','text',true),('reconciliation_jobs','source_revision','integer',false),
  ('reconciliation_jobs','approved_payload','jsonb',false),('reconciliation_jobs','status','text',true),
  ('reconciliation_jobs','attempt_count','integer',true),('reconciliation_jobs','next_attempt_at','timestamp with time zone',false),
  ('reconciliation_jobs','lease_token','uuid',false),('reconciliation_jobs','lease_expires_at','timestamp with time zone',false),
  ('reconciliation_jobs','sql_outcome','text',true),('reconciliation_jobs','mongo_outcome','text',true),
  ('reconciliation_jobs','last_error','jsonb',false),('reconciliation_jobs','conflict_details','jsonb',false),
  ('reconciliation_jobs','created_at','timestamp with time zone',true),('reconciliation_jobs','updated_at','timestamp with time zone',true),
  ('reconciliation_jobs','completed_at','timestamp with time zone',false),
  ('schema_migrations','version','integer',true),('schema_migrations','name','text',true),
  ('schema_migrations','checksum','character(64)',true),('schema_migrations','applied_at','timestamp with time zone',true)
), relations AS (
  SELECT c.oid,c.relname,c.relkind,c.relpersistence,c.relrowsecurity,c.relforcerowsecurity
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='public' AND c.relname IN ('products','reconciliation_jobs','schema_migrations')
), constraints_expected(table_name,name) AS (VALUES
  ('products','products_id_check'),('products','products_name_check'),('products','products_description_check'),
  ('products','products_category_check'),('products','products_revision_check'),('products','products_internal_note_check'),
  ('reconciliation_jobs','jobs_kind_check'),('reconciliation_jobs','jobs_product_id_check'),('reconciliation_jobs','jobs_payload_check'),
  ('reconciliation_jobs','jobs_status_check'),('reconciliation_jobs','jobs_sql_outcome_check'),('reconciliation_jobs','jobs_mongo_outcome_check'),
  ('reconciliation_jobs','jobs_lease_check'),('reconciliation_jobs','jobs_next_attempt_check'),('reconciliation_jobs','jobs_completed_check'),
  ('reconciliation_jobs','jobs_attempt_count_check'),('reconciliation_jobs','jobs_last_error_check'),
  ('reconciliation_jobs','jobs_conflict_details_check'),('reconciliation_jobs','jobs_conflict_requires_details_check')
), indexes_expected(name,definition) AS (VALUES
  ('uq_jobs_update_revision','CREATE UNIQUE INDEX uq_jobs_update_revision ON public.reconciliation_jobs USING btree (product_id, source_revision) WHERE (kind = ''update''::text)'),
  ('ix_jobs_pending_due','CREATE INDEX ix_jobs_pending_due ON public.reconciliation_jobs USING btree (next_attempt_at, job_id) WHERE (status = ''pending''::text)'),
  ('ix_jobs_processing_lease','CREATE INDEX ix_jobs_processing_lease ON public.reconciliation_jobs USING btree (lease_expires_at, job_id) WHERE (status = ''processing''::text)'),
  ('ix_jobs_product_history','CREATE INDEX ix_jobs_product_history ON public.reconciliation_jobs USING btree (product_id, created_at DESC)')
)
SELECT COALESCE(
  has_schema_privilege('public','USAGE')
  AND (SELECT count(*) FROM relations WHERE relkind='r' AND relpersistence='p' AND NOT relrowsecurity AND NOT relforcerowsecurity)=3
  AND NOT EXISTS (SELECT 1 FROM relations WHERE NOT has_table_privilege(oid,'SELECT')
    OR (relname='products' AND NOT has_table_privilege(oid,'INSERT'))
    OR (relname<>'schema_migrations' AND NOT has_table_privilege(oid,'UPDATE')))
  AND NOT EXISTS (SELECT 1 FROM expected e LEFT JOIN relations r ON r.relname=e.table_name
    LEFT JOIN pg_attribute a ON a.attrelid=r.oid AND a.attname=e.name AND NOT a.attisdropped
    WHERE a.attname IS NULL OR format_type(a.atttypid,a.atttypmod)<>e.type_name OR a.attnotnull<>e.required
      OR a.attgenerated<>'' OR a.attidentity<>'')
  AND NOT EXISTS (SELECT 1 FROM constraints_expected e LEFT JOIN relations r ON r.relname=e.table_name
    LEFT JOIN pg_constraint c ON c.conrelid=r.oid AND c.conname=e.name
    WHERE c.oid IS NULL OR c.contype<>'c' OR NOT c.convalidated OR NOT c.conenforced)
  AND (SELECT count(*) FROM pg_constraint c JOIN relations r ON r.oid=c.conrelid WHERE c.contype='p' AND c.convalidated
    AND ((r.relname='products' AND pg_get_constraintdef(c.oid)='PRIMARY KEY (product_id)')
      OR (r.relname='reconciliation_jobs' AND pg_get_constraintdef(c.oid)='PRIMARY KEY (job_id)')
      OR (r.relname='schema_migrations' AND pg_get_constraintdef(c.oid)='PRIMARY KEY (version)')))=3
  AND (SELECT count(*) FROM pg_attribute a JOIN relations r ON r.oid=a.attrelid
    JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    WHERE ((r.relname IN ('products','schema_migrations') AND a.attname='applied_at')
      OR (r.relname='reconciliation_jobs' AND a.attname IN ('created_at','updated_at','next_attempt_at')))
    AND pg_get_expr(d.adbin,d.adrelid)='clock_timestamp()')=5
  AND NOT EXISTS (SELECT 1 FROM indexes_expected e LEFT JOIN pg_class c ON c.relname=e.name
    AND c.relnamespace='public'::regnamespace LEFT JOIN pg_index i ON i.indexrelid=c.oid
    WHERE c.oid IS NULL OR i.indexrelid IS NULL OR NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive OR pg_get_indexdef(i.indexrelid)<>e.definition)
  AND (SELECT count(*) FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
    WHERE t.tgrelid=to_regclass('public.reconciliation_jobs') AND t.tgenabled IN ('O','A') AND NOT t.tgisinternal
      AND p.pronamespace='public'::regnamespace AND t.tgqual IS NULL AND NOT t.tgdeferrable
      AND ((t.tgname='reconciliation_jobs_before_update' AND t.tgtype=19 AND p.proname='enforce_reconciliation_job_update'
        AND p.prosrc LIKE '%NEW.updated_at := clock_timestamp();%' AND p.prosrc LIKE '%IS DISTINCT FROM ROW(OLD.job_id%')
      OR (t.tgname='reconciliation_jobs_ready' AND t.tgtype=21 AND p.proname='notify_reconciliation_ready'
        AND p.prosrc LIKE '%pg_notify(''reconciliation_ready''%')))=2
  AND (SELECT count(*) FROM public.schema_migrations)=1
  AND EXISTS (SELECT 1 FROM public.schema_migrations WHERE version=1 AND name='initial_reconciliation_schema'
    AND checksum='5db10cca93e78e24ec8b32f0f9d278d04c7f4d2ffc7ed84cfbf6aa9cb7fbb4f9'),
  false) AS ready`;
