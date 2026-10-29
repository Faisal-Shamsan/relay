-- M21 credentials and row security.
-- Hashes are SHA-256 of the local development keys. The raw keys are not stored.
-- A missing app.tenant_id fails closed for relay_app reads.
-- Transition checks apply only when that setting is present, so maintenance and
-- the existing in-process SQL tests keep their previous behavior.

CREATE TABLE IF NOT EXISTS api_credentials (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  key_hash text NOT NULL CHECK (key_hash ~ '^[0-9a-f]{64}$'),
  kind text NOT NULL CHECK (kind IN ('client', 'worker')),
  label text NOT NULL CHECK (length(label) > 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  revoked_at timestamptz,
  UNIQUE (key_hash)
);

INSERT INTO api_credentials (id, tenant_id, key_hash, kind, label)
VALUES
  (
    '22222222-2222-4222-8222-222222222221',
    '11111111-1111-4111-8111-111111111111',
    '9ec1153c5c4fe4e2f7c27731f8ca76522513b332181d18583db6a88169782768',
    'client',
    'local-dev-client'
  ),
  (
    '22222222-2222-4222-8222-222222222222',
    '11111111-1111-4111-8111-111111111111',
    '28bb7a776fc041003901a9f98f8ed7c69b8a113d1887695fc58d3fabb7d4120c',
    'worker',
    'local-dev-worker'
  )
ON CONFLICT (key_hash) DO NOTHING;

REVOKE ALL ON TABLE api_credentials FROM PUBLIC;
REVOKE ALL ON TABLE api_credentials FROM relay_app;

CREATE OR REPLACE FUNCTION relay_authenticate(p_key_hash text)
RETURNS TABLE (tenant_id uuid, kind text)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT c.tenant_id, c.kind
  FROM api_credentials c
  WHERE c.key_hash = p_key_hash
    AND c.revoked_at IS NULL
$$;

REVOKE ALL ON FUNCTION relay_authenticate(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_authenticate(text) TO relay_app;

CREATE OR REPLACE FUNCTION relay_guc_tenant()
RETURNS uuid
LANGUAGE plpgsql
STABLE
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_setting text := current_setting('app.tenant_id', true);
BEGIN
  IF v_setting IS NULL OR btrim(v_setting) = '' THEN
    RETURN NULL;
  END IF;
  RETURN v_setting::uuid;
EXCEPTION
  WHEN invalid_text_representation THEN
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION relay_guc_tenant() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_guc_tenant() TO relay_app;

CREATE OR REPLACE FUNCTION relay_run_visible(p_run_id uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT EXISTS (
    SELECT 1
    FROM runs r
    WHERE r.id = p_run_id
      AND r.tenant_id = relay_guc_tenant()
  )
$$;

REVOKE ALL ON FUNCTION relay_run_visible(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_run_visible(uuid) TO relay_app;

CREATE OR REPLACE FUNCTION relay_session_visible(p_session_id uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT EXISTS (
    SELECT 1
    FROM worker_sessions s
    WHERE s.id = p_session_id
      AND s.tenant_id = relay_guc_tenant()
  )
$$;

REVOKE ALL ON FUNCTION relay_session_visible(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_session_visible(uuid) TO relay_app;

CREATE OR REPLACE FUNCTION relay_enforce_tenant()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_tenant uuid := relay_guc_tenant();
  v_old uuid;
  v_new uuid;
  v_session uuid;
BEGIN
  IF v_tenant IS NULL THEN
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    END IF;
    RETURN NEW;
  END IF;

  IF TG_OP <> 'INSERT' THEN
    IF TG_TABLE_NAME = 'tenants' THEN
      v_old := OLD.id;
    ELSIF TG_TABLE_NAME IN ('workflow_versions', 'runs', 'worker_sessions', 'worker_session_versions', 'registration_receipts') THEN
      v_old := OLD.tenant_id;
    ELSIF TG_TABLE_NAME = 'worker_session_receipts' THEN
      v_session := OLD.worker_session_id;
      SELECT tenant_id INTO v_old FROM worker_sessions WHERE id = v_session;
    ELSE
      SELECT tenant_id INTO v_old FROM runs WHERE id = OLD.run_id;
    END IF;
    IF v_old IS DISTINCT FROM v_tenant THEN
      RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
    END IF;
  END IF;

  IF TG_OP <> 'DELETE' THEN
    IF TG_TABLE_NAME = 'tenants' THEN
      v_new := NEW.id;
    ELSIF TG_TABLE_NAME IN ('workflow_versions', 'runs', 'worker_sessions', 'worker_session_versions', 'registration_receipts') THEN
      v_new := NEW.tenant_id;
    ELSIF TG_TABLE_NAME = 'worker_session_receipts' THEN
      v_session := NEW.worker_session_id;
      SELECT tenant_id INTO v_new FROM worker_sessions WHERE id = v_session;
    ELSE
      SELECT tenant_id INTO v_new FROM runs WHERE id = NEW.run_id;
    END IF;
    IF v_new IS DISTINCT FROM v_tenant THEN
      RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
    END IF;
  END IF;

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION relay_enforce_tenant() FROM PUBLIC;

DO $$
DECLARE
  v_table text;
BEGIN
  FOREACH v_table IN ARRAY ARRAY[
    'tenants',
    'workflow_versions',
    'runs',
    'run_events',
    'commands',
    'steps',
    'step_attempts',
    'run_queue',
    'operation_receipts',
    'run_stream_events',
    'worker_sessions',
    'worker_session_versions',
    'worker_session_receipts',
    'registration_receipts'
  ]
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS relay_tenant_guard ON %I', v_table);
    EXECUTE format(
      'CREATE TRIGGER relay_tenant_guard BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION relay_enforce_tenant()',
      v_table
    );
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', v_table);
    EXECUTE format('DROP POLICY IF EXISTS relay_tenant_select ON %I', v_table);
  END LOOP;
END;
$$;

CREATE POLICY relay_tenant_select ON tenants
  FOR SELECT TO relay_app
  USING (id = relay_guc_tenant());

CREATE POLICY relay_tenant_select ON workflow_versions
  FOR SELECT TO relay_app
  USING (tenant_id = relay_guc_tenant());

CREATE POLICY relay_tenant_select ON runs
  FOR SELECT TO relay_app
  USING (tenant_id = relay_guc_tenant());

CREATE POLICY relay_tenant_select ON worker_sessions
  FOR SELECT TO relay_app
  USING (tenant_id = relay_guc_tenant());

CREATE POLICY relay_tenant_select ON worker_session_versions
  FOR SELECT TO relay_app
  USING (tenant_id = relay_guc_tenant());

CREATE POLICY relay_tenant_select ON registration_receipts
  FOR SELECT TO relay_app
  USING (tenant_id = relay_guc_tenant());

CREATE POLICY relay_tenant_select ON run_events
  FOR SELECT TO relay_app
  USING (relay_run_visible(run_id));

CREATE POLICY relay_tenant_select ON commands
  FOR SELECT TO relay_app
  USING (relay_run_visible(run_id));

CREATE POLICY relay_tenant_select ON steps
  FOR SELECT TO relay_app
  USING (relay_run_visible(run_id));

CREATE POLICY relay_tenant_select ON step_attempts
  FOR SELECT TO relay_app
  USING (relay_run_visible(run_id));

CREATE POLICY relay_tenant_select ON run_queue
  FOR SELECT TO relay_app
  USING (relay_run_visible(run_id));

CREATE POLICY relay_tenant_select ON operation_receipts
  FOR SELECT TO relay_app
  USING (relay_run_visible(run_id));

CREATE POLICY relay_tenant_select ON run_stream_events
  FOR SELECT TO relay_app
  USING (relay_run_visible(run_id));

CREATE POLICY relay_tenant_select ON worker_session_receipts
  FOR SELECT TO relay_app
  USING (relay_session_visible(worker_session_id));

CREATE OR REPLACE FUNCTION relay_lookup_receipt(
  p_run_id uuid,
  p_operation_id uuid,
  p_request_hash text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_hash text;
  v_response jsonb;
  v_tenant uuid := relay_guc_tenant();
  v_run_tenant uuid;
BEGIN
  IF v_tenant IS NOT NULL THEN
    SELECT tenant_id INTO v_run_tenant FROM runs WHERE id = p_run_id;
    IF v_run_tenant IS DISTINCT FROM v_tenant THEN
      RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
    END IF;
  END IF;
  SELECT request_hash, response INTO v_hash, v_response
  FROM operation_receipts
  WHERE run_id = p_run_id AND operation_id = p_operation_id;
  IF NOT FOUND THEN
    RETURN NULL;
  END IF;
  IF v_hash = p_request_hash THEN
    RETURN v_response;
  END IF;
  RAISE EXCEPTION 'IdempotencyConflict' USING DETAIL = 'operation id was reused with a different request';
END;
$$;

CREATE OR REPLACE FUNCTION relay_require_lease(
  p_run_id uuid,
  p_worker_session_id uuid,
  p_fence bigint,
  p_now timestamptz
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_run_state text;
  v_fence bigint;
  v_queue_state text;
  v_owner uuid;
  v_expires timestamptz;
  v_tenant uuid := relay_guc_tenant();
  v_run_tenant uuid;
  v_session_tenant uuid;
BEGIN
  IF v_tenant IS NOT NULL THEN
    SELECT tenant_id INTO v_run_tenant FROM runs WHERE id = p_run_id;
    SELECT tenant_id INTO v_session_tenant FROM worker_sessions WHERE id = p_worker_session_id;
    IF v_run_tenant IS DISTINCT FROM v_tenant OR v_session_tenant IS DISTINCT FROM v_tenant THEN
      RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
    END IF;
  END IF;

  SELECT state, current_fence INTO v_run_state, v_fence FROM runs WHERE id = p_run_id;
  SELECT state, lease_owner, lease_expires_at
    INTO v_queue_state, v_owner, v_expires
  FROM run_queue WHERE run_id = p_run_id;

  IF v_run_state IN ('completed', 'failed', 'cancelled') THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run is terminal';
  END IF;
  IF v_queue_state IS DISTINCT FROM 'leased' OR v_owner IS DISTINCT FROM p_worker_session_id THEN
    RAISE EXCEPTION 'LeaseLost' USING DETAIL = 'worker does not own the lease';
  END IF;
  IF v_fence IS DISTINCT FROM p_fence THEN
    RAISE EXCEPTION 'FenceViolation' USING DETAIL = 'fence does not match';
  END IF;
  IF v_expires IS NULL OR NOT (v_expires > p_now) THEN
    RAISE EXCEPTION 'LeaseLost' USING DETAIL = 'lease is expired';
  END IF;
END;
$$;
