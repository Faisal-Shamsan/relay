-- M30 explicit redrive. A terminal run stays terminal. The new run is a different id.

CREATE TABLE IF NOT EXISTS run_redrives (
  source_run_id uuid NOT NULL REFERENCES runs(id),
  idempotency_key text NOT NULL CHECK (octet_length(idempotency_key) BETWEEN 1 AND 256),
  request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
  new_run_id uuid NOT NULL REFERENCES runs(id),
  PRIMARY KEY (source_run_id, idempotency_key)
);

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
    ELSIF TG_TABLE_NAME = 'run_redrives' THEN
      SELECT tenant_id INTO v_old FROM runs WHERE id = OLD.source_run_id;
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
    ELSIF TG_TABLE_NAME = 'run_redrives' THEN
      SELECT tenant_id INTO v_new FROM runs WHERE id = NEW.source_run_id;
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

DROP TRIGGER IF EXISTS relay_tenant_guard ON run_redrives;
CREATE TRIGGER relay_tenant_guard
  BEFORE INSERT OR UPDATE OR DELETE ON run_redrives
  FOR EACH ROW EXECUTE FUNCTION relay_enforce_tenant();

ALTER TABLE run_redrives ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS relay_tenant_select ON run_redrives;
CREATE POLICY relay_tenant_select ON run_redrives
  FOR SELECT TO relay_app
  USING (relay_run_visible(source_run_id));

REVOKE ALL ON TABLE run_redrives FROM PUBLIC;
GRANT SELECT ON TABLE run_redrives TO relay_app;

CREATE OR REPLACE FUNCTION relay_redrive_run(
  p_source_run_id uuid,
  p_idempotency_key text,
  p_request_hash text,
  p_input jsonb,
  p_input_present boolean,
  p_trigger_hash text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_guc uuid := relay_guc_tenant();
  v_state text;
  v_tenant uuid;
  v_version_id uuid;
  v_stored_input jsonb;
  v_stored_hash text;
  v_name text;
  v_version text;
  v_existing_run uuid;
  v_existing_hash text;
  v_input jsonb;
  v_trigger text;
  v_new_run uuid;
  v_now timestamptz;
  v_seq bigint;
BEGIN
  IF p_idempotency_key IS NULL
     OR octet_length(p_idempotency_key) < 1
     OR octet_length(p_idempotency_key) > 256 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'idempotency key length is invalid';
  END IF;
  IF p_request_hash IS NULL OR p_request_hash !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'request hash is invalid';
  END IF;

  SELECT r.state, r.tenant_id, r.workflow_version_id, r.input, r.trigger_hash, wv.name, wv.version
    INTO v_state, v_tenant, v_version_id, v_stored_input, v_stored_hash, v_name, v_version
  FROM runs r
  JOIN workflow_versions wv ON wv.id = r.workflow_version_id
  WHERE r.id = p_source_run_id;

  IF NOT FOUND OR (v_guc IS NOT NULL AND v_tenant IS DISTINCT FROM v_guc) THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
  END IF;

  SELECT new_run_id, request_hash
    INTO v_existing_run, v_existing_hash
  FROM run_redrives
  WHERE source_run_id = p_source_run_id AND idempotency_key = p_idempotency_key
  FOR UPDATE;

  IF FOUND THEN
    IF v_existing_hash IS DISTINCT FROM p_request_hash THEN
      RAISE EXCEPTION 'IdempotencyConflict'
        USING DETAIL = 'redrive idempotency key was reused with a different body';
    END IF;
    RETURN jsonb_build_object('runId', v_existing_run, 'created', false);
  END IF;

  IF v_state NOT IN ('completed', 'failed', 'cancelled') THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run is not terminal';
  END IF;

  v_input := CASE WHEN p_input_present THEN p_input ELSE v_stored_input END;
  v_trigger := CASE WHEN p_input_present THEN p_trigger_hash ELSE v_stored_hash END;
  IF v_trigger IS NULL OR v_trigger !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'trigger hash is invalid';
  END IF;

  v_new_run := gen_random_uuid();
  BEGIN
    INSERT INTO runs (
      id, tenant_id, workflow_version_id, state, input, trigger_hash, idempotency_key, current_fence
    ) VALUES (
      v_new_run, v_tenant, v_version_id, 'pending', v_input, v_trigger, NULL, 0
    );
    SELECT clock_timestamp() INTO v_now;
    v_seq := relay_alloc_events(v_new_run, 1);
    INSERT INTO run_events (run_id, seq, fence, actor, type, payload, created_at)
    VALUES (
      v_new_run, v_seq, 0, 'client', 'RunCreated',
      jsonb_build_object(
        'schemaVersion', 1,
        'tenantId', v_tenant,
        'workflowName', v_name,
        'workflowVersion', v_version,
        'input', v_input,
        'triggerHash', v_trigger,
        'idempotencyKey', NULL
      ),
      v_now
    );
    INSERT INTO run_queue (run_id, state, priority, available_at)
    VALUES (v_new_run, 'ready', 0, v_now);
    INSERT INTO run_redrives (source_run_id, idempotency_key, request_hash, new_run_id)
    VALUES (p_source_run_id, p_idempotency_key, p_request_hash, v_new_run);
  EXCEPTION
    WHEN unique_violation THEN
      SELECT new_run_id, request_hash
        INTO v_existing_run, v_existing_hash
      FROM run_redrives
      WHERE source_run_id = p_source_run_id AND idempotency_key = p_idempotency_key;
      IF NOT FOUND THEN
        RAISE;
      END IF;
      IF v_existing_hash IS DISTINCT FROM p_request_hash THEN
        RAISE EXCEPTION 'IdempotencyConflict'
          USING DETAIL = 'redrive idempotency key was reused with a different body';
      END IF;
      RETURN jsonb_build_object('runId', v_existing_run, 'created', false);
  END;

  RETURN jsonb_build_object('runId', v_new_run, 'created', true);
END;
$$;

REVOKE ALL ON FUNCTION relay_redrive_run(uuid, text, text, jsonb, boolean, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_redrive_run(uuid, text, text, jsonb, boolean, text) TO relay_app;
