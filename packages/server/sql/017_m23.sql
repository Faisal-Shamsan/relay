-- M23 durable signal inbox. A signal is history. The inbox row is a projection.

CREATE TABLE IF NOT EXISTS signal_inbox (
  run_id uuid NOT NULL REFERENCES runs(id),
  ingress_seq bigint NOT NULL CHECK (ingress_seq >= 1),
  name text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 256),
  payload jsonb NOT NULL,
  payload_hash text NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
  idempotency_key text NOT NULL CHECK (octet_length(idempotency_key) BETWEEN 1 AND 256),
  consumed_by_command_pos bigint CHECK (consumed_by_command_pos >= 0),
  PRIMARY KEY (run_id, ingress_seq),
  UNIQUE (run_id, idempotency_key),
  FOREIGN KEY (run_id, ingress_seq) REFERENCES run_events(run_id, seq)
);

DROP TRIGGER IF EXISTS relay_tenant_guard ON signal_inbox;
CREATE TRIGGER relay_tenant_guard
  BEFORE INSERT OR UPDATE OR DELETE ON signal_inbox
  FOR EACH ROW EXECUTE FUNCTION relay_enforce_tenant();

ALTER TABLE signal_inbox ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS relay_tenant_select ON signal_inbox;
CREATE POLICY relay_tenant_select ON signal_inbox
  FOR SELECT TO relay_app
  USING (relay_run_visible(run_id));

GRANT SELECT ON TABLE signal_inbox TO relay_app;

CREATE OR REPLACE FUNCTION relay_accept_signal(
  p_run_id uuid,
  p_name text,
  p_payload jsonb,
  p_payload_hash text,
  p_idempotency_key text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_state text;
  v_fence bigint;
  v_seq bigint;
  v_existing_seq bigint;
  v_existing_hash text;
  v_tenant uuid := relay_guc_tenant();
  v_run_tenant uuid;
BEGIN
  IF p_name IS NULL OR octet_length(p_name) < 1 OR octet_length(p_name) > 256 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'signal name length is invalid';
  END IF;
  IF p_idempotency_key IS NULL OR octet_length(p_idempotency_key) < 1 OR octet_length(p_idempotency_key) > 256 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'idempotency key length is invalid';
  END IF;
  IF p_payload_hash !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'signal payload hash is invalid';
  END IF;

  SELECT state, current_fence, tenant_id INTO v_state, v_fence, v_run_tenant
  FROM runs WHERE id = p_run_id FOR UPDATE;
  IF NOT FOUND OR (v_tenant IS NOT NULL AND v_run_tenant IS DISTINCT FROM v_tenant) THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
  END IF;
  IF v_state IN ('completed', 'failed', 'cancelled') THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run is terminal';
  END IF;

  SELECT ingress_seq, payload_hash INTO v_existing_seq, v_existing_hash
  FROM signal_inbox
  WHERE run_id = p_run_id AND idempotency_key = p_idempotency_key;
  IF FOUND THEN
    IF v_existing_hash IS DISTINCT FROM p_payload_hash THEN
      RAISE EXCEPTION 'IdempotencyConflict' USING DETAIL = 'signal idempotency key was reused with a different payload';
    END IF;
    RETURN jsonb_build_object(
      'accepted', true,
      'replayed', true,
      'ingressSeq', v_existing_seq::text,
      'name', p_name
    );
  END IF;

  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, payload)
  VALUES (
    p_run_id, v_seq, v_fence, 'client', 'SignalReceived',
    jsonb_build_object(
      'schemaVersion', 1,
      'name', p_name,
      'payload', p_payload,
      'payloadHash', p_payload_hash,
      'idempotencyKey', p_idempotency_key
    )
  );
  INSERT INTO signal_inbox (run_id, ingress_seq, name, payload, payload_hash, idempotency_key)
  VALUES (p_run_id, v_seq, p_name, p_payload, p_payload_hash, p_idempotency_key);
  RETURN jsonb_build_object(
    'accepted', true,
    'replayed', false,
    'ingressSeq', v_seq::text,
    'name', p_name
  );
END;
$$;

REVOKE ALL ON FUNCTION relay_accept_signal(uuid, text, jsonb, text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_accept_signal(uuid, text, jsonb, text, text) TO relay_app;

CREATE OR REPLACE FUNCTION relay_rebuild_inbox(p_run_id uuid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
BEGIN
  DELETE FROM signal_inbox WHERE run_id = p_run_id;
  INSERT INTO signal_inbox (run_id, ingress_seq, name, payload, payload_hash, idempotency_key, consumed_by_command_pos)
  SELECT
    e.run_id,
    e.seq,
    e.payload->>'name',
    e.payload->'payload',
    e.payload->>'payloadHash',
    e.payload->>'idempotencyKey',
    NULL
  FROM run_events e
  WHERE e.run_id = p_run_id
    AND e.type = 'SignalReceived';
END;
$$;

REVOKE ALL ON FUNCTION relay_rebuild_inbox(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_rebuild_inbox(uuid) FROM relay_app;

DO $patch$
DECLARE
  v_def text;
  v_needle text := '  v_check := relay_verify_projections(p_run_id);';
  v_replacement text := E'  PERFORM relay_rebuild_inbox(p_run_id);\n  v_check := relay_verify_projections(p_run_id);';
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'public' AND p.proname = 'relay_rebuild_projections';
  v_def := replace(v_def, chr(13), '');
  IF position(v_needle in v_def) = 0 THEN
    RAISE EXCEPTION 'rebuild inbox anchor missing';
  END IF;
  EXECUTE replace(v_def, v_needle, v_replacement);
END;
$patch$;
