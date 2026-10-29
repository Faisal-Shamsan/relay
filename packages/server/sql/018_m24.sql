-- M24 named waits. One signal or one timeout resolves the wait. The deadline is not recomputed.

CREATE TABLE IF NOT EXISTS wait_deadlines (
  run_id uuid NOT NULL REFERENCES runs(id),
  command_pos bigint NOT NULL CHECK (command_pos >= 0),
  wake_at timestamptz NOT NULL,
  PRIMARY KEY (run_id, command_pos)
);

CREATE INDEX IF NOT EXISTS wait_due ON wait_deadlines (wake_at, run_id);

DROP TRIGGER IF EXISTS relay_tenant_guard ON wait_deadlines;
CREATE TRIGGER relay_tenant_guard
  BEFORE INSERT OR UPDATE OR DELETE ON wait_deadlines
  FOR EACH ROW EXECUTE FUNCTION relay_enforce_tenant();

ALTER TABLE wait_deadlines ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS relay_tenant_select ON wait_deadlines;
CREATE POLICY relay_tenant_select ON wait_deadlines
  FOR SELECT TO relay_app
  USING (relay_run_visible(run_id));

GRANT SELECT ON TABLE wait_deadlines TO relay_app;

CREATE UNIQUE INDEX IF NOT EXISTS run_one_wait_resolved
  ON run_events (run_id, command_pos)
  WHERE type = 'WaitResolved';

CREATE OR REPLACE FUNCTION relay_resolve_open_wait(
  p_run_id uuid,
  p_now timestamptz,
  p_outcome text,
  p_ingress bigint,
  p_payload jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_pos bigint;
  v_name text;
  v_fence bigint;
  v_seq bigint;
  v_result jsonb;
  v_signal bigint;
  v_signal_payload jsonb;
BEGIN
  SELECT c.command_pos, c.command_key INTO v_pos, v_name
  FROM commands c
  WHERE c.run_id = p_run_id AND c.kind = 'wait' AND c.outcome IS NULL
  ORDER BY c.command_pos
  LIMIT 1
  FOR UPDATE;
  IF v_pos IS NULL THEN
    RETURN NULL;
  END IF;
  IF EXISTS (
    SELECT 1 FROM run_events
    WHERE run_id = p_run_id AND command_pos = v_pos AND type = 'WaitResolved'
  ) THEN
    RETURN NULL;
  END IF;

  IF p_outcome = 'signal' THEN
    SELECT ingress_seq, payload INTO v_signal, v_signal_payload
    FROM signal_inbox
    WHERE run_id = p_run_id
      AND name = v_name
      AND consumed_by_command_pos IS NULL
    ORDER BY ingress_seq
    LIMIT 1
    FOR UPDATE;
    IF v_signal IS NULL THEN
      RETURN NULL;
    END IF;
    UPDATE signal_inbox
    SET consumed_by_command_pos = v_pos
    WHERE run_id = p_run_id AND ingress_seq = v_signal;
    v_result := jsonb_build_object(
      'outcome', 'signal',
      'payload', v_signal_payload,
      'ingressSeq', v_signal::text
    );
  ELSE
    v_result := jsonb_build_object('outcome', 'timeout');
  END IF;

  SELECT current_fence INTO v_fence FROM runs WHERE id = p_run_id;
  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
  VALUES (
    p_run_id, v_seq, v_fence, 'system', 'WaitResolved', v_pos,
    jsonb_build_object('schemaVersion', 1, 'result', v_result)
  );
  UPDATE commands SET outcome = v_result WHERE run_id = p_run_id AND command_pos = v_pos;
  DELETE FROM wait_deadlines WHERE run_id = p_run_id AND command_pos = v_pos;
  UPDATE runs SET state = 'pending' WHERE id = p_run_id;
  UPDATE run_queue
  SET state = 'ready',
      lease_owner = NULL,
      lease_expires_at = NULL,
      available_at = p_now
  WHERE run_id = p_run_id;
  RETURN v_result;
END;
$$;

REVOKE ALL ON FUNCTION relay_resolve_open_wait(uuid, timestamptz, text, bigint, jsonb) FROM PUBLIC;

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
  v_now timestamptz;
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
  SELECT clock_timestamp() INTO v_now;
  PERFORM relay_resolve_open_wait(p_run_id, v_now, 'signal', v_seq, p_payload);
  RETURN jsonb_build_object(
    'accepted', true,
    'replayed', false,
    'ingressSeq', v_seq::text,
    'name', p_name
  );
END;
$$;

CREATE OR REPLACE FUNCTION relay_declare_wait(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_key text,
  p_timeout bigint,
  p_expected_pos bigint,
  p_request_hash text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_now timestamptz;
  v_wake timestamptz;
  v_wake_text text;
  v_receipt jsonb;
  v_existing_kind text;
  v_existing_key text;
  v_existing_timeout text;
  v_existing_outcome jsonb;
  v_descriptor jsonb;
  v_seq bigint;
  v_response jsonb;
  v_signal bigint;
  v_signal_payload jsonb;
  v_result jsonb;
BEGIN
  IF p_key IS NULL OR octet_length(p_key) < 1 OR octet_length(p_key) > 256 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'wait name is invalid';
  END IF;
  IF p_timeout IS NULL OR p_timeout < 0 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'wait timeout is invalid';
  END IF;
  IF p_expected_pos IS NULL OR p_expected_pos < 0 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'command position is invalid';
  END IF;

  PERFORM 1 FROM runs WHERE id = p_run_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
  END IF;
  PERFORM 1 FROM run_queue WHERE run_id = p_run_id FOR UPDATE;
  SELECT clock_timestamp() INTO v_now;
  v_receipt := relay_lookup_receipt(p_run_id, p_operation_id, p_request_hash);
  IF v_receipt IS NOT NULL THEN
    RETURN v_receipt;
  END IF;
  PERFORM relay_require_lease(p_run_id, p_worker_session_id, p_fence, v_now);

  SELECT c.kind, c.command_key, c.descriptor->>'timeoutMs', c.outcome
    INTO v_existing_kind, v_existing_key, v_existing_timeout, v_existing_outcome
  FROM commands c
  WHERE c.run_id = p_run_id AND c.command_pos = p_expected_pos;
  IF FOUND THEN
    IF v_existing_kind IS DISTINCT FROM 'wait'
      OR v_existing_key IS DISTINCT FROM p_key
      OR v_existing_timeout IS DISTINCT FROM p_timeout::text THEN
      RAISE EXCEPTION 'Nondeterminism' USING DETAIL = 'wait command does not match';
    END IF;
    v_response := jsonb_build_object(
      'commandPos', p_expected_pos::text,
      'timeoutMs', p_timeout,
      'suspended', v_existing_outcome IS NULL,
      'result', v_existing_outcome
    );
    INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
    VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
    RETURN v_response;
  END IF;

  IF p_expected_pos IS DISTINCT FROM (SELECT next_command_pos FROM runs WHERE id = p_run_id) THEN
    RAISE EXCEPTION 'Nondeterminism' USING DETAIL = 'expected command position does not match';
  END IF;
  IF EXISTS (
    SELECT 1 FROM commands WHERE run_id = p_run_id AND kind = 'wait' AND command_key = p_key
  ) THEN
    RAISE EXCEPTION 'DuplicateStepKey' USING DETAIL = 'wait name already exists';
  END IF;

  SELECT ingress_seq, payload INTO v_signal, v_signal_payload
  FROM signal_inbox
  WHERE run_id = p_run_id AND name = p_key AND consumed_by_command_pos IS NULL
  ORDER BY ingress_seq
  LIMIT 1
  FOR UPDATE;

  v_descriptor := jsonb_build_object('kind', 'wait', 'key', p_key, 'timeoutMs', p_timeout);
  IF v_signal IS NOT NULL OR p_timeout = 0 THEN
    IF v_signal IS NOT NULL THEN
      v_result := jsonb_build_object(
        'outcome', 'signal',
        'payload', v_signal_payload,
        'ingressSeq', v_signal::text
      );
    ELSE
      v_result := jsonb_build_object('outcome', 'timeout');
    END IF;
    v_seq := relay_alloc_events(p_run_id, 2);
    INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
    VALUES (
      p_run_id, v_seq, p_fence, 'worker', 'CommandRecorded', p_expected_pos,
      jsonb_build_object('schemaVersion', 1, 'descriptor', v_descriptor)
    );
    INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
    VALUES (
      p_run_id, v_seq + 1, p_fence, 'system', 'WaitResolved', p_expected_pos,
      jsonb_build_object('schemaVersion', 1, 'result', v_result)
    );
    INSERT INTO commands (run_id, command_pos, kind, command_key, descriptor, outcome, recorded_event_seq)
    VALUES (p_run_id, p_expected_pos, 'wait', p_key, v_descriptor, v_result, v_seq);
    IF v_signal IS NOT NULL THEN
      UPDATE signal_inbox SET consumed_by_command_pos = p_expected_pos
      WHERE run_id = p_run_id AND ingress_seq = v_signal;
    END IF;
    UPDATE runs SET next_command_pos = next_command_pos + 1 WHERE id = p_run_id;
    v_response := jsonb_build_object(
      'commandPos', p_expected_pos::text,
      'timeoutMs', p_timeout,
      'suspended', false,
      'result', v_result
    );
  ELSE
    BEGIN
      v_wake := date_trunc('milliseconds', v_now) + (p_timeout * interval '1 millisecond');
    EXCEPTION
      WHEN datetime_field_overflow OR interval_field_overflow THEN
        RAISE EXCEPTION 'Serialization' USING DETAIL = 'wait timeout is too large';
    END;
    v_wake_text := to_char(v_wake AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');
    v_seq := relay_alloc_events(p_run_id, 2);
    INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
    VALUES (
      p_run_id, v_seq, p_fence, 'worker', 'CommandRecorded', p_expected_pos,
      jsonb_build_object('schemaVersion', 1, 'descriptor', v_descriptor)
    );
    INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
    VALUES (
      p_run_id, v_seq + 1, p_fence, 'worker', 'RunSuspended', p_expected_pos,
      jsonb_build_object('schemaVersion', 1, 'reason', 'wait', 'wakeAt', v_wake_text, 'key', p_key)
    );
    INSERT INTO commands (run_id, command_pos, kind, command_key, descriptor, recorded_event_seq)
    VALUES (p_run_id, p_expected_pos, 'wait', p_key, v_descriptor, v_seq);
    INSERT INTO wait_deadlines (run_id, command_pos, wake_at)
    VALUES (p_run_id, p_expected_pos, v_wake);
    UPDATE runs
    SET next_command_pos = next_command_pos + 1,
        state = 'waiting'
    WHERE id = p_run_id;
    UPDATE run_queue
    SET state = 'parked',
        lease_owner = NULL,
        lease_expires_at = NULL,
        available_at = NULL
    WHERE run_id = p_run_id;
    UPDATE worker_sessions
    SET active_run_id = NULL, active_fence = NULL, history_watermark = NULL
    WHERE active_run_id = p_run_id;
    v_response := jsonb_build_object(
      'commandPos', p_expected_pos::text,
      'timeoutMs', p_timeout,
      'suspended', true,
      'wakeAt', v_wake_text
    );
  END IF;

  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
END;
$$;

REVOKE ALL ON FUNCTION relay_declare_wait(uuid, uuid, uuid, bigint, text, bigint, bigint, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_declare_wait(uuid, uuid, uuid, bigint, text, bigint, bigint, text) TO relay_app;

CREATE OR REPLACE FUNCTION relay_fire_one_wait()
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_run uuid;
  v_now timestamptz;
  v_wake timestamptz;
  v_state text;
  v_queue text;
  v_resolved jsonb;
BEGIN
  SELECT r.id, d.wake_at INTO v_run, v_wake
  FROM wait_deadlines d
  JOIN runs r ON r.id = d.run_id
  JOIN run_queue q ON q.run_id = r.id
  WHERE r.state = 'waiting'
    AND q.state = 'parked'
    AND d.wake_at <= clock_timestamp()
  ORDER BY d.wake_at, d.run_id
  FOR UPDATE OF r SKIP LOCKED
  LIMIT 1;

  IF v_run IS NULL THEN
    RETURN 'none';
  END IF;

  PERFORM 1 FROM run_queue WHERE run_id = v_run FOR UPDATE;
  SELECT clock_timestamp() INTO v_now;
  SELECT state INTO v_state FROM runs WHERE id = v_run;
  SELECT state INTO v_queue FROM run_queue WHERE run_id = v_run;
  IF v_state IS DISTINCT FROM 'waiting'
    OR v_queue IS DISTINCT FROM 'parked'
    OR v_wake IS NULL
    OR v_wake > v_now THEN
    RETURN 'skipped';
  END IF;

  v_resolved := relay_resolve_open_wait(v_run, v_now, 'timeout', NULL, NULL);
  IF v_resolved IS NULL THEN
    RETURN 'skipped';
  END IF;
  RETURN 'fired';
END;
$$;

REVOKE ALL ON FUNCTION relay_fire_one_wait() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_fire_one_wait() TO relay_app;

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

  UPDATE signal_inbox i
  SET consumed_by_command_pos = resolved.command_pos
  FROM run_events resolved
  WHERE i.run_id = p_run_id
    AND resolved.run_id = i.run_id
    AND resolved.type = 'WaitResolved'
    AND resolved.payload->'result'->>'outcome' = 'signal'
    AND i.ingress_seq = (resolved.payload->'result'->>'ingressSeq')::bigint;

  DELETE FROM wait_deadlines WHERE run_id = p_run_id;
  INSERT INTO wait_deadlines (run_id, command_pos, wake_at)
  SELECT e.run_id, e.command_pos, (e.payload->>'wakeAt')::timestamptz
  FROM run_events e
  WHERE e.run_id = p_run_id
    AND e.type = 'RunSuspended'
    AND e.payload->>'reason' = 'wait'
    AND NOT EXISTS (
      SELECT 1 FROM run_events resolved
      WHERE resolved.run_id = e.run_id
        AND resolved.command_pos = e.command_pos
        AND resolved.type = 'WaitResolved'
    );
END;
$$;

DO $waitpatch$
DECLARE
  v_def text;
  v_needle text := '      WHEN e.payload->''descriptor''->>''kind'' = ''sleep'' AND fired.seq IS NOT NULL' || chr(10)
    || '        THEN to_jsonb(scheduled.payload->>''wakeAt'')' || chr(10)
    || '      ELSE NULL';
  v_join text := '  LEFT JOIN run_events scheduled' || chr(10)
    || '    ON scheduled.run_id = e.run_id AND scheduled.command_pos = e.command_pos AND scheduled.type = ''TimerScheduled''';
  v_state text := '      ) THEN ''sleeping''';
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'public' AND p.proname = 'relay_rebuild_projections';
  v_def := replace(v_def, chr(13), '');
  IF position(v_needle IN v_def) = 0 THEN
    RAISE EXCEPTION 'rebuild wait outcome anchor missing';
  END IF;
  v_def := replace(
    v_def,
    v_needle,
    '      WHEN e.payload->''descriptor''->>''kind'' = ''sleep'' AND fired.seq IS NOT NULL' || chr(10)
      || '        THEN to_jsonb(scheduled.payload->>''wakeAt'')' || chr(10)
      || '      WHEN e.payload->''descriptor''->>''kind'' = ''wait'' AND resolved.seq IS NOT NULL' || chr(10)
      || '        THEN resolved.payload->''result''' || chr(10)
      || '      ELSE NULL'
  );
  IF position(v_join IN v_def) = 0 THEN
    RAISE EXCEPTION 'rebuild timer join anchor missing';
  END IF;
  v_def := replace(
    v_def,
    v_join,
    v_join || chr(10)
      || '  LEFT JOIN run_events resolved' || chr(10)
      || '    ON resolved.run_id = e.run_id AND resolved.command_pos = e.command_pos AND resolved.type = ''WaitResolved'''
  );
  IF position(v_state IN v_def) = 0 THEN
    RAISE EXCEPTION 'rebuild sleeping state anchor missing';
  END IF;
  v_def := replace(
    v_def,
    v_state,
    '      ) THEN ''sleeping''' || chr(10)
      || '      WHEN EXISTS (' || chr(10)
      || '        SELECT 1 FROM run_events suspended' || chr(10)
      || '        WHERE suspended.run_id = r.id' || chr(10)
      || '          AND suspended.type = ''RunSuspended''' || chr(10)
      || '          AND suspended.payload->>''reason'' = ''wait''' || chr(10)
      || '          AND NOT EXISTS (' || chr(10)
      || '            SELECT 1 FROM run_events done' || chr(10)
      || '            WHERE done.run_id = suspended.run_id' || chr(10)
      || '              AND done.command_pos = suspended.command_pos' || chr(10)
      || '              AND done.type = ''WaitResolved''' || chr(10)
      || '          )' || chr(10)
      || '      ) THEN ''waiting'''
  );
  EXECUTE v_def;
END;
$waitpatch$;

DO $verifypatch$
DECLARE
  v_def text;
  v_needle text := '          ) THEN ''sleeping''';
BEGIN
  SELECT pg_get_functiondef('relay_verify_workflow_projections(uuid)'::regprocedure) INTO v_def;
  v_def := replace(v_def, chr(13), '');
  IF position(v_needle IN v_def) = 0 THEN
    RAISE EXCEPTION 'verify sleeping anchor missing';
  END IF;
  v_def := replace(
    v_def,
    v_needle,
    v_needle || chr(10)
      || '          WHEN EXISTS (' || chr(10)
      || '            SELECT 1 FROM run_events suspended' || chr(10)
      || '            WHERE suspended.run_id = r.id' || chr(10)
      || '              AND suspended.type = ''RunSuspended''' || chr(10)
      || '              AND suspended.payload->>''reason'' = ''wait''' || chr(10)
      || '              AND NOT EXISTS (' || chr(10)
      || '                SELECT 1 FROM run_events done' || chr(10)
      || '                WHERE done.run_id = suspended.run_id' || chr(10)
      || '                  AND done.command_pos = suspended.command_pos' || chr(10)
      || '                  AND done.type = ''WaitResolved''' || chr(10)
      || '              )' || chr(10)
      || '          ) THEN ''waiting'''
  );
  EXECUTE v_def;
END;
$verifypatch$;
