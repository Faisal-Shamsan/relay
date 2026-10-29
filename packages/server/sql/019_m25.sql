-- M25 cancellation races completion under the run lock.
-- A terminal run is returned unchanged. A winning cancel abandons unfinished attempts.

CREATE OR REPLACE FUNCTION relay_cancel_run(p_run_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_state text;
  v_output jsonb;
  v_error jsonb;
  v_fence bigint;
  v_tenant uuid := relay_guc_tenant();
  v_run_tenant uuid;
  v_now timestamptz;
  v_open integer;
  v_seq bigint;
  v_step_key text;
  v_attempt integer;
  v_attempt_fence bigint;
  v_command_pos bigint;
BEGIN
  SELECT state, output, error, current_fence, tenant_id
    INTO v_state, v_output, v_error, v_fence, v_run_tenant
  FROM runs WHERE id = p_run_id FOR UPDATE;
  IF NOT FOUND OR (v_tenant IS NOT NULL AND v_run_tenant IS DISTINCT FROM v_tenant) THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
  END IF;
  PERFORM 1 FROM run_queue WHERE run_id = p_run_id FOR UPDATE;
  SELECT clock_timestamp() INTO v_now;

  IF v_state IN ('completed', 'failed', 'cancelled') THEN
    RETURN jsonb_build_object('state', v_state, 'output', v_output, 'error', v_error);
  END IF;

  SELECT count(*) INTO v_open
  FROM step_attempts
  WHERE run_id = p_run_id AND state = 'running';
  v_seq := relay_alloc_events(p_run_id, v_open + 1);

  FOR v_step_key, v_attempt, v_attempt_fence, v_command_pos IN
    SELECT a.step_key, a.attempt, a.fence, s.command_pos
    FROM step_attempts a
    JOIN steps s ON s.run_id = a.run_id AND s.step_key = a.step_key
    WHERE a.run_id = p_run_id AND a.state = 'running'
    ORDER BY a.step_key, a.attempt
  LOOP
    INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, step_key, attempt, payload)
    VALUES (
      p_run_id, v_seq, v_fence, 'client', 'StepAttemptAbandoned', v_command_pos, v_step_key, v_attempt,
      jsonb_build_object(
        'schemaVersion', 1,
        'reason', 'cancelled',
        'oldFence', v_attempt_fence::text
      )
    );
    UPDATE step_attempts
    SET state = 'abandoned', ended_at = v_now
    WHERE run_id = p_run_id AND step_key = v_step_key AND attempt = v_attempt;
    UPDATE steps
    SET state = 'interrupted', retry_at = NULL
    WHERE run_id = p_run_id AND step_key = v_step_key AND state = 'running';
    v_seq := v_seq + 1;
  END LOOP;

  INSERT INTO run_events (run_id, seq, fence, actor, type, payload)
  VALUES (
    p_run_id, v_seq, v_fence, 'client', 'RunCancelled',
    jsonb_build_object('schemaVersion', 1)
  );
  UPDATE runs
  SET state = 'cancelled',
      output = NULL,
      error = NULL,
      completed_at = v_now
  WHERE id = p_run_id;
  DELETE FROM run_queue WHERE run_id = p_run_id;
  DELETE FROM wait_deadlines WHERE run_id = p_run_id;
  UPDATE worker_sessions
  SET active_run_id = NULL, active_fence = NULL, history_watermark = NULL
  WHERE active_run_id = p_run_id;
  PERFORM relay_append_stream_control(
    p_run_id,
    'run_terminal',
    NULL,
    NULL,
    jsonb_build_object('schemaVersion', 1, 'state', 'cancelled')
  );
  RETURN jsonb_build_object('state', 'cancelled', 'output', NULL, 'error', NULL);
END;
$$;

REVOKE ALL ON FUNCTION relay_cancel_run(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_cancel_run(uuid) TO relay_app;

DO $cancelpatch$
DECLARE
  v_def text;
  v_open text := '      AND NOT EXISTS (' || chr(10)
    || '        SELECT 1 FROM run_events e' || chr(10)
    || '        WHERE e.run_id = s.run_id AND e.type = ''RunFailed''' || chr(10)
    || '          AND e.payload->''error''->>''code'' = ''RecoveryBudgetExceeded''' || chr(10)
    || '      )' || chr(10)
    || '      AND s.state IS DISTINCT FROM ''running''';
  v_stream text := 'AND r.state IN (''completed'', ''failed'')';
  v_step text := 'WHEN started.step_key IS NOT NULL THEN ''running''';
BEGIN
  SELECT pg_get_functiondef('relay_verify_workflow_projections(uuid)'::regprocedure) INTO v_def;
  v_def := replace(v_def, chr(13), '');
  IF position(v_open IN v_def) = 0 THEN
    RAISE EXCEPTION 'verify step-open anchor missing';
  END IF;
  v_def := replace(
    v_def,
    v_open,
    '      AND NOT EXISTS (' || chr(10)
      || '        SELECT 1 FROM run_events e' || chr(10)
      || '        WHERE e.run_id = s.run_id AND e.type = ''RunFailed''' || chr(10)
      || '          AND e.payload->''error''->>''code'' = ''RecoveryBudgetExceeded''' || chr(10)
      || '      )' || chr(10)
      || '      AND NOT EXISTS (' || chr(10)
      || '        SELECT 1 FROM run_events e' || chr(10)
      || '        WHERE e.run_id = s.run_id AND e.type = ''RunCancelled''' || chr(10)
      || '      )' || chr(10)
      || '      AND s.state IS DISTINCT FROM ''running'''
  );
  EXECUTE v_def;

  SELECT pg_get_functiondef('relay_verify_stream(uuid)'::regprocedure) INTO v_def;
  v_def := replace(v_def, chr(13), '');
  IF position(v_stream IN v_def) = 0 THEN
    RAISE EXCEPTION 'stream terminal anchor missing';
  END IF;
  v_def := replace(v_def, v_stream, 'AND r.state IN (''completed'', ''failed'', ''cancelled'')');
  EXECUTE v_def;

  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'public' AND p.proname = 'relay_rebuild_projections';
  v_def := replace(v_def, chr(13), '');
  IF position(v_step IN v_def) = 0 THEN
    RAISE EXCEPTION 'rebuild interrupted step anchor missing';
  END IF;
  v_def := replace(
    v_def,
    v_step,
    'WHEN started.step_key IS NOT NULL AND EXISTS (' || chr(10)
      || '        SELECT 1 FROM run_events abandoned' || chr(10)
      || '        WHERE abandoned.run_id = c.run_id AND abandoned.step_key = child.step_key' || chr(10)
      || '          AND abandoned.type = ''StepAttemptAbandoned'' AND abandoned.seq > started.seq' || chr(10)
      || '      ) THEN ''interrupted''' || chr(10)
      || '      WHEN started.step_key IS NOT NULL THEN ''running'''
  );
  EXECUTE v_def;
END;
$cancelpatch$;
