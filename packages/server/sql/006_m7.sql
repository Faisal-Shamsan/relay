-- M7 Phase 1 hooks and read-only projection check.
-- Hooks use the M3 helper. The HTTP server does not set relay.failpoint.
-- relay_verify_projections reads history and projections. It does not repair them.

CREATE OR REPLACE FUNCTION relay_m7_patch(p_name text, p_needle text, p_replacement text)
RETURNS void
LANGUAGE plpgsql
AS $m7$
DECLARE
  v_def text;
  v_count integer;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'public' AND p.proname = p_name;
  IF v_def IS NULL THEN
    RAISE EXCEPTION 'M7 hook target missing: %', p_name;
  END IF;
  v_def := replace(v_def, chr(13), '');
  p_needle := replace(p_needle, chr(13), '');
  p_replacement := replace(p_replacement, chr(13), '');
  v_count := (length(v_def) - length(replace(v_def, p_needle, ''))) / length(p_needle);
  IF v_count <> 1 THEN
    RAISE EXCEPTION 'M7 hook anchor count % in %', v_count, p_name;
  END IF;
  EXECUTE replace(v_def, p_needle, p_replacement);
END;
$m7$;

SELECT relay_m7_patch(
  'relay_grant_lease',
  $needle$  PERFORM 1 FROM run_queue WHERE run_id = v_run_id FOR UPDATE;
  SELECT state INTO v_queue_state FROM run_queue WHERE run_id = v_run_id;
$needle$,
  $replace$  PERFORM 1 FROM run_queue WHERE run_id = v_run_id FOR UPDATE;
  PERFORM relay_test_failpoint('lease.after-lock-before-validation');
  SELECT state INTO v_queue_state FROM run_queue WHERE run_id = v_run_id;
$replace$
);

SELECT relay_m7_patch(
  'relay_grant_lease',
  $needle$  INSERT INTO worker_session_receipts (worker_session_id, operation_id, request_hash, response)
  VALUES (p_worker_session_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
$needle$,
  $replace$  INSERT INTO worker_session_receipts (worker_session_id, operation_id, request_hash, response)
  VALUES (p_worker_session_id, p_operation_id, p_request_hash, v_response);
  PERFORM relay_test_failpoint('lease.after-update-before-commit');
  RETURN v_response;
$replace$
);

SELECT relay_m7_patch(
  'relay_heartbeat',
  $needle$  UPDATE run_queue
  SET lease_expires_at = v_deadline
  WHERE run_id = p_run_id;
$needle$,
  $replace$  UPDATE run_queue
  SET lease_expires_at = v_deadline
  WHERE run_id = p_run_id;
  PERFORM relay_test_failpoint('heartbeat.after-update-before-commit');
$replace$
);

SELECT relay_m7_patch(
  'relay_declare_and_start',
  $needle$  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
$needle$,
  $replace$  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  PERFORM relay_test_failpoint('command.after-declaration-before-commit');
  RETURN v_response;
$replace$
);

SELECT relay_m7_patch(
  'relay_declare_parallel',
  $needle$  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
$needle$,
  $replace$  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  PERFORM relay_test_failpoint('command.after-declaration-before-commit');
  RETURN v_response;
$replace$
);

SELECT relay_m7_patch(
  'relay_complete_step',
  $needle$  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
$needle$,
  $replace$  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  PERFORM relay_test_failpoint('step.before-completion-commit');
  RETURN v_response;
$replace$
);

CREATE OR REPLACE FUNCTION relay_m7_require(p_name text, p_hook text)
RETURNS void
LANGUAGE plpgsql
AS $m7$
DECLARE
  v_def text;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'public' AND p.proname = p_name;
  IF v_def IS NULL OR position(p_hook in v_def) = 0 THEN
    RAISE EXCEPTION 'M7 hook % missing from %', p_hook, p_name;
  END IF;
END;
$m7$;

SELECT relay_m7_require('relay_grant_lease', 'lease.after-lock-before-validation');
SELECT relay_m7_require('relay_grant_lease', 'lease.after-update-before-commit');
SELECT relay_m7_require('relay_heartbeat', 'heartbeat.after-update-before-commit');
SELECT relay_m7_require('relay_declare_and_start', 'command.after-declaration-before-commit');
SELECT relay_m7_require('relay_declare_parallel', 'command.after-declaration-before-commit');
SELECT relay_m7_require('relay_complete_step', 'step.before-completion-commit');

DROP FUNCTION relay_m7_patch(text, text, text);
DROP FUNCTION relay_m7_require(text, text);

CREATE OR REPLACE FUNCTION relay_verify_projections(p_run_id uuid)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $verify$
  WITH mismatches AS (
    SELECT 'run-missing'::text AS kind, 'run not found'::text AS detail
    WHERE NOT EXISTS (SELECT 1 FROM runs WHERE id = p_run_id)
    UNION ALL
    SELECT 'run-input', r.id::text
    FROM runs r
    JOIN run_events e ON e.run_id = r.id AND e.type = 'RunCreated'
    WHERE r.id = p_run_id AND r.input IS DISTINCT FROM (e.payload->'input')
    UNION ALL
    SELECT 'run-completed', r.state
    FROM runs r
    JOIN run_events e ON e.run_id = r.id AND e.type = 'RunCompleted'
    WHERE r.id = p_run_id
      AND (r.state IS DISTINCT FROM 'completed' OR r.output IS DISTINCT FROM (e.payload->'output'))
    UNION ALL
    SELECT 'run-failed', r.state
    FROM runs r
    JOIN run_events e ON e.run_id = r.id AND e.type = 'RunFailed'
    WHERE r.id = p_run_id
      AND (r.state IS DISTINCT FROM 'failed' OR r.error IS DISTINCT FROM (e.payload->'error'))
    UNION ALL
    SELECT 'run-state', r.state
    FROM runs r
    WHERE r.id = p_run_id
      AND NOT EXISTS (
        SELECT 1 FROM run_events e
        WHERE e.run_id = r.id AND e.type IN ('RunCompleted', 'RunFailed', 'RunCancelled')
      )
      AND r.state IS DISTINCT FROM (
        CASE
          WHEN (
            SELECT e.type FROM run_events e
            WHERE e.run_id = r.id AND e.type IN ('LeaseGranted', 'LeaseExpired', 'LeaseReleased')
            ORDER BY e.seq DESC
            LIMIT 1
          ) = 'LeaseGranted' THEN 'running'
          ELSE 'pending'
        END
      )
    UNION ALL
    SELECT 'command-missing', e.seq::text
    FROM run_events e
    WHERE e.run_id = p_run_id AND e.type = 'CommandRecorded'
      AND NOT EXISTS (
        SELECT 1 FROM commands c
        WHERE c.run_id = e.run_id
          AND c.command_pos = e.command_pos
          AND c.recorded_event_seq = e.seq
          AND c.kind = e.payload->'descriptor'->>'kind'
          AND c.command_key = e.payload->'descriptor'->>'key'
          AND c.descriptor = e.payload->'descriptor'
      )
    UNION ALL
    SELECT 'command-extra', c.command_pos::text
    FROM commands c
    WHERE c.run_id = p_run_id
      AND NOT EXISTS (
        SELECT 1 FROM run_events e
        WHERE e.run_id = c.run_id AND e.type = 'CommandRecorded' AND e.command_pos = c.command_pos
      )
    UNION ALL
    SELECT 'step-missing', c.command_key
    FROM commands c
    WHERE c.run_id = p_run_id AND c.kind = 'step'
      AND NOT EXISTS (
        SELECT 1 FROM steps s
        WHERE s.run_id = c.run_id
          AND s.step_key = c.command_key
          AND s.command_pos = c.command_pos
          AND s.child_index = 0
          AND s.first_seen_ordinal::text = c.descriptor->>'firstSeenOrdinal'
      )
    UNION ALL
    SELECT 'parallel-child', c.command_key || ':' || child.value
    FROM commands c
    CROSS JOIN LATERAL jsonb_array_elements_text(c.descriptor->'children') WITH ORDINALITY AS child(value, ord)
    WHERE c.run_id = p_run_id AND c.kind = 'parallel'
      AND NOT EXISTS (
        SELECT 1 FROM steps s
        WHERE s.run_id = c.run_id
          AND s.step_key = child.value
          AND s.command_pos = c.command_pos
          AND s.child_index = child.ord - 1
          AND s.first_seen_ordinal::text = c.descriptor->'firstSeenOrdinals'->>((child.ord - 1)::int)
      )
    UNION ALL
    SELECT 'step-extra', s.step_key
    FROM steps s
    WHERE s.run_id = p_run_id
      AND NOT EXISTS (
        SELECT 1 FROM commands c
        WHERE c.run_id = s.run_id
          AND c.command_pos = s.command_pos
          AND (
            (c.kind = 'step' AND c.command_key = s.step_key)
            OR (
              c.kind = 'parallel'
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements_text(c.descriptor->'children') AS child(value)
                WHERE child.value = s.step_key
              )
            )
          )
      )
    UNION ALL
    SELECT 'step-completed', e.step_key
    FROM run_events e
    JOIN steps s ON s.run_id = e.run_id AND s.step_key = e.step_key
    WHERE e.run_id = p_run_id AND e.type = 'StepCompleted'
      AND (s.state IS DISTINCT FROM 'completed' OR s.output IS DISTINCT FROM (e.payload->'result'))
    UNION ALL
    SELECT 'step-failed', e.step_key
    FROM run_events e
    JOIN steps s ON s.run_id = e.run_id AND s.step_key = e.step_key
    WHERE e.run_id = p_run_id AND e.type = 'StepFailed'
      AND (s.state IS DISTINCT FROM 'exhausted' OR s.error IS DISTINCT FROM (e.payload->'error'))
    UNION ALL
    SELECT 'step-completed-extra', s.step_key
    FROM steps s
    WHERE s.run_id = p_run_id AND s.state = 'completed'
      AND NOT EXISTS (
        SELECT 1 FROM run_events e
        WHERE e.run_id = s.run_id AND e.step_key = s.step_key AND e.type = 'StepCompleted'
      )
    UNION ALL
    SELECT 'step-open', s.step_key
    FROM steps s
    WHERE s.run_id = p_run_id
      AND EXISTS (
        SELECT 1 FROM run_events e
        WHERE e.run_id = s.run_id AND e.step_key = s.step_key AND e.type = 'StepStarted'
      )
      AND NOT EXISTS (
        SELECT 1 FROM run_events e
        WHERE e.run_id = s.run_id AND e.step_key = s.step_key AND e.type IN ('StepCompleted', 'StepFailed')
      )
      AND NOT EXISTS (
        SELECT 1 FROM run_events e
        WHERE e.run_id = s.run_id AND e.type = 'RunFailed'
          AND e.payload->'error'->>'code' = 'RecoveryBudgetExceeded'
      )
      AND s.state IS DISTINCT FROM 'running'
    UNION ALL
    SELECT 'attempt-missing', e.step_key || ':' || e.attempt::text
    FROM run_events e
    WHERE e.run_id = p_run_id AND e.type = 'StepStarted'
      AND NOT EXISTS (
        SELECT 1 FROM step_attempts a
        WHERE a.run_id = e.run_id
          AND a.step_key = e.step_key
          AND a.attempt = e.attempt
          AND a.attempt_token = (e.payload->>'attemptToken')::uuid
          AND a.fence = e.fence
      )
    UNION ALL
    SELECT 'attempt-extra', a.step_key || ':' || a.attempt::text
    FROM step_attempts a
    WHERE a.run_id = p_run_id
      AND NOT EXISTS (
        SELECT 1 FROM run_events e
        WHERE e.run_id = a.run_id AND e.step_key = a.step_key AND e.attempt = a.attempt AND e.type = 'StepStarted'
      )
    UNION ALL
    SELECT 'attempt-state', a.step_key || ':' || a.attempt::text
    FROM step_attempts a
    WHERE a.run_id = p_run_id
      AND a.state IS DISTINCT FROM (
        CASE
          WHEN EXISTS (
            SELECT 1 FROM run_events e
            WHERE e.run_id = a.run_id AND e.step_key = a.step_key AND e.attempt = a.attempt AND e.type = 'StepCompleted'
          ) THEN 'completed'
          WHEN EXISTS (
            SELECT 1 FROM run_events e
            WHERE e.run_id = a.run_id AND e.step_key = a.step_key AND e.attempt = a.attempt AND e.type = 'StepFailed'
          ) THEN 'failed'
          WHEN EXISTS (
            SELECT 1 FROM run_events e
            WHERE e.run_id = a.run_id AND e.step_key = a.step_key AND e.attempt = a.attempt AND e.type = 'StepAttemptAbandoned'
          ) THEN 'abandoned'
          ELSE 'running'
        END
      )
    UNION ALL
    SELECT 'allocator-event-seq', r.next_event_seq::text
    FROM runs r
    WHERE r.id = p_run_id
      AND r.next_event_seq IS DISTINCT FROM (
        SELECT max(e.seq) + 1 FROM run_events e WHERE e.run_id = r.id
      )
    UNION ALL
    SELECT 'allocator-command-pos', r.next_command_pos::text
    FROM runs r
    WHERE r.id = p_run_id
      AND r.next_command_pos IS DISTINCT FROM (SELECT count(*) FROM commands c WHERE c.run_id = r.id)
    UNION ALL
    SELECT 'allocator-step-ordinal', r.next_step_ordinal::text
    FROM runs r
    WHERE r.id = p_run_id
      AND r.next_step_ordinal IS DISTINCT FROM (SELECT count(*) FROM steps s WHERE s.run_id = r.id)
    UNION ALL
    SELECT 'allocator-fence', r.current_fence::text
    FROM runs r
    WHERE r.id = p_run_id
      AND r.current_fence IS DISTINCT FROM (SELECT max(e.fence) FROM run_events e WHERE e.run_id = r.id)
  )
  SELECT jsonb_build_object(
    'ok', NOT EXISTS (SELECT 1 FROM mismatches),
    'mismatches', COALESCE(
      (SELECT jsonb_agg(jsonb_build_object('check', kind, 'detail', detail) ORDER BY kind, detail) FROM mismatches),
      '[]'::jsonb
    )
  );
$verify$;

REVOKE ALL ON FUNCTION relay_verify_projections(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_verify_projections(uuid) TO relay_app;
