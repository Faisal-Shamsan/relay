-- M5 bounded parallel steps.
-- One transaction records the group command and every child projection.
-- Each child start and terminal outcome commits on its own.
-- Phase 1 marks a failed child exhausted. There is no retry.

CREATE OR REPLACE FUNCTION relay_declare_parallel(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_group_key text,
  p_expected_pos bigint,
  p_children jsonb,
  p_concurrency integer,
  p_request_hash text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_now timestamptz;
  v_receipt jsonb;
  v_pos bigint;
  v_ordinal bigint;
  v_seq bigint;
  v_element jsonb;
  v_key text;
  v_keys text[] := ARRAY[]::text[];
  v_ordinals jsonb := '[]'::jsonb;
  v_children jsonb := '[]'::jsonb;
  v_descriptor jsonb;
  v_response jsonb;
  v_index integer;
BEGIN
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

  IF p_group_key IS NULL OR octet_length(p_group_key) < 1 OR octet_length(p_group_key) > 256 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'group key length is invalid';
  END IF;
  IF p_concurrency IS NULL OR p_concurrency < 1 OR p_concurrency > 64 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'concurrency must be from 1 to 64';
  END IF;
  IF p_children IS NULL OR jsonb_typeof(p_children) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'children must be an array';
  END IF;
  IF jsonb_array_length(p_children) > 1000 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'parallel group exceeds 1000 children';
  END IF;

  SELECT next_command_pos, next_step_ordinal INTO v_pos, v_ordinal
  FROM runs WHERE id = p_run_id;
  IF v_pos IS DISTINCT FROM p_expected_pos THEN
    RAISE EXCEPTION 'Nondeterminism' USING DETAIL = 'expected command position does not match';
  END IF;
  IF EXISTS (
    SELECT 1 FROM commands
    WHERE run_id = p_run_id AND kind = 'parallel' AND command_key = p_group_key
  ) THEN
    RAISE EXCEPTION 'DuplicateStepKey' USING DETAIL = 'parallel group key already exists';
  END IF;

  FOR v_element IN SELECT value FROM jsonb_array_elements(p_children)
  LOOP
    IF jsonb_typeof(v_element) IS DISTINCT FROM 'string' THEN
      RAISE EXCEPTION 'Serialization' USING DETAIL = 'child step key must be a string';
    END IF;
    v_key := v_element #>> '{}';
    IF v_key IS NULL OR octet_length(v_key) < 1 OR octet_length(v_key) > 256 THEN
      RAISE EXCEPTION 'Serialization' USING DETAIL = 'child step key length is invalid';
    END IF;
    IF v_key = ANY(v_keys) OR EXISTS (
      SELECT 1 FROM steps WHERE run_id = p_run_id AND step_key = v_key
    ) THEN
      RAISE EXCEPTION 'DuplicateStepKey' USING DETAIL = 'step key already exists';
    END IF;
    v_keys := array_append(v_keys, v_key);
  END LOOP;

  FOR v_index IN 1..cardinality(v_keys)
  LOOP
    v_ordinals := v_ordinals || to_jsonb((v_ordinal + v_index - 1)::text);
    v_children := v_children || to_jsonb(v_keys[v_index]);
  END LOOP;

  v_descriptor := jsonb_build_object(
    'kind', 'parallel',
    'key', p_group_key,
    'children', v_children,
    'firstSeenOrdinals', v_ordinals,
    'concurrency', p_concurrency,
    'explicitInput', false
  );
  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'CommandRecorded', v_pos,
    jsonb_build_object('schemaVersion', 1, 'descriptor', v_descriptor)
  );
  INSERT INTO commands (run_id, command_pos, kind, command_key, descriptor, recorded_event_seq)
  VALUES (p_run_id, v_pos, 'parallel', p_group_key, v_descriptor, v_seq);

  FOR v_index IN 1..cardinality(v_keys)
  LOOP
    INSERT INTO steps (
      run_id, step_key, command_pos, child_index, first_seen_ordinal, state, attempt, retry_policy
    ) VALUES (
      p_run_id,
      v_keys[v_index],
      v_pos,
      v_index - 1,
      v_ordinal + v_index - 1,
      'declared',
      0,
      '{"maxAttempts":1}'::jsonb
    );
  END LOOP;

  UPDATE runs
  SET next_command_pos = next_command_pos + 1,
      next_step_ordinal = next_step_ordinal + cardinality(v_keys)
  WHERE id = p_run_id;

  v_response := jsonb_build_object(
    'commandPos', v_pos::text,
    'groupKey', p_group_key,
    'concurrency', p_concurrency,
    'childCount', cardinality(v_keys)
  );
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
END;
$$;

CREATE OR REPLACE FUNCTION relay_start_attempt(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_step_key text,
  p_request_hash text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_now timestamptz;
  v_receipt jsonb;
  v_state text;
  v_command_pos bigint;
  v_ordinal bigint;
  v_attempt integer;
  v_token uuid;
  v_seq bigint;
  v_response jsonb;
BEGIN
  PERFORM 1 FROM runs WHERE id = p_run_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
  END IF;
  PERFORM 1 FROM run_queue WHERE run_id = p_run_id FOR UPDATE;
  PERFORM 1 FROM steps WHERE run_id = p_run_id AND step_key = p_step_key FOR UPDATE;

  SELECT clock_timestamp() INTO v_now;
  v_receipt := relay_lookup_receipt(p_run_id, p_operation_id, p_request_hash);
  IF v_receipt IS NOT NULL THEN
    RETURN v_receipt;
  END IF;
  PERFORM relay_require_lease(p_run_id, p_worker_session_id, p_fence, v_now);

  SELECT state, command_pos, first_seen_ordinal, attempt
    INTO v_state, v_command_pos, v_ordinal, v_attempt
  FROM steps WHERE run_id = p_run_id AND step_key = p_step_key;
  IF v_state IS NULL THEN
    RAISE EXCEPTION 'AttemptMismatch' USING DETAIL = 'step does not exist';
  END IF;
  IF v_state IS DISTINCT FROM 'declared' OR v_attempt IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'AttemptMismatch' USING DETAIL = 'declared step is required';
  END IF;

  v_token := gen_random_uuid();
  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, step_key, attempt, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'StepStarted', v_command_pos, p_step_key, 1,
    jsonb_build_object(
      'schemaVersion', 1,
      'attemptToken', v_token,
      'firstSeenOrdinal', v_ordinal::text,
      'reason', 'initial'
    )
  );
  UPDATE steps
  SET state = 'running', attempt = 1
  WHERE run_id = p_run_id AND step_key = p_step_key;
  INSERT INTO step_attempts (
    run_id, step_key, attempt, attempt_token, fence, state, started_at
  ) VALUES (
    p_run_id, p_step_key, 1, v_token, p_fence, 'running', v_now
  );

  v_response := jsonb_build_object(
    'commandPos', v_command_pos::text,
    'firstSeenOrdinal', v_ordinal::text,
    'attempt', 1,
    'attemptToken', v_token,
    'stepKey', p_step_key
  );
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
END;
$$;

CREATE OR REPLACE FUNCTION relay_fail_step(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_step_key text,
  p_attempt integer,
  p_attempt_token uuid,
  p_error jsonb,
  p_request_hash text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_now timestamptz;
  v_receipt jsonb;
  v_command_pos bigint;
  v_token uuid;
  v_attempt_fence bigint;
  v_attempt_state text;
  v_seq bigint;
  v_response jsonb;
BEGIN
  PERFORM 1 FROM runs WHERE id = p_run_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
  END IF;
  PERFORM 1 FROM run_queue WHERE run_id = p_run_id FOR UPDATE;
  PERFORM 1 FROM steps WHERE run_id = p_run_id AND step_key = p_step_key FOR UPDATE;
  PERFORM 1 FROM step_attempts
    WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = p_attempt
    FOR UPDATE;

  SELECT clock_timestamp() INTO v_now;
  v_receipt := relay_lookup_receipt(p_run_id, p_operation_id, p_request_hash);
  IF v_receipt IS NOT NULL THEN
    RETURN v_receipt;
  END IF;
  PERFORM relay_require_lease(p_run_id, p_worker_session_id, p_fence, v_now);

  IF jsonb_typeof(p_error) IS DISTINCT FROM 'object'
     OR p_error->>'code' IS DISTINCT FROM 'StepFailed'
     OR jsonb_typeof(p_error->'message') IS DISTINCT FROM 'string' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'step failure must be a StepFailed object';
  END IF;

  SELECT command_pos INTO v_command_pos
  FROM steps WHERE run_id = p_run_id AND step_key = p_step_key;
  SELECT attempt_token, fence, state
    INTO v_token, v_attempt_fence, v_attempt_state
  FROM step_attempts
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = p_attempt;

  IF v_token IS NULL
     OR v_attempt_state IS DISTINCT FROM 'running'
     OR v_token IS DISTINCT FROM p_attempt_token
     OR v_attempt_fence IS DISTINCT FROM p_fence THEN
    RAISE EXCEPTION 'AttemptMismatch' USING DETAIL = 'attempt does not match the open authorization';
  END IF;

  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, step_key, attempt, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'StepFailed', v_command_pos, p_step_key, p_attempt,
    jsonb_build_object('schemaVersion', 1, 'error', p_error)
  );
  UPDATE step_attempts
  SET state = 'failed', ended_at = v_now
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = p_attempt;
  UPDATE steps
  SET state = 'exhausted', error = p_error
  WHERE run_id = p_run_id AND step_key = p_step_key;

  v_response := jsonb_build_object(
    'attempt', p_attempt,
    'attemptToken', p_attempt_token,
    'error', p_error
  );
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
END;
$$;

REVOKE ALL ON FUNCTION relay_declare_parallel(uuid, uuid, uuid, bigint, text, bigint, jsonb, integer, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_start_attempt(uuid, uuid, uuid, bigint, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_fail_step(uuid, uuid, uuid, bigint, text, integer, uuid, jsonb, text) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION relay_declare_parallel(uuid, uuid, uuid, bigint, text, bigint, jsonb, integer, text) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_start_attempt(uuid, uuid, uuid, bigint, text, text) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_fail_step(uuid, uuid, uuid, bigint, text, integer, uuid, jsonb, text) TO relay_app;
