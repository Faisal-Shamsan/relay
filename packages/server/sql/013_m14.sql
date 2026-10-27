-- M14: explicit retry policy. The delay is chosen once, under the run lock.
-- A missing policy keeps the Phase 1 recovery ceiling and does not retry a callback failure.

CREATE FUNCTION relay_canonical_retry(p_retry jsonb)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_max integer;
  v_initial bigint;
  v_cap bigint;
  v_multiplier numeric;
  v_codes jsonb;
  v_count integer;
  v_distinct integer;
BEGIN
  IF p_retry IS NULL OR jsonb_typeof(p_retry) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'retry policy is invalid';
  END IF;
  IF EXISTS (
    SELECT 1 FROM jsonb_object_keys(p_retry) AS key
    WHERE key NOT IN ('maxAttempts', 'initialDelayMs', 'maxDelayMs', 'multiplier', 'jitter', 'retryableCodes')
  ) OR (
    SELECT count(*) FROM jsonb_object_keys(p_retry)
  ) IS DISTINCT FROM 6 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'retry policy is invalid';
  END IF;
  IF jsonb_typeof(p_retry->'maxAttempts') IS DISTINCT FROM 'number'
     OR p_retry->>'maxAttempts' !~ '^[0-9]{1,5}$' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'maxAttempts must be from 1 to 10000';
  END IF;
  v_max := (p_retry->>'maxAttempts')::integer;
  IF v_max < 1 OR v_max > 10000 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'maxAttempts must be from 1 to 10000';
  END IF;
  IF jsonb_typeof(p_retry->'initialDelayMs') IS DISTINCT FROM 'number'
     OR p_retry->>'initialDelayMs' !~ '^[0-9]{1,16}$'
     OR jsonb_typeof(p_retry->'maxDelayMs') IS DISTINCT FROM 'number'
     OR p_retry->>'maxDelayMs' !~ '^[0-9]{1,16}$' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'retry delay must be a nonnegative safe integer';
  END IF;
  v_initial := (p_retry->>'initialDelayMs')::bigint;
  v_cap := (p_retry->>'maxDelayMs')::bigint;
  IF v_initial < 0 OR v_cap < 0 OR v_initial > 9007199254740991 OR v_cap > 9007199254740991 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'retry delay must be a nonnegative safe integer';
  END IF;
  IF jsonb_typeof(p_retry->'multiplier') IS DISTINCT FROM 'number' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'multiplier must be a finite number >= 1';
  END IF;
  v_multiplier := (p_retry->>'multiplier')::numeric;
  IF v_multiplier < 1 OR v_multiplier > 9007199254740991 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'multiplier must be a finite number >= 1';
  END IF;
  IF p_retry->>'jitter' IS DISTINCT FROM 'none' AND p_retry->>'jitter' IS DISTINCT FROM 'full' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'jitter must be none or full';
  END IF;
  IF jsonb_typeof(p_retry->'retryableCodes') IS DISTINCT FROM 'array'
     OR jsonb_array_length(p_retry->'retryableCodes') > 32 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'retryableCodes must be an array of at most 32 strings';
  END IF;
  SELECT count(*), count(DISTINCT value)
    INTO v_count, v_distinct
  FROM jsonb_array_elements_text(p_retry->'retryableCodes') AS item(value);
  IF v_count IS DISTINCT FROM v_distinct
     OR EXISTS (
       SELECT 1 FROM jsonb_array_elements_text(p_retry->'retryableCodes') AS item(value)
       WHERE octet_length(value) < 1 OR octet_length(value) > 64
     ) THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'retryableCodes must be unique non-empty strings';
  END IF;
  SELECT COALESCE(jsonb_agg(to_jsonb(item.value) ORDER BY item.ord), '[]'::jsonb)
    INTO v_codes
  FROM jsonb_array_elements_text(p_retry->'retryableCodes') WITH ORDINALITY AS item(value, ord);
  RETURN jsonb_build_object(
    'maxAttempts', v_max,
    'initialDelayMs', v_initial,
    'maxDelayMs', v_cap,
    'multiplier', v_multiplier,
    'jitter', p_retry->>'jitter',
    'retryableCodes', v_codes
  );
END;
$$;

CREATE FUNCTION relay_choose_retry_delay(
  p_policy jsonb,
  p_attempt integer,
  p_now timestamptz
) RETURNS TABLE (delay_ms bigint, retry_at timestamptz, retry_at_text text)
LANGUAGE plpgsql
AS $$
DECLARE
  v_initial numeric;
  v_max numeric;
  v_mult numeric;
  v_cap numeric;
  v_exp integer;
  v_step integer;
  v_delay bigint;
  v_at timestamptz;
  v_text text;
BEGIN
  v_initial := (p_policy->>'initialDelayMs')::numeric;
  v_max := (p_policy->>'maxDelayMs')::numeric;
  v_mult := (p_policy->>'multiplier')::numeric;
  v_exp := p_attempt - 1;
  v_cap := LEAST(v_initial, v_max);
  IF v_exp > 0 AND v_initial > 0 AND v_mult > 1 THEN
    v_cap := v_initial;
    FOR v_step IN 1..v_exp LOOP
      IF v_cap >= v_max OR (v_cap > 0 AND (v_max / v_mult) < v_cap) THEN
        v_cap := v_max;
        EXIT;
      END IF;
      v_cap := v_cap * v_mult;
      IF v_cap > v_max THEN
        v_cap := v_max;
        EXIT;
      END IF;
    END LOOP;
  END IF;
  v_cap := floor(LEAST(v_cap, v_max));
  IF p_policy->>'jitter' = 'none' OR v_cap = 0 THEN
    v_delay := v_cap::bigint;
  ELSE
    v_delay := floor(random() * (v_cap + 1))::bigint;
  END IF;
  BEGIN
    v_at := date_trunc('milliseconds', p_now) + (v_delay * interval '1 millisecond');
  EXCEPTION
    WHEN datetime_field_overflow OR interval_field_overflow THEN
      RAISE EXCEPTION 'Serialization' USING DETAIL = 'retry delay is too large';
  END;
  v_text := to_char(v_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');
  IF v_text !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'retry deadline is invalid';
  END IF;
  delay_ms := v_delay;
  retry_at := v_at;
  retry_at_text := v_text;
  RETURN NEXT;
END;
$$;

CREATE FUNCTION relay_suspend_for_retry(
  p_run_id uuid,
  p_fence bigint,
  p_command_pos bigint
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_due timestamptz;
  v_due_text text;
  v_seq bigint;
BEGIN
  PERFORM 1 FROM steps
  WHERE run_id = p_run_id AND command_pos = p_command_pos
  FOR UPDATE;

  IF EXISTS (
    SELECT 1 FROM steps
    WHERE run_id = p_run_id
      AND command_pos = p_command_pos
      AND state IN ('declared', 'running')
  ) THEN
    RETURN false;
  END IF;

  SELECT min(retry_at) INTO v_due
  FROM steps
  WHERE run_id = p_run_id
    AND command_pos = p_command_pos
    AND state = 'retry_wait';
  IF v_due IS NULL THEN
    RETURN false;
  END IF;

  v_due_text := to_char(v_due AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');
  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'RunSuspended', p_command_pos,
    jsonb_build_object('schemaVersion', 1, 'reason', 'retry', 'retryAt', v_due_text)
  );
  UPDATE runs SET state = 'retrying' WHERE id = p_run_id;
  UPDATE run_queue
  SET state = 'scheduled',
      lease_owner = NULL,
      lease_expires_at = NULL,
      available_at = v_due
  WHERE run_id = p_run_id;
  UPDATE worker_sessions
  SET active_run_id = NULL,
      active_fence = NULL,
      history_watermark = NULL
  WHERE active_run_id = p_run_id;
  PERFORM relay_test_failpoint('retry.after-suspend-before-commit');
  RETURN true;
END;
$$;

DROP FUNCTION relay_declare_and_start(uuid, uuid, uuid, bigint, text, bigint, text, boolean, jsonb, text);

CREATE FUNCTION relay_declare_and_start(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_step_key text,
  p_expected_pos bigint,
  p_request_hash text,
  p_input_present boolean DEFAULT false,
  p_input jsonb DEFAULT NULL,
  p_input_hash text DEFAULT NULL,
  p_retry jsonb DEFAULT NULL
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
  v_token uuid;
  v_descriptor jsonb;
  v_policy jsonb;
  v_response jsonb;
BEGIN
  IF p_input_present IS NULL THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'explicit input flag is invalid';
  END IF;
  IF p_input_present THEN
    IF p_input IS NULL OR p_input_hash IS NULL OR p_input_hash !~ '^[0-9a-f]{64}$' THEN
      RAISE EXCEPTION 'Serialization' USING DETAIL = 'explicit input fingerprint is invalid';
    END IF;
  ELSIF p_input IS NOT NULL OR p_input_hash IS NOT NULL THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'input requires an explicit input';
  END IF;
  IF p_retry IS NULL THEN
    v_policy := '{"maxAttempts":1}'::jsonb;
  ELSE
    v_policy := relay_canonical_retry(p_retry);
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

  SELECT next_command_pos, next_step_ordinal INTO v_pos, v_ordinal
  FROM runs WHERE id = p_run_id;
  IF v_pos IS DISTINCT FROM p_expected_pos THEN
    RAISE EXCEPTION 'Nondeterminism' USING DETAIL = 'expected command position does not match';
  END IF;
  IF EXISTS (SELECT 1 FROM steps WHERE run_id = p_run_id AND step_key = p_step_key) THEN
    RAISE EXCEPTION 'DuplicateStepKey' USING DETAIL = 'step key already exists';
  END IF;

  v_token := gen_random_uuid();
  v_descriptor := jsonb_build_object(
    'kind', 'step',
    'key', p_step_key,
    'firstSeenOrdinal', v_ordinal::text,
    'explicitInput', p_input_present
  );
  IF p_input_present THEN
    v_descriptor := v_descriptor || jsonb_build_object('inputHash', p_input_hash, 'input', p_input);
  END IF;
  IF p_retry IS NOT NULL THEN
    v_descriptor := v_descriptor || jsonb_build_object('retry', v_policy);
  END IF;
  v_seq := relay_alloc_events(p_run_id, 2);

  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'CommandRecorded', v_pos,
    jsonb_build_object('schemaVersion', 1, 'descriptor', v_descriptor)
  );
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, step_key, attempt, payload)
  VALUES (
    p_run_id, v_seq + 1, p_fence, 'worker', 'StepStarted', v_pos, p_step_key, 1,
    jsonb_build_object(
      'schemaVersion', 1,
      'attemptToken', v_token,
      'firstSeenOrdinal', v_ordinal::text,
      'reason', 'initial'
    )
  );
  INSERT INTO commands (run_id, command_pos, kind, command_key, descriptor, recorded_event_seq)
  VALUES (p_run_id, v_pos, 'step', p_step_key, v_descriptor, v_seq);
  INSERT INTO steps (
    run_id, step_key, command_pos, child_index, first_seen_ordinal, state, attempt, retry_policy,
    input_present, input, input_hash
  ) VALUES (
    p_run_id, p_step_key, v_pos, 0, v_ordinal, 'running', 1, v_policy,
    p_input_present, CASE WHEN p_input_present THEN p_input ELSE NULL END, CASE WHEN p_input_present THEN p_input_hash ELSE NULL END
  );
  INSERT INTO step_attempts (
    run_id, step_key, attempt, attempt_token, fence, state, started_at
  ) VALUES (
    p_run_id, p_step_key, 1, v_token, p_fence, 'running', v_now
  );
  UPDATE runs
  SET next_command_pos = next_command_pos + 1,
      next_step_ordinal = next_step_ordinal + 1
  WHERE id = p_run_id;

  v_response := jsonb_build_object(
    'commandPos', v_pos::text,
    'firstSeenOrdinal', v_ordinal::text,
    'attempt', 1,
    'attemptToken', v_token,
    'stepKey', p_step_key
  );
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  PERFORM relay_test_failpoint('command.after-declaration-before-commit');
  RETURN v_response;
END;
$$;

DROP FUNCTION relay_declare_parallel(uuid, uuid, uuid, bigint, text, bigint, jsonb, integer, text, jsonb);

CREATE FUNCTION relay_declare_parallel(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_group_key text,
  p_expected_pos bigint,
  p_children jsonb,
  p_concurrency integer,
  p_request_hash text,
  p_child_inputs jsonb DEFAULT NULL,
  p_child_retries jsonb DEFAULT NULL
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
  v_inputs jsonb := '[]'::jsonb;
  v_retry_specs jsonb := '[]'::jsonb;
  v_policies jsonb := '[]'::jsonb;
  v_spec jsonb;
  v_retry_spec jsonb;
  v_present boolean;
  v_hash text;
  v_any_input boolean := false;
  v_any_retry boolean := false;
  v_policy jsonb;
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
  IF p_child_inputs IS NOT NULL AND (
    jsonb_typeof(p_child_inputs) IS DISTINCT FROM 'array'
    OR jsonb_array_length(p_child_inputs) IS DISTINCT FROM jsonb_array_length(p_children)
  ) THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'child inputs must align with children';
  END IF;
  IF p_child_retries IS NOT NULL AND (
    jsonb_typeof(p_child_retries) IS DISTINCT FROM 'array'
    OR jsonb_array_length(p_child_retries) IS DISTINCT FROM jsonb_array_length(p_children)
  ) THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'child retries must align with children';
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
    v_spec := CASE WHEN p_child_inputs IS NULL THEN NULL ELSE p_child_inputs->(v_index - 1) END;
    v_present := COALESCE((v_spec->>'explicitInput')::boolean, false);
    IF NOT v_present THEN
      IF v_spec IS NOT NULL AND (v_spec ? 'input' OR v_spec ? 'inputHash') THEN
        RAISE EXCEPTION 'Serialization' USING DETAIL = 'input requires an explicit input';
      END IF;
      v_inputs := v_inputs || jsonb_build_array(jsonb_build_object('explicitInput', false));
    ELSE
      v_hash := v_spec->>'inputHash';
      IF v_spec IS NULL OR NOT (v_spec ? 'input') OR v_hash IS NULL OR v_hash !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'Serialization' USING DETAIL = 'explicit input fingerprint is invalid';
      END IF;
      v_any_input := true;
      v_inputs := v_inputs || jsonb_build_array(jsonb_build_object(
        'explicitInput', true,
        'inputHash', v_hash,
        'input', v_spec->'input'
      ));
    END IF;

    v_retry_spec := CASE WHEN p_child_retries IS NULL THEN NULL ELSE p_child_retries->(v_index - 1) END;
    IF v_retry_spec IS NULL OR NOT COALESCE((v_retry_spec->>'explicitRetry')::boolean, false) THEN
      IF v_retry_spec IS NOT NULL AND (
        jsonb_typeof(v_retry_spec) IS DISTINCT FROM 'object' OR v_retry_spec ? 'retry'
      ) THEN
        RAISE EXCEPTION 'Serialization' USING DETAIL = 'child retry policy is invalid';
      END IF;
      v_policy := '{"maxAttempts":1}'::jsonb;
      v_retry_specs := v_retry_specs || jsonb_build_array(jsonb_build_object('explicitRetry', false));
    ELSE
      IF NOT (v_retry_spec ? 'retry') THEN
        RAISE EXCEPTION 'Serialization' USING DETAIL = 'child retry policy is invalid';
      END IF;
      v_policy := relay_canonical_retry(v_retry_spec->'retry');
      v_any_retry := true;
      v_retry_specs := v_retry_specs || jsonb_build_array(jsonb_build_object(
        'explicitRetry', true,
        'retry', v_policy
      ));
    END IF;
    v_policies := v_policies || jsonb_build_array(v_policy);
  END LOOP;

  v_descriptor := jsonb_build_object(
    'kind', 'parallel',
    'key', p_group_key,
    'children', v_children,
    'firstSeenOrdinals', v_ordinals,
    'concurrency', p_concurrency,
    'explicitInput', false
  );
  IF v_any_input THEN
    v_descriptor := v_descriptor || jsonb_build_object('childInputs', v_inputs);
  END IF;
  IF v_any_retry THEN
    v_descriptor := v_descriptor || jsonb_build_object('childRetries', v_retry_specs);
  END IF;
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
    v_spec := v_inputs->(v_index - 1);
    v_present := COALESCE((v_spec->>'explicitInput')::boolean, false);
    v_policy := v_policies->(v_index - 1);
    INSERT INTO steps (
      run_id, step_key, command_pos, child_index, first_seen_ordinal, state, attempt, retry_policy,
      input_present, input, input_hash
    ) VALUES (
      p_run_id,
      v_keys[v_index],
      v_pos,
      v_index - 1,
      v_ordinal + v_index - 1,
      'declared',
      0,
      v_policy,
      v_present,
      CASE WHEN v_present THEN v_spec->'input' ELSE NULL END,
      CASE WHEN v_present THEN v_spec->>'inputHash' ELSE NULL END
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
  PERFORM relay_test_failpoint('command.after-declaration-before-commit');
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
  v_retry_at timestamptz;
  v_policy jsonb;
  v_token uuid;
  v_seq bigint;
  v_response jsonb;
  v_reason text;
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

  SELECT state, command_pos, first_seen_ordinal, attempt, retry_at, retry_policy
    INTO v_state, v_command_pos, v_ordinal, v_attempt, v_retry_at, v_policy
  FROM steps WHERE run_id = p_run_id AND step_key = p_step_key;
  IF v_state IS NULL THEN
    RAISE EXCEPTION 'AttemptMismatch' USING DETAIL = 'step does not exist';
  END IF;

  IF v_state = 'retry_wait' THEN
    IF v_retry_at IS NULL OR v_retry_at > v_now THEN
      RAISE EXCEPTION 'NotExpired' USING DETAIL = 'retry is not due';
    END IF;
    v_attempt := v_attempt + 1;
    IF v_policy ? 'retryableCodes' AND v_attempt > (v_policy->>'maxAttempts')::integer THEN
      RAISE EXCEPTION 'AttemptMismatch' USING DETAIL = 'retry budget is already exhausted';
    END IF;
    v_reason := 'retry';
    UPDATE steps
    SET state = 'running', attempt = v_attempt, retry_at = NULL, error = NULL
    WHERE run_id = p_run_id AND step_key = p_step_key;
  ELSIF v_state = 'declared' AND v_attempt = 0 THEN
    v_attempt := 1;
    v_reason := 'initial';
    UPDATE steps
    SET state = 'running', attempt = 1
    WHERE run_id = p_run_id AND step_key = p_step_key;
  ELSE
    RAISE EXCEPTION 'AttemptMismatch' USING DETAIL = 'declared step is required';
  END IF;

  v_token := gen_random_uuid();
  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, step_key, attempt, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'StepStarted', v_command_pos, p_step_key, v_attempt,
    jsonb_build_object(
      'schemaVersion', 1,
      'attemptToken', v_token,
      'firstSeenOrdinal', v_ordinal::text,
      'reason', v_reason
    )
  );
  INSERT INTO step_attempts (
    run_id, step_key, attempt, attempt_token, fence, state, started_at
  ) VALUES (
    p_run_id, p_step_key, v_attempt, v_token, p_fence, 'running', v_now
  );

  v_response := jsonb_build_object(
    'commandPos', v_command_pos::text,
    'firstSeenOrdinal', v_ordinal::text,
    'attempt', v_attempt,
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
  v_last bigint;
  v_seq bigint;
  v_policy jsonb;
  v_explicit boolean;
  v_retrying boolean := false;
  v_delay bigint;
  v_retry_at timestamptz;
  v_retry_text text;
  v_suspended boolean := false;
  v_payload jsonb;
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
     OR jsonb_typeof(p_error->'code') IS DISTINCT FROM 'string'
     OR octet_length(p_error->>'code') < 1
     OR octet_length(p_error->>'code') > 64
     OR jsonb_typeof(p_error->'message') IS DISTINCT FROM 'string'
     OR octet_length(p_error->>'message') < 1 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'step failure must carry a code and a message';
  END IF;

  SELECT command_pos, retry_policy INTO v_command_pos, v_policy
  FROM steps WHERE run_id = p_run_id AND step_key = p_step_key;
  SELECT attempt_token, fence, state, last_batch_seq
    INTO v_token, v_attempt_fence, v_attempt_state, v_last
  FROM step_attempts
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = p_attempt;

  IF v_token IS NULL
     OR v_attempt_state IS DISTINCT FROM 'running'
     OR v_token IS DISTINCT FROM p_attempt_token
     OR v_attempt_fence IS DISTINCT FROM p_fence THEN
    RAISE EXCEPTION 'AttemptMismatch' USING DETAIL = 'attempt does not match the open authorization';
  END IF;

  v_explicit := v_policy ? 'retryableCodes';
  IF v_explicit
     AND p_attempt < (v_policy->>'maxAttempts')::integer
     AND v_policy->'retryableCodes' ? (p_error->>'code') THEN
    SELECT chosen.delay_ms, chosen.retry_at, chosen.retry_at_text
      INTO v_delay, v_retry_at, v_retry_text
    FROM relay_choose_retry_delay(v_policy, p_attempt, v_now) AS chosen;
    v_retrying := true;
  END IF;

  v_payload := jsonb_build_object(
    'schemaVersion', 1,
    'error', p_error,
    'exhausted', NOT v_retrying,
    'explicitRetry', v_explicit
  );
  IF v_retrying THEN
    v_payload := v_payload || jsonb_build_object('delayMs', v_delay::text, 'retryAt', v_retry_text);
  END IF;

  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, step_key, attempt, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'StepFailed', v_command_pos, p_step_key, p_attempt, v_payload
  );
  UPDATE step_attempts
  SET state = 'failed', ended_at = v_now, final_batch_seq = v_last
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = p_attempt;
  IF v_retrying THEN
    UPDATE steps
    SET state = 'retry_wait', error = p_error, retry_at = v_retry_at
    WHERE run_id = p_run_id AND step_key = p_step_key;
  ELSE
    UPDATE steps
    SET state = 'exhausted', error = p_error, retry_at = NULL
    WHERE run_id = p_run_id AND step_key = p_step_key;
  END IF;
  PERFORM relay_append_stream_control(
    p_run_id,
    'step_failed',
    p_step_key,
    p_attempt,
    jsonb_build_object('schemaVersion', 1, 'error', p_error, 'finalBatchSeq', v_last::text)
  );
  v_suspended := relay_suspend_for_retry(p_run_id, p_fence, v_command_pos);

  v_response := jsonb_build_object(
    'attempt', p_attempt,
    'attemptToken', p_attempt_token,
    'error', p_error,
    'exhausted', NOT v_retrying,
    'retrying', v_retrying,
    'suspended', v_suspended
  );
  IF v_retrying THEN
    v_response := v_response || jsonb_build_object('delayMs', v_delay::text, 'retryAt', v_retry_text);
  END IF;
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
END;
$$;

CREATE OR REPLACE FUNCTION relay_complete_step(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_step_key text,
  p_attempt integer,
  p_attempt_token uuid,
  p_result jsonb,
  p_request_hash text,
  p_final_batch_seq bigint DEFAULT -1
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
  v_last bigint;
  v_chunk bigint;
  v_seq bigint;
  v_suspended boolean;
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

  SELECT command_pos INTO v_command_pos
  FROM steps WHERE run_id = p_run_id AND step_key = p_step_key;
  SELECT attempt_token, fence, state, last_batch_seq
    INTO v_token, v_attempt_fence, v_attempt_state, v_last
  FROM step_attempts
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = p_attempt;

  IF v_token IS NULL
     OR v_attempt_state IS DISTINCT FROM 'running'
     OR v_token IS DISTINCT FROM p_attempt_token
     OR v_attempt_fence IS DISTINCT FROM p_fence THEN
    RAISE EXCEPTION 'AttemptMismatch' USING DETAIL = 'attempt does not match the open authorization';
  END IF;
  IF p_final_batch_seq IS NULL OR v_last IS DISTINCT FROM p_final_batch_seq THEN
    RAISE EXCEPTION 'StreamManifest' USING DETAIL = 'final batch sequence does not match';
  END IF;

  SELECT seq INTO v_chunk
  FROM run_stream_events
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = p_attempt AND kind = 'chunk'
  ORDER BY batch_seq DESC
  LIMIT 1;

  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, step_key, attempt, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'StepCompleted', v_command_pos, p_step_key, p_attempt,
    jsonb_build_object(
      'schemaVersion', 1,
      'result', p_result,
      'finalBatchSeq', p_final_batch_seq::text,
      'lastChunkStreamSeq', to_jsonb(v_chunk::text)
    )
  );
  UPDATE step_attempts
  SET state = 'completed', ended_at = v_now, final_batch_seq = p_final_batch_seq
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = p_attempt;
  UPDATE steps
  SET state = 'completed', output = p_result, error = NULL, retry_at = NULL
  WHERE run_id = p_run_id AND step_key = p_step_key;
  PERFORM relay_append_stream_control(
    p_run_id,
    'step_completed',
    p_step_key,
    p_attempt,
    jsonb_build_object(
      'schemaVersion', 1,
      'finalBatchSeq', p_final_batch_seq::text,
      'lastChunkStreamSeq', to_jsonb(v_chunk::text)
    )
  );
  v_suspended := relay_suspend_for_retry(p_run_id, p_fence, v_command_pos);

  v_response := jsonb_build_object(
    'attempt', p_attempt,
    'attemptToken', p_attempt_token,
    'result', p_result,
    'suspended', v_suspended
  );
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  PERFORM relay_test_failpoint('step.before-completion-commit');
  RETURN v_response;
END;
$$;

CREATE FUNCTION relay_fire_one_retry()
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_run uuid;
  v_now timestamptz;
  v_state text;
  v_queue text;
  v_due timestamptz;
BEGIN
  SELECT r.id INTO v_run
  FROM run_queue q
  JOIN runs r ON r.id = q.run_id
  WHERE q.state = 'scheduled'
    AND r.state = 'retrying'
    AND q.available_at <= clock_timestamp()
  ORDER BY q.available_at, q.run_id
  FOR UPDATE OF r SKIP LOCKED
  LIMIT 1;

  IF v_run IS NULL THEN
    RETURN 'none';
  END IF;

  PERFORM 1 FROM run_queue WHERE run_id = v_run FOR UPDATE;
  SELECT clock_timestamp() INTO v_now;
  SELECT state, available_at INTO v_queue, v_due FROM run_queue WHERE run_id = v_run;
  SELECT state INTO v_state FROM runs WHERE id = v_run;
  IF v_state IS DISTINCT FROM 'retrying'
     OR v_queue IS DISTINCT FROM 'scheduled'
     OR v_due IS NULL
     OR v_due > v_now THEN
    RETURN 'skipped';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM steps WHERE run_id = v_run AND state = 'retry_wait'
  ) THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'retrying run has no waiting step';
  END IF;

  UPDATE runs SET state = 'pending' WHERE id = v_run;
  UPDATE run_queue
  SET state = 'ready',
      lease_owner = NULL,
      lease_expires_at = NULL,
      available_at = v_now
  WHERE run_id = v_run;
  PERFORM relay_test_failpoint('retry.after-wake-before-commit');
  RETURN 'fired';
END;
$$;

CREATE FUNCTION relay_fail_run(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
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
  v_seq bigint;
  v_response jsonb;
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

  IF jsonb_typeof(p_error) IS DISTINCT FROM 'object'
     OR p_error->>'code' IS DISTINCT FROM 'StepExecutionError'
     OR jsonb_typeof(p_error->'message') IS DISTINCT FROM 'string'
     OR octet_length(p_error->>'message') < 1 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'run failure must be a StepExecutionError';
  END IF;

  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'RunFailed',
    jsonb_build_object('schemaVersion', 1, 'error', p_error)
  );
  UPDATE runs
  SET state = 'failed',
      error = p_error,
      completed_at = v_now
  WHERE id = p_run_id;
  DELETE FROM run_queue WHERE run_id = p_run_id;
  UPDATE worker_sessions
  SET active_run_id = NULL,
      active_fence = NULL,
      history_watermark = NULL
  WHERE id = p_worker_session_id AND active_run_id = p_run_id;
  PERFORM relay_append_stream_control(
    p_run_id,
    'run_terminal',
    NULL,
    NULL,
    jsonb_build_object('schemaVersion', 1, 'state', 'failed')
  );
  v_response := jsonb_build_object('state', 'failed', 'error', p_error);
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
END;
$$;

CREATE FUNCTION relay_failed_runs(p_tenant uuid)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT COALESCE(
    (
      SELECT jsonb_agg(id ORDER BY completed_at, id)
      FROM runs
      WHERE tenant_id = p_tenant AND state = 'failed'
    ),
    '[]'::jsonb
  );
$$;

DO $$
DECLARE
  src text;
BEGIN
  SELECT pg_get_functiondef('relay_restart_attempt(uuid,uuid,uuid,bigint,text,text)'::regprocedure) INTO src;
  src := replace(src, chr(13), '');
  IF position('v_new_attempt := v_old_attempt + 1;' IN src) = 0 THEN
    RAISE EXCEPTION 'restart function did not contain the attempt increment';
  END IF;
  src := replace(
    src,
    'v_new_attempt := v_old_attempt + 1;',
    'v_new_attempt := v_old_attempt + 1;' || chr(10)
      || '  SELECT CASE WHEN retry_policy ? ''retryableCodes'' THEN (retry_policy->>''maxAttempts'')::integer ELSE v_budget END' || chr(10)
      || '    INTO v_budget FROM steps WHERE run_id = p_run_id AND step_key = p_step_key;'
  );
  EXECUTE src;
END $$;

DO $$
DECLARE
  src text;
BEGIN
  SELECT pg_get_functiondef('relay_verify_workflow_projections(uuid)'::regprocedure) INTO src;
  src := replace(src, chr(13), '');
  IF position('THEN ''sleeping''' IN src) = 0 THEN
    RAISE EXCEPTION 'verify function did not contain the sleeping state';
  END IF;
  IF position('AND (s.state IS DISTINCT FROM ''exhausted'' OR s.error IS DISTINCT FROM (e.payload->''error''))' IN src) = 0 THEN
    RAISE EXCEPTION 'verify function did not contain the exhausted step check';
  END IF;
  src := replace(
    src,
    'AND (s.state IS DISTINCT FROM ''exhausted'' OR s.error IS DISTINCT FROM (e.payload->''error''))',
    'AND e.payload->>''exhausted'' IS DISTINCT FROM ''false''' || chr(10)
      || '      AND (s.state IS DISTINCT FROM ''exhausted'' OR s.error IS DISTINCT FROM (e.payload->''error''))'
  );
  src := replace(
    src,
    'THEN ''sleeping''',
    'THEN ''sleeping''' || chr(10)
      || '          WHEN EXISTS (' || chr(10)
      || '            SELECT 1 FROM steps s' || chr(10)
      || '            JOIN run_queue q ON q.run_id = s.run_id' || chr(10)
      || '            WHERE s.run_id = r.id AND s.state = ''retry_wait'' AND q.state = ''scheduled''' || chr(10)
      || '          ) THEN ''retrying'''
  );
  IF position('THEN ''retrying''' IN src) = 0 THEN
    RAISE EXCEPTION 'verify function did not gain the retrying state';
  END IF;
  EXECUTE src;
END $$;

CREATE FUNCTION relay_verify_retries(p_run_id uuid)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $verify$
  WITH mismatches AS (
    SELECT 'retry-deadline'::text AS kind, s.step_key AS detail
    FROM steps s
    JOIN run_events e
      ON e.run_id = s.run_id AND e.step_key = s.step_key AND e.attempt = s.attempt AND e.type = 'StepFailed'
    WHERE s.run_id = p_run_id AND s.state = 'retry_wait'
      AND (
        e.payload->>'exhausted' IS DISTINCT FROM 'false'
        OR e.payload->>'retryAt' IS DISTINCT FROM to_char(s.retry_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
        OR e.payload->>'delayMs' !~ '^[0-9]+$'
      )
    UNION ALL
    SELECT 'retry-queue', r.state
    FROM runs r
    JOIN run_queue q ON q.run_id = r.id
    WHERE r.id = p_run_id AND r.state = 'retrying'
      AND (
        q.state IS DISTINCT FROM 'scheduled'
        OR q.lease_owner IS NOT NULL
        OR q.available_at IS DISTINCT FROM (
          SELECT min(s.retry_at) FROM steps s WHERE s.run_id = r.id AND s.state = 'retry_wait'
        )
        OR (
          SELECT e.payload->>'reason'
          FROM run_events e
          WHERE e.run_id = r.id AND e.type = 'RunSuspended'
          ORDER BY e.seq DESC
          LIMIT 1
        ) IS DISTINCT FROM 'retry'
      )
    UNION ALL
    SELECT 'retry-policy', s.step_key
    FROM steps s
    JOIN commands c ON c.run_id = s.run_id AND c.command_pos = s.command_pos AND c.kind = 'step'
    WHERE s.run_id = p_run_id
      AND (
        (s.retry_policy ? 'retryableCodes' AND c.descriptor->'retry' IS DISTINCT FROM s.retry_policy)
        OR (NOT (s.retry_policy ? 'retryableCodes') AND c.descriptor ? 'retry')
      )
    UNION ALL
    SELECT 'retry-child-policy', s.step_key
    FROM steps s
    JOIN commands c ON c.run_id = s.run_id AND c.command_pos = s.command_pos AND c.kind = 'parallel'
    WHERE s.run_id = p_run_id
      AND (
        (
          s.retry_policy ? 'retryableCodes'
          AND (c.descriptor->'childRetries'->s.child_index->'retry') IS DISTINCT FROM s.retry_policy
        )
        OR (
          NOT (s.retry_policy ? 'retryableCodes')
          AND c.descriptor->'childRetries'->s.child_index->>'explicitRetry' = 'true'
        )
      )
  )
  SELECT jsonb_build_object(
    'ok', NOT EXISTS (SELECT 1 FROM mismatches),
    'mismatches', COALESCE(
      (SELECT jsonb_agg(jsonb_build_object('check', kind, 'detail', detail) ORDER BY kind, detail) FROM mismatches),
      '[]'::jsonb
    )
  );
$verify$;

CREATE OR REPLACE FUNCTION relay_verify_projections(p_run_id uuid)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT jsonb_build_object(
    'ok', (core->>'ok')::boolean
      AND (stream->>'ok')::boolean
      AND (inputs->>'ok')::boolean
      AND (helpers->>'ok')::boolean
      AND (timers->>'ok')::boolean
      AND (retries->>'ok')::boolean,
    'mismatches', COALESCE(core->'mismatches', '[]'::jsonb)
      || COALESCE(stream->'mismatches', '[]'::jsonb)
      || COALESCE(inputs->'mismatches', '[]'::jsonb)
      || COALESCE(helpers->'mismatches', '[]'::jsonb)
      || COALESCE(timers->'mismatches', '[]'::jsonb)
      || COALESCE(retries->'mismatches', '[]'::jsonb)
  )
  FROM (
    SELECT relay_verify_workflow_projections(p_run_id) AS core,
           relay_verify_stream(p_run_id) AS stream,
           relay_verify_inputs(p_run_id) AS inputs,
           relay_verify_helpers(p_run_id) AS helpers,
           relay_verify_timers(p_run_id) AS timers,
           relay_verify_retries(p_run_id) AS retries
  ) checks;
$$;

REVOKE ALL ON FUNCTION relay_canonical_retry(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_choose_retry_delay(jsonb, integer, timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_suspend_for_retry(uuid, bigint, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_declare_and_start(uuid, uuid, uuid, bigint, text, bigint, text, boolean, jsonb, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_declare_parallel(uuid, uuid, uuid, bigint, text, bigint, jsonb, integer, text, jsonb, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_fire_one_retry() FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_fail_run(uuid, uuid, uuid, bigint, jsonb, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_failed_runs(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_verify_retries(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_verify_retries(uuid) FROM relay_app;

GRANT EXECUTE ON FUNCTION relay_declare_and_start(uuid, uuid, uuid, bigint, text, bigint, text, boolean, jsonb, text, jsonb) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_declare_parallel(uuid, uuid, uuid, bigint, text, bigint, jsonb, integer, text, jsonb, jsonb) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_fire_one_retry() TO relay_app;
GRANT EXECUTE ON FUNCTION relay_fail_run(uuid, uuid, uuid, bigint, jsonb, text) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_failed_runs(uuid) TO relay_app;
