-- M11: explicit step inputs. The fingerprint is stored with the command.
-- A missing input and an explicit null are different. Retry policy stays unchanged.

ALTER TABLE steps
  ADD COLUMN input_present boolean NOT NULL DEFAULT false,
  ADD COLUMN input jsonb,
  ADD COLUMN input_hash text;

ALTER TABLE steps
  ADD CONSTRAINT steps_input_hash_hex
  CHECK (input_hash IS NULL OR input_hash ~ '^[0-9a-f]{64}$');

ALTER TABLE steps
  ADD CONSTRAINT steps_input_fingerprint
  CHECK (
    (input_present AND input IS NOT NULL AND input_hash IS NOT NULL)
    OR (NOT input_present AND input IS NULL AND input_hash IS NULL)
  );

DROP FUNCTION relay_declare_and_start(uuid, uuid, uuid, bigint, text, bigint, text);

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
  p_input_hash text DEFAULT NULL
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
    p_run_id, p_step_key, v_pos, 0, v_ordinal, 'running', 1, '{"maxAttempts":1}'::jsonb,
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

DROP FUNCTION relay_declare_parallel(uuid, uuid, uuid, bigint, text, bigint, jsonb, integer, text);

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
  p_child_inputs jsonb DEFAULT NULL
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
  v_spec jsonb;
  v_present boolean;
  v_hash text;
  v_any_input boolean := false;
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
      '{"maxAttempts":1}'::jsonb,
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

REVOKE ALL ON FUNCTION relay_declare_and_start(uuid, uuid, uuid, bigint, text, bigint, text, boolean, jsonb, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_declare_parallel(uuid, uuid, uuid, bigint, text, bigint, jsonb, integer, text, jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_declare_and_start(uuid, uuid, uuid, bigint, text, bigint, text, boolean, jsonb, text) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_declare_parallel(uuid, uuid, uuid, bigint, text, bigint, jsonb, integer, text, jsonb) TO relay_app;

CREATE FUNCTION relay_verify_inputs(p_run_id uuid)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $verify$
  WITH mismatches AS (
    SELECT 'step-input'::text AS kind, c.command_key AS detail
    FROM commands c
    JOIN steps s
      ON s.run_id = c.run_id AND s.step_key = c.command_key AND s.command_pos = c.command_pos AND s.child_index = 0
    WHERE c.run_id = p_run_id AND c.kind = 'step'
      AND (
        COALESCE((c.descriptor->>'explicitInput')::boolean, false) IS DISTINCT FROM s.input_present
        OR (s.input_present AND (
          s.input_hash IS DISTINCT FROM c.descriptor->>'inputHash'
          OR s.input IS DISTINCT FROM (c.descriptor->'input')
        ))
        OR (NOT s.input_present AND (s.input IS NOT NULL OR s.input_hash IS NOT NULL))
      )
    UNION ALL
    SELECT 'parallel-input', c.command_key || ':' || child.value
    FROM commands c
    CROSS JOIN LATERAL jsonb_array_elements_text(c.descriptor->'children') WITH ORDINALITY AS child(value, ord)
    JOIN steps s
      ON s.run_id = c.run_id
     AND s.command_pos = c.command_pos
     AND s.step_key = child.value
     AND s.child_index = child.ord - 1
    WHERE c.run_id = p_run_id AND c.kind = 'parallel'
      AND (
        (
          c.descriptor->'childInputs' IS NULL
          AND s.input_present
        )
        OR (
          c.descriptor->'childInputs' IS NOT NULL
          AND (
            s.input_present IS DISTINCT FROM COALESCE((c.descriptor->'childInputs'->((child.ord - 1)::int)->>'explicitInput')::boolean, false)
            OR (
              s.input_present AND (
                s.input_hash IS DISTINCT FROM c.descriptor->'childInputs'->((child.ord - 1)::int)->>'inputHash'
                OR s.input IS DISTINCT FROM (c.descriptor->'childInputs'->((child.ord - 1)::int)->'input')
              )
            )
            OR (NOT s.input_present AND (s.input IS NOT NULL OR s.input_hash IS NOT NULL))
          )
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
    'ok', (core->>'ok')::boolean AND (stream->>'ok')::boolean AND (inputs->>'ok')::boolean,
    'mismatches', COALESCE(core->'mismatches', '[]'::jsonb)
      || COALESCE(stream->'mismatches', '[]'::jsonb)
      || COALESCE(inputs->'mismatches', '[]'::jsonb)
  )
  FROM (
    SELECT relay_verify_workflow_projections(p_run_id) AS core,
           relay_verify_stream(p_run_id) AS stream,
           relay_verify_inputs(p_run_id) AS inputs
  ) checks;
$$;

REVOKE ALL ON FUNCTION relay_verify_inputs(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_verify_inputs(uuid) FROM relay_app;
