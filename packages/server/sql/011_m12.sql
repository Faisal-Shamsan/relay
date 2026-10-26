-- M12: journaled helpers. The server draws now, random, and uuid once.
-- Replay and an identical request return that stored value.

CREATE FUNCTION relay_record_helper(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_kind text,
  p_key text,
  p_expected_pos bigint,
  p_request_hash text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_now timestamptz;
  v_receipt jsonb;
  v_existing_kind text;
  v_existing_key text;
  v_existing_outcome jsonb;
  v_descriptor jsonb;
  v_outcome jsonb;
  v_seq bigint;
  v_response jsonb;
BEGIN
  IF p_kind IS NULL OR p_kind NOT IN ('now', 'random', 'uuid') THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'helper kind is invalid';
  END IF;
  IF p_key IS NULL OR octet_length(p_key) < 1 OR octet_length(p_key) > 256 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'helper key is invalid';
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

  SELECT kind, command_key, outcome
    INTO v_existing_kind, v_existing_key, v_existing_outcome
  FROM commands
  WHERE run_id = p_run_id AND command_pos = p_expected_pos;
  IF FOUND THEN
    IF v_existing_kind IS DISTINCT FROM p_kind
      OR v_existing_key IS DISTINCT FROM p_key
      OR v_existing_outcome IS NULL THEN
      RAISE EXCEPTION 'Nondeterminism' USING DETAIL = 'helper command does not match';
    END IF;
    v_response := jsonb_build_object('commandPos', p_expected_pos::text, 'outcome', v_existing_outcome);
    INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
    VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
    RETURN v_response;
  END IF;

  IF p_expected_pos IS DISTINCT FROM (SELECT next_command_pos FROM runs WHERE id = p_run_id) THEN
    RAISE EXCEPTION 'Nondeterminism' USING DETAIL = 'expected command position does not match';
  END IF;
  IF EXISTS (
    SELECT 1 FROM commands
    WHERE run_id = p_run_id AND kind = p_kind AND command_key = p_key
  ) THEN
    RAISE EXCEPTION 'DuplicateStepKey' USING DETAIL = 'helper key already exists';
  END IF;

  v_descriptor := jsonb_build_object('kind', p_kind, 'key', p_key);
  IF p_kind = 'now' THEN
    v_outcome := to_jsonb(to_char(
      date_trunc('milliseconds', v_now) AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    ));
  ELSIF p_kind = 'random' THEN
    v_outcome := to_jsonb(random());
  ELSE
    v_outcome := to_jsonb(gen_random_uuid()::text);
  END IF;

  IF p_kind = 'now' AND (
    jsonb_typeof(v_outcome) IS DISTINCT FROM 'string'
    OR v_outcome #>> '{}' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
  ) THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'clock value is invalid';
  END IF;
  IF p_kind = 'random' AND (
    jsonb_typeof(v_outcome) IS DISTINCT FROM 'number'
    OR (v_outcome #>> '{}')::float8 < 0
    OR (v_outcome #>> '{}')::float8 >= 1
  ) THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'random value is invalid';
  END IF;
  IF p_kind = 'uuid' AND (
    jsonb_typeof(v_outcome) IS DISTINCT FROM 'string'
    OR v_outcome #>> '{}' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
  ) THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'uuid value is invalid';
  END IF;

  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'CommandRecorded', p_expected_pos,
    jsonb_build_object('schemaVersion', 1, 'descriptor', v_descriptor, 'outcome', v_outcome)
  );
  INSERT INTO commands (run_id, command_pos, kind, command_key, descriptor, outcome, recorded_event_seq)
  VALUES (p_run_id, p_expected_pos, p_kind, p_key, v_descriptor, v_outcome, v_seq);
  UPDATE runs SET next_command_pos = next_command_pos + 1 WHERE id = p_run_id;

  v_response := jsonb_build_object('commandPos', p_expected_pos::text, 'outcome', v_outcome);
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  PERFORM relay_test_failpoint('command.after-declaration-before-commit');
  RETURN v_response;
END;
$$;

REVOKE ALL ON FUNCTION relay_record_helper(uuid, uuid, uuid, bigint, text, text, bigint, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_record_helper(uuid, uuid, uuid, bigint, text, text, bigint, text) TO relay_app;

CREATE OR REPLACE FUNCTION relay_verify_helpers(p_run_id uuid)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $verify$
  WITH mismatches AS (
    SELECT 'helper-outcome' AS kind, c.command_pos::text AS detail
    FROM commands c
    JOIN run_events e ON e.run_id = c.run_id AND e.seq = c.recorded_event_seq
    WHERE c.run_id = p_run_id
      AND c.kind IN ('now', 'random', 'uuid')
      AND (
        c.outcome IS NULL
        OR e.type IS DISTINCT FROM 'CommandRecorded'
        OR e.payload->'outcome' IS DISTINCT FROM c.outcome
        OR e.payload->'descriptor' IS DISTINCT FROM c.descriptor
      )
    UNION ALL
    SELECT 'helper-shape', c.command_pos::text
    FROM commands c
    WHERE c.run_id = p_run_id
      AND (
        (
          c.kind = 'now'
          AND (
            jsonb_typeof(c.outcome) IS DISTINCT FROM 'string'
            OR c.outcome #>> '{}' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
          )
        )
        OR (
          c.kind = 'random'
          AND (
            jsonb_typeof(c.outcome) IS DISTINCT FROM 'number'
            OR (c.outcome #>> '{}')::float8 < 0
            OR (c.outcome #>> '{}')::float8 >= 1
          )
        )
        OR (
          c.kind = 'uuid'
          AND (
            jsonb_typeof(c.outcome) IS DISTINCT FROM 'string'
            OR c.outcome #>> '{}' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
          )
        )
      )
    UNION ALL
    SELECT 'helper-step', c.command_key
    FROM commands c
    JOIN steps s ON s.run_id = c.run_id AND s.command_pos = c.command_pos
    WHERE c.run_id = p_run_id AND c.kind IN ('now', 'random', 'uuid')
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
    'ok', (core->>'ok')::boolean AND (stream->>'ok')::boolean AND (inputs->>'ok')::boolean AND (helpers->>'ok')::boolean,
    'mismatches', COALESCE(core->'mismatches', '[]'::jsonb)
      || COALESCE(stream->'mismatches', '[]'::jsonb)
      || COALESCE(inputs->'mismatches', '[]'::jsonb)
      || COALESCE(helpers->'mismatches', '[]'::jsonb)
  )
  FROM (
    SELECT relay_verify_workflow_projections(p_run_id) AS core,
           relay_verify_stream(p_run_id) AS stream,
           relay_verify_inputs(p_run_id) AS inputs,
           relay_verify_helpers(p_run_id) AS helpers
  ) checks;
$$;

REVOKE ALL ON FUNCTION relay_verify_helpers(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_verify_helpers(uuid) FROM relay_app;
