-- M8 durable stream log. Chunks and control events share one per-run sequence.
-- A duplicate batch returns the stored sequence and does not allocate another.
-- Completion, failure, finish, and restart commit their stream controls with the workflow transition.

ALTER TABLE runs
  ADD COLUMN next_stream_seq bigint NOT NULL DEFAULT 1 CHECK (next_stream_seq >= 1);

ALTER TABLE step_attempts
  ADD COLUMN last_batch_seq bigint NOT NULL DEFAULT -1 CHECK (last_batch_seq >= -1),
  ADD COLUMN final_batch_seq bigint CHECK (final_batch_seq >= -1),
  ADD CONSTRAINT step_attempts_final_matches_last
    CHECK (final_batch_seq IS NULL OR final_batch_seq = last_batch_seq);

CREATE TABLE run_stream_events (
  run_id uuid NOT NULL REFERENCES runs(id),
  seq bigint NOT NULL CHECK (seq >= 1),
  kind text NOT NULL CHECK (kind IN
    ('chunk','superseded','step_completed','step_failed','run_terminal')),
  step_key text,
  attempt integer CHECK (attempt >= 1),
  batch_seq bigint CHECK (batch_seq >= 0),
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  payload_hash text CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (run_id, seq),
  FOREIGN KEY (run_id, step_key, attempt)
    REFERENCES step_attempts(run_id, step_key, attempt),
  CHECK (
    (kind = 'run_terminal' AND step_key IS NULL AND attempt IS NULL)
    OR
    (kind <> 'run_terminal' AND step_key IS NOT NULL AND attempt IS NOT NULL)
  ),
  CHECK ((kind = 'chunk') = (batch_seq IS NOT NULL)),
  CHECK ((kind = 'chunk') = (payload_hash IS NOT NULL))
);

CREATE UNIQUE INDEX stream_batch_identity
  ON run_stream_events(run_id, step_key, attempt, batch_seq)
  WHERE kind = 'chunk';
CREATE UNIQUE INDEX stream_attempt_control
  ON run_stream_events(run_id, step_key, attempt, kind)
  WHERE kind IN ('superseded','step_completed','step_failed');
CREATE UNIQUE INDEX stream_run_terminal ON run_stream_events(run_id)
  WHERE kind = 'run_terminal';

CREATE OR REPLACE FUNCTION relay_alloc_stream(p_run_id uuid, p_count bigint)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_first bigint;
BEGIN
  IF p_count IS NULL OR p_count < 1 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'stream allocation count must be positive';
  END IF;
  UPDATE runs
  SET next_stream_seq = next_stream_seq + p_count
  WHERE id = p_run_id
  RETURNING next_stream_seq - p_count INTO v_first;
  IF v_first IS NULL THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
  END IF;
  RETURN v_first;
END;
$$;

CREATE OR REPLACE FUNCTION relay_append_stream_control(
  p_run_id uuid,
  p_kind text,
  p_step_key text,
  p_attempt integer,
  p_payload jsonb
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_seq bigint;
BEGIN
  v_seq := relay_alloc_stream(p_run_id, 1);
  INSERT INTO run_stream_events (run_id, seq, kind, step_key, attempt, payload)
  VALUES (p_run_id, v_seq, p_kind, p_step_key, p_attempt, p_payload);
  RETURN v_seq;
END;
$$;

CREATE OR REPLACE FUNCTION relay_append_stream_batch(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_step_key text,
  p_attempt integer,
  p_attempt_token uuid,
  p_batch_seq bigint,
  p_text text,
  p_payload_hash text,
  p_request_hash text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_now timestamptz;
  v_receipt jsonb;
  v_existing_seq bigint;
  v_existing_hash text;
  v_existing_text text;
  v_last bigint;
  v_token uuid;
  v_attempt_fence bigint;
  v_attempt_state text;
  v_seq bigint;
  v_response jsonb;
BEGIN
  IF p_text IS NULL OR octet_length(p_text) < 1 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'stream batch text is empty';
  END IF;
  IF p_batch_seq IS NULL OR p_batch_seq < 0 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'batch sequence must be nonnegative';
  END IF;
  IF p_payload_hash IS NULL OR p_payload_hash !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'batch hash is invalid';
  END IF;

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

  SELECT seq, payload_hash, payload->>'text'
    INTO v_existing_seq, v_existing_hash, v_existing_text
  FROM run_stream_events
  WHERE run_id = p_run_id
    AND kind = 'chunk'
    AND step_key = p_step_key
    AND attempt = p_attempt
    AND batch_seq = p_batch_seq;
  IF FOUND THEN
    IF v_existing_hash IS NOT DISTINCT FROM p_payload_hash
       AND v_existing_text IS NOT DISTINCT FROM p_text THEN
      v_response := jsonb_build_object(
        'streamSeq', v_existing_seq::text,
        'batchSeq', p_batch_seq::text
      );
      INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
      VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
      RETURN v_response;
    END IF;
    RAISE EXCEPTION 'StreamBatchConflict' USING DETAIL = 'stream batch payload does not match';
  END IF;

  PERFORM relay_require_lease(p_run_id, p_worker_session_id, p_fence, v_now);
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
  IF p_batch_seq IS DISTINCT FROM v_last + 1 THEN
    RAISE EXCEPTION 'StreamGap' USING DETAIL = 'stream batch is not the next sequence';
  END IF;

  v_seq := relay_alloc_stream(p_run_id, 1);
  INSERT INTO run_stream_events (
    run_id, seq, kind, step_key, attempt, batch_seq, payload, payload_hash
  ) VALUES (
    p_run_id, v_seq, 'chunk', p_step_key, p_attempt, p_batch_seq,
    jsonb_build_object('schemaVersion', 1, 'text', p_text, 'batchSeq', p_batch_seq::text),
    p_payload_hash
  );
  UPDATE step_attempts
  SET last_batch_seq = p_batch_seq
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = p_attempt;

  v_response := jsonb_build_object(
    'streamSeq', v_seq::text,
    'batchSeq', p_batch_seq::text
  );
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
END;
$$;

DROP FUNCTION relay_complete_step(uuid, uuid, uuid, bigint, text, integer, uuid, jsonb, text);

CREATE FUNCTION relay_complete_step(
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
  SET state = 'completed', output = p_result
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

  v_response := jsonb_build_object(
    'attempt', p_attempt,
    'attemptToken', p_attempt_token,
    'result', p_result
  );
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  PERFORM relay_test_failpoint('step.before-completion-commit');
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

  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, step_key, attempt, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'StepFailed', v_command_pos, p_step_key, p_attempt,
    jsonb_build_object('schemaVersion', 1, 'error', p_error)
  );
  UPDATE step_attempts
  SET state = 'failed', ended_at = v_now, final_batch_seq = v_last
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = p_attempt;
  UPDATE steps
  SET state = 'exhausted', error = p_error
  WHERE run_id = p_run_id AND step_key = p_step_key;
  PERFORM relay_append_stream_control(
    p_run_id,
    'step_failed',
    p_step_key,
    p_attempt,
    jsonb_build_object('schemaVersion', 1, 'error', p_error, 'finalBatchSeq', v_last::text)
  );

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

CREATE OR REPLACE FUNCTION relay_finish_run(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_output jsonb,
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
  v_commands bigint;
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

  SELECT next_command_pos INTO v_commands FROM runs WHERE id = p_run_id;
  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'RunCompleted',
    jsonb_build_object(
      'schemaVersion', 1,
      'output', p_output,
      'consumedCommandCount', v_commands::text
    )
  );
  PERFORM relay_test_failpoint('transition.after-event-insert-before-projections');
  UPDATE runs
  SET state = 'completed',
      output = p_output,
      completed_at = v_now
  WHERE id = p_run_id;
  PERFORM relay_test_failpoint('transition.after-projections-before-queue');
  DELETE FROM run_queue WHERE run_id = p_run_id;
  UPDATE worker_sessions
  SET active_run_id = NULL,
      active_fence = NULL,
      history_watermark = NULL
  WHERE id = p_worker_session_id AND active_run_id = p_run_id;

  v_response := jsonb_build_object('state', 'completed', 'output', p_output);
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  PERFORM relay_append_stream_control(
    p_run_id,
    'run_terminal',
    NULL,
    NULL,
    jsonb_build_object('schemaVersion', 1, 'state', 'completed')
  );
  PERFORM relay_test_failpoint('transition.before-commit');
  RETURN v_response;
END;
$$;

CREATE OR REPLACE FUNCTION relay_restart_attempt(
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
  v_old_attempt integer;
  v_old_token uuid;
  v_old_fence bigint;
  v_command_pos bigint;
  v_ordinal bigint;
  v_budget integer;
  v_new_attempt integer;
  v_token uuid;
  v_seq bigint;
  v_response jsonb;
  v_step_state text;
  v_error jsonb;
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

  SELECT state, command_pos, first_seen_ordinal, recovery_budget
    INTO v_step_state, v_command_pos, v_ordinal, v_budget
  FROM steps WHERE run_id = p_run_id AND step_key = p_step_key;
  IF v_step_state IS NULL THEN
    RAISE EXCEPTION 'AttemptMismatch' USING DETAIL = 'step does not exist';
  END IF;
  IF v_step_state = 'completed' THEN
    RAISE EXCEPTION 'AttemptMismatch' USING DETAIL = 'completed step cannot restart';
  END IF;

  SELECT attempt, attempt_token, fence INTO v_old_attempt, v_old_token, v_old_fence
  FROM step_attempts
  WHERE run_id = p_run_id AND step_key = p_step_key AND state = 'running'
  ORDER BY attempt DESC
  LIMIT 1
  FOR UPDATE;
  IF v_old_attempt IS NULL THEN
    RAISE EXCEPTION 'AttemptMismatch' USING DETAIL = 'no running attempt to abandon';
  END IF;

  v_new_attempt := v_old_attempt + 1;
  IF v_new_attempt > v_budget THEN
    v_error := jsonb_build_object(
      'code', 'RecoveryBudgetExceeded',
      'message', 'recovery budget is exhausted'
    );
    v_seq := relay_alloc_events(p_run_id, 2);
    INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, step_key, attempt, payload)
    VALUES (
      p_run_id, v_seq, p_fence, 'worker', 'StepAttemptAbandoned', v_command_pos, p_step_key, v_old_attempt,
      jsonb_build_object(
        'schemaVersion', 1,
        'reason', 'recovery_budget_exceeded',
        'oldFence', v_old_fence::text
      )
    );
    INSERT INTO run_events (run_id, seq, fence, actor, type, payload)
    VALUES (
      p_run_id, v_seq + 1, p_fence, 'worker', 'RunFailed',
      jsonb_build_object('schemaVersion', 1, 'error', v_error)
    );
    UPDATE step_attempts
    SET state = 'abandoned', ended_at = v_now
    WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = v_old_attempt;
    UPDATE steps
    SET state = 'interrupted'
    WHERE run_id = p_run_id AND step_key = p_step_key;
    UPDATE runs
    SET state = 'failed',
        error = v_error,
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
    v_response := jsonb_build_object('code', 'RecoveryBudgetExceeded', 'state', 'failed');
    INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
    VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
    RETURN v_response;
  END IF;

  v_token := gen_random_uuid();
  v_seq := relay_alloc_events(p_run_id, 2);

  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, step_key, attempt, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'StepAttemptAbandoned', v_command_pos, p_step_key, v_old_attempt,
    jsonb_build_object(
      'schemaVersion', 1,
      'reason', 'attempt_restarted',
      'oldFence', v_old_fence::text
    )
  );
  UPDATE step_attempts
  SET state = 'abandoned', ended_at = v_now
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = v_old_attempt;
  PERFORM relay_append_stream_control(
    p_run_id,
    'superseded',
    p_step_key,
    v_old_attempt,
    jsonb_build_object(
      'schemaVersion', 1,
      'stepKey', p_step_key,
      'oldAttempt', v_old_attempt,
      'newAttempt', v_new_attempt,
      'reason', 'attempt_restarted'
    )
  );
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, step_key, attempt, payload)
  VALUES (
    p_run_id, v_seq + 1, p_fence, 'worker', 'StepStarted', v_command_pos, p_step_key, v_new_attempt,
    jsonb_build_object(
      'schemaVersion', 1,
      'attemptToken', v_token,
      'firstSeenOrdinal', v_ordinal::text,
      'reason', 'recovery'
    )
  );
  UPDATE steps
  SET state = 'running', attempt = v_new_attempt
  WHERE run_id = p_run_id AND step_key = p_step_key;
  INSERT INTO step_attempts (
    run_id, step_key, attempt, attempt_token, fence, state, started_at
  ) VALUES (
    p_run_id, p_step_key, v_new_attempt, v_token, p_fence, 'running', v_now
  );

  v_response := jsonb_build_object(
    'commandPos', v_command_pos::text,
    'firstSeenOrdinal', v_ordinal::text,
    'attempt', v_new_attempt,
    'attemptToken', v_token,
    'stepKey', p_step_key
  );
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
END;
$$;

CREATE OR REPLACE FUNCTION relay_verify_stream(p_run_id uuid)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $verify$
  WITH mismatches AS (
    SELECT 'stream-allocator'::text AS kind, r.next_stream_seq::text AS detail
    FROM runs r
    WHERE r.id = p_run_id
      AND r.next_stream_seq IS DISTINCT FROM (
        SELECT COALESCE(max(e.seq), 0) + 1 FROM run_stream_events e WHERE e.run_id = r.id
      )
    UNION ALL
    SELECT 'stream-sequence-gap', COALESCE(max(e.seq), 0)::text
    FROM run_stream_events e
    WHERE e.run_id = p_run_id
    HAVING count(*) IS DISTINCT FROM COALESCE(max(e.seq), 0)
    UNION ALL
    SELECT 'stream-terminal-missing', r.state
    FROM runs r
    WHERE r.id = p_run_id
      AND r.state IN ('completed', 'failed')
      AND NOT EXISTS (
        SELECT 1 FROM run_stream_events e WHERE e.run_id = r.id AND e.kind = 'run_terminal'
      )
    UNION ALL
    SELECT 'stream-terminal-extra', r.state
    FROM runs r
    WHERE r.id = p_run_id
      AND r.state NOT IN ('completed', 'failed', 'cancelled')
      AND EXISTS (
        SELECT 1 FROM run_stream_events e WHERE e.run_id = r.id AND e.kind = 'run_terminal'
      )
    UNION ALL
    SELECT 'stream-batch-gap', a.step_key || ':' || a.attempt::text
    FROM step_attempts a
    WHERE a.run_id = p_run_id
      AND (
        (
          SELECT count(*) FROM run_stream_events e
          WHERE e.run_id = a.run_id AND e.step_key = a.step_key AND e.attempt = a.attempt AND e.kind = 'chunk'
        ) IS DISTINCT FROM (CASE WHEN a.last_batch_seq < 0 THEN 0 ELSE a.last_batch_seq + 1 END)
        OR EXISTS (
          SELECT 1 FROM generate_series(0::bigint, a.last_batch_seq) AS expected(batch_seq)
          WHERE a.last_batch_seq >= 0
            AND NOT EXISTS (
              SELECT 1 FROM run_stream_events e
              WHERE e.run_id = a.run_id AND e.step_key = a.step_key AND e.attempt = a.attempt
                AND e.kind = 'chunk' AND e.batch_seq = expected.batch_seq
            )
        )
      )
    UNION ALL
    SELECT 'stream-final-batch', a.step_key || ':' || a.attempt::text
    FROM step_attempts a
    WHERE a.run_id = p_run_id
      AND a.state IN ('completed', 'failed')
      AND a.final_batch_seq IS DISTINCT FROM a.last_batch_seq
    UNION ALL
    SELECT 'stream-completion-missing', a.step_key || ':' || a.attempt::text
    FROM step_attempts a
    WHERE a.run_id = p_run_id AND a.state = 'completed'
      AND (
        SELECT count(*) FROM run_stream_events e
        WHERE e.run_id = a.run_id AND e.step_key = a.step_key AND e.attempt = a.attempt AND e.kind = 'step_completed'
      ) IS DISTINCT FROM 1
    UNION ALL
    SELECT 'stream-failure-missing', a.step_key || ':' || a.attempt::text
    FROM step_attempts a
    WHERE a.run_id = p_run_id AND a.state = 'failed'
      AND (
        SELECT count(*) FROM run_stream_events e
        WHERE e.run_id = a.run_id AND e.step_key = a.step_key AND e.attempt = a.attempt AND e.kind = 'step_failed'
      ) IS DISTINCT FROM 1
    UNION ALL
    SELECT 'stream-completion-extra', e.step_key || ':' || e.attempt::text
    FROM run_stream_events e
    WHERE e.run_id = p_run_id AND e.kind = 'step_completed'
      AND NOT EXISTS (
        SELECT 1 FROM step_attempts a
        WHERE a.run_id = e.run_id AND a.step_key = e.step_key AND a.attempt = e.attempt AND a.state = 'completed'
      )
    UNION ALL
    SELECT 'stream-supersession-missing', a.step_key || ':' || a.attempt::text
    FROM step_attempts a
    WHERE a.run_id = p_run_id
      AND a.state = 'abandoned'
      AND EXISTS (
        SELECT 1 FROM step_attempts newer
        WHERE newer.run_id = a.run_id AND newer.step_key = a.step_key AND newer.attempt > a.attempt
      )
      AND NOT EXISTS (
        SELECT 1 FROM run_stream_events e
        WHERE e.run_id = a.run_id AND e.step_key = a.step_key AND e.attempt = a.attempt AND e.kind = 'superseded'
      )
    UNION ALL
    SELECT 'stream-superseded-completed', e.step_key || ':' || e.attempt::text
    FROM run_stream_events e
    JOIN step_attempts a
      ON a.run_id = e.run_id AND a.step_key = e.step_key AND a.attempt = e.attempt
    WHERE e.run_id = p_run_id AND e.kind = 'superseded' AND a.state = 'completed'
  )
  SELECT jsonb_build_object(
    'ok', NOT EXISTS (SELECT 1 FROM mismatches),
    'mismatches', COALESCE(
      (SELECT jsonb_agg(jsonb_build_object('check', kind, 'detail', detail) ORDER BY kind, detail) FROM mismatches),
      '[]'::jsonb
    )
  );
$verify$;

ALTER FUNCTION relay_verify_projections(uuid) RENAME TO relay_verify_workflow_projections;

CREATE FUNCTION relay_verify_projections(p_run_id uuid)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT jsonb_build_object(
    'ok', (core->>'ok')::boolean AND (stream->>'ok')::boolean,
    'mismatches', COALESCE(core->'mismatches', '[]'::jsonb) || COALESCE(stream->'mismatches', '[]'::jsonb)
  )
  FROM (
    SELECT relay_verify_workflow_projections(p_run_id) AS core,
           relay_verify_stream(p_run_id) AS stream
  ) checks;
$$;

REVOKE ALL ON FUNCTION relay_alloc_stream(uuid, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_append_stream_control(uuid, text, text, integer, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_append_stream_batch(uuid, uuid, uuid, bigint, text, integer, uuid, bigint, text, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_complete_step(uuid, uuid, uuid, bigint, text, integer, uuid, jsonb, text, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_verify_stream(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_verify_workflow_projections(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_verify_workflow_projections(uuid) FROM relay_app;
REVOKE ALL ON TABLE run_stream_events FROM PUBLIC;

GRANT SELECT ON TABLE run_stream_events TO relay_app;
GRANT EXECUTE ON FUNCTION relay_append_stream_batch(uuid, uuid, uuid, bigint, text, integer, uuid, bigint, text, text, text) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_complete_step(uuid, uuid, uuid, bigint, text, integer, uuid, jsonb, text, bigint) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_verify_projections(uuid) TO relay_app;
