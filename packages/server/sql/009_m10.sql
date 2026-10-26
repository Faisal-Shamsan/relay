-- M10: stream retention. The workflow record and step results stay; the stream log does not.

ALTER TABLE runs
  ADD COLUMN stream_expired_at timestamptz;

ALTER TABLE runs
  ADD CONSTRAINT runs_stream_expired_terminal
  CHECK (stream_expired_at IS NULL OR completed_at IS NOT NULL);

CREATE OR REPLACE FUNCTION relay_expire_streams(p_horizon interval)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_run uuid;
  v_now timestamptz;
BEGIN
  IF p_horizon IS NULL OR p_horizon < interval '0' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'stream retention horizon is invalid';
  END IF;

  SELECT r.id INTO v_run
  FROM runs r
  WHERE r.stream_expired_at IS NULL
    AND r.state IN ('completed', 'failed', 'cancelled')
    AND r.completed_at IS NOT NULL
    AND r.completed_at + p_horizon <= clock_timestamp()
  ORDER BY r.completed_at, r.id
  LIMIT 1
  FOR UPDATE OF r SKIP LOCKED;

  IF v_run IS NULL THEN
    RETURN 'none';
  END IF;

  SELECT clock_timestamp() INTO v_now;

  PERFORM 1
  FROM runs r
  WHERE r.id = v_run
    AND r.stream_expired_at IS NULL
    AND r.state IN ('completed', 'failed', 'cancelled')
    AND r.completed_at IS NOT NULL
    AND r.completed_at + p_horizon <= v_now;

  IF NOT FOUND THEN
    RETURN 'skipped';
  END IF;

  DELETE FROM run_stream_events WHERE run_id = v_run;
  UPDATE runs SET stream_expired_at = v_now WHERE id = v_run;
  PERFORM pg_notify('relay_stream', v_run::text);
  RETURN 'expired';
END;
$$;

REVOKE ALL ON FUNCTION relay_expire_streams(interval) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_expire_streams(interval) TO relay_app;

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

  IF EXISTS (SELECT 1 FROM runs WHERE id = p_run_id AND stream_expired_at IS NOT NULL) THEN
    RAISE EXCEPTION 'StreamExpired' USING DETAIL = 'stream history has expired';
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
      AND r.stream_expired_at IS NULL
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
      AND r.stream_expired_at IS NULL
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
      AND (SELECT stream_expired_at FROM runs WHERE id = p_run_id) IS NULL
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
    WHERE a.run_id = p_run_id
      AND (SELECT stream_expired_at FROM runs WHERE id = p_run_id) IS NULL
      AND a.state = 'completed'
      AND (
        SELECT count(*) FROM run_stream_events e
        WHERE e.run_id = a.run_id AND e.step_key = a.step_key AND e.attempt = a.attempt AND e.kind = 'step_completed'
      ) IS DISTINCT FROM 1
    UNION ALL
    SELECT 'stream-failure-missing', a.step_key || ':' || a.attempt::text
    FROM step_attempts a
    WHERE a.run_id = p_run_id
      AND (SELECT stream_expired_at FROM runs WHERE id = p_run_id) IS NULL
      AND a.state = 'failed'
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
      AND (SELECT stream_expired_at FROM runs WHERE id = p_run_id) IS NULL
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
    UNION ALL
    SELECT 'stream-expired-residual', count(*)::text
    FROM run_stream_events e
    WHERE e.run_id = p_run_id
      AND EXISTS (SELECT 1 FROM runs r WHERE r.id = p_run_id AND r.stream_expired_at IS NOT NULL)
    HAVING count(*) > 0
  )
  SELECT jsonb_build_object(
    'ok', NOT EXISTS (SELECT 1 FROM mismatches),
    'mismatches', COALESCE(
      (SELECT jsonb_agg(jsonb_build_object('check', kind, 'detail', detail) ORDER BY kind, detail) FROM mismatches),
      '[]'::jsonb
    )
  );
$verify$;
