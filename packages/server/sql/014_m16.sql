-- M16: rebuild business projections from run_events.
-- A live lease is refused. Fence, allocators, queue, receipts, and the stream log stay.

CREATE FUNCTION relay_rebuild_projections(p_run_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_now timestamptz;
  v_queue text;
  v_expires timestamptz;
  v_terminal text;
  v_terminal_payload jsonb;
  v_created jsonb;
  v_state text;
  v_check jsonb;
BEGIN
  PERFORM 1 FROM runs WHERE id = p_run_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
  END IF;
  PERFORM 1 FROM run_queue WHERE run_id = p_run_id FOR UPDATE;
  SELECT clock_timestamp() INTO v_now;
  SELECT state, lease_expires_at INTO v_queue, v_expires
  FROM run_queue WHERE run_id = p_run_id;
  IF v_queue = 'leased' AND v_expires > v_now THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run is not quiesced';
  END IF;

  INSERT INTO commands (run_id, command_pos, kind, command_key, descriptor, outcome, recorded_event_seq)
  SELECT
    e.run_id,
    e.command_pos,
    e.payload->'descriptor'->>'kind',
    e.payload->'descriptor'->>'key',
    e.payload->'descriptor',
    CASE
      WHEN e.payload->'descriptor'->>'kind' IN ('now', 'random', 'uuid') THEN e.payload->'outcome'
      WHEN e.payload->'descriptor'->>'kind' = 'sleep' AND fired.seq IS NOT NULL
        THEN to_jsonb(scheduled.payload->>'wakeAt')
      ELSE NULL
    END,
    e.seq
  FROM run_events e
  LEFT JOIN run_events fired
    ON fired.run_id = e.run_id AND fired.command_pos = e.command_pos AND fired.type = 'TimerFired'
  LEFT JOIN run_events scheduled
    ON scheduled.run_id = e.run_id AND scheduled.command_pos = e.command_pos AND scheduled.type = 'TimerScheduled'
  WHERE e.run_id = p_run_id AND e.type = 'CommandRecorded'
  ON CONFLICT (run_id, command_pos) DO UPDATE
  SET kind = EXCLUDED.kind,
      command_key = EXCLUDED.command_key,
      descriptor = EXCLUDED.descriptor,
      outcome = EXCLUDED.outcome,
      recorded_event_seq = EXCLUDED.recorded_event_seq;

  INSERT INTO steps (
    run_id, step_key, command_pos, child_index, first_seen_ordinal, state, attempt, retry_policy,
    retry_at, output, error, input_present, input, input_hash
  )
  SELECT
    c.run_id,
    child.step_key,
    c.command_pos,
    child.child_index,
    child.ordinal,
    CASE
      WHEN done.step_key IS NOT NULL THEN 'completed'
      WHEN failed.step_key IS NOT NULL AND (started.seq IS NULL OR started.seq < failed.seq) THEN
        CASE WHEN failed.payload->>'exhausted' = 'false' THEN 'retry_wait' ELSE 'exhausted' END
      WHEN started.step_key IS NOT NULL THEN 'running'
      ELSE 'declared'
    END,
    CASE
      WHEN done.step_key IS NOT NULL THEN done.attempt
      WHEN failed.step_key IS NOT NULL AND (started.seq IS NULL OR started.seq < failed.seq) THEN failed.attempt
      WHEN started.step_key IS NOT NULL THEN started.attempt
      ELSE 0
    END,
    CASE
      WHEN c.kind = 'step' AND c.descriptor ? 'retry' THEN c.descriptor->'retry'
      WHEN c.kind = 'parallel'
        AND COALESCE(c.descriptor->'childRetries'->child.child_index->>'explicitRetry', 'false') = 'true'
        THEN c.descriptor->'childRetries'->child.child_index->'retry'
      ELSE '{"maxAttempts":1}'::jsonb
    END,
    CASE
      WHEN done.step_key IS NULL
        AND failed.step_key IS NOT NULL
        AND (started.seq IS NULL OR started.seq < failed.seq)
        AND failed.payload->>'exhausted' = 'false'
        THEN (failed.payload->>'retryAt')::timestamptz
      ELSE NULL
    END,
    CASE WHEN done.step_key IS NOT NULL THEN done.payload->'result' ELSE NULL END,
    CASE
      WHEN done.step_key IS NULL
        AND failed.step_key IS NOT NULL
        AND (started.seq IS NULL OR started.seq < failed.seq)
        THEN failed.payload->'error'
      ELSE NULL
    END,
    CASE
      WHEN c.kind = 'step' THEN COALESCE((c.descriptor->>'explicitInput')::boolean, false)
      WHEN c.kind = 'parallel' AND c.descriptor->'childInputs' IS NOT NULL
        THEN COALESCE((c.descriptor->'childInputs'->child.child_index->>'explicitInput')::boolean, false)
      ELSE false
    END,
    CASE
      WHEN c.kind = 'step' AND COALESCE((c.descriptor->>'explicitInput')::boolean, false)
        THEN c.descriptor->'input'
      WHEN c.kind = 'parallel'
        AND COALESCE((c.descriptor->'childInputs'->child.child_index->>'explicitInput')::boolean, false)
        THEN c.descriptor->'childInputs'->child.child_index->'input'
      ELSE NULL
    END,
    CASE
      WHEN c.kind = 'step' AND COALESCE((c.descriptor->>'explicitInput')::boolean, false)
        THEN c.descriptor->>'inputHash'
      WHEN c.kind = 'parallel'
        AND COALESCE((c.descriptor->'childInputs'->child.child_index->>'explicitInput')::boolean, false)
        THEN c.descriptor->'childInputs'->child.child_index->>'inputHash'
      ELSE NULL
    END
  FROM commands c
  CROSS JOIN LATERAL (
    SELECT
      c.command_key AS step_key,
      0 AS child_index,
      (c.descriptor->>'firstSeenOrdinal')::bigint AS ordinal
    WHERE c.kind = 'step'
    UNION ALL
    SELECT
      item.value,
      (item.ord - 1)::integer,
      (c.descriptor->'firstSeenOrdinals'->>((item.ord - 1)::int))::bigint
    FROM jsonb_array_elements_text(c.descriptor->'children') WITH ORDINALITY AS item(value, ord)
    WHERE c.kind = 'parallel'
  ) AS child
  LEFT JOIN LATERAL (
    SELECT e.step_key, e.attempt, e.payload
    FROM run_events e
    WHERE e.run_id = c.run_id AND e.step_key = child.step_key AND e.type = 'StepCompleted'
    ORDER BY e.seq DESC
    LIMIT 1
  ) AS done ON true
  LEFT JOIN LATERAL (
    SELECT e.step_key, e.attempt, e.seq, e.payload
    FROM run_events e
    WHERE e.run_id = c.run_id AND e.step_key = child.step_key AND e.type = 'StepFailed'
    ORDER BY e.seq DESC
    LIMIT 1
  ) AS failed ON true
  LEFT JOIN LATERAL (
    SELECT e.step_key, e.attempt, e.seq
    FROM run_events e
    WHERE e.run_id = c.run_id AND e.step_key = child.step_key AND e.type = 'StepStarted'
    ORDER BY e.seq DESC
    LIMIT 1
  ) AS started ON true
  WHERE c.run_id = p_run_id AND c.kind IN ('step', 'parallel')
  ON CONFLICT (run_id, step_key) DO UPDATE
  SET command_pos = EXCLUDED.command_pos,
      child_index = EXCLUDED.child_index,
      first_seen_ordinal = EXCLUDED.first_seen_ordinal,
      state = EXCLUDED.state,
      attempt = EXCLUDED.attempt,
      retry_policy = EXCLUDED.retry_policy,
      retry_at = EXCLUDED.retry_at,
      output = EXCLUDED.output,
      error = EXCLUDED.error,
      input_present = EXCLUDED.input_present,
      input = EXCLUDED.input,
      input_hash = EXCLUDED.input_hash;

  INSERT INTO step_attempts (
    run_id, step_key, attempt, attempt_token, fence, state, started_at, ended_at, last_batch_seq, final_batch_seq
  )
  SELECT
    e.run_id,
    e.step_key,
    e.attempt,
    (e.payload->>'attemptToken')::uuid,
    e.fence,
    CASE
      WHEN EXISTS (
        SELECT 1 FROM run_events done
        WHERE done.run_id = e.run_id AND done.step_key = e.step_key AND done.attempt = e.attempt AND done.type = 'StepCompleted'
      ) THEN 'completed'
      WHEN EXISTS (
        SELECT 1 FROM run_events failed
        WHERE failed.run_id = e.run_id AND failed.step_key = e.step_key AND failed.attempt = e.attempt AND failed.type = 'StepFailed'
      ) THEN 'failed'
      WHEN EXISTS (
        SELECT 1 FROM run_events abandoned
        WHERE abandoned.run_id = e.run_id AND abandoned.step_key = e.step_key AND abandoned.attempt = e.attempt
          AND abandoned.type = 'StepAttemptAbandoned'
      ) THEN 'abandoned'
      ELSE 'running'
    END,
    v_now,
    CASE
      WHEN EXISTS (
        SELECT 1 FROM run_events open_attempt
        WHERE open_attempt.run_id = e.run_id AND open_attempt.step_key = e.step_key AND open_attempt.attempt = e.attempt
          AND open_attempt.type = 'StepStarted'
      ) AND NOT EXISTS (
        SELECT 1 FROM run_events closed
        WHERE closed.run_id = e.run_id AND closed.step_key = e.step_key AND closed.attempt = e.attempt
          AND closed.type IN ('StepCompleted', 'StepFailed', 'StepAttemptAbandoned')
      ) THEN NULL
      ELSE v_now
    END,
    (
      SELECT count(*)::bigint
      FROM run_stream_events chunk
      WHERE chunk.run_id = e.run_id AND chunk.step_key = e.step_key AND chunk.attempt = e.attempt AND chunk.kind = 'chunk'
    ) - 1,
    CASE
      WHEN EXISTS (
        SELECT 1 FROM run_events terminal
        WHERE terminal.run_id = e.run_id AND terminal.step_key = e.step_key AND terminal.attempt = e.attempt
          AND terminal.type IN ('StepCompleted', 'StepFailed')
      ) THEN (
        SELECT count(*)::bigint
        FROM run_stream_events chunk
        WHERE chunk.run_id = e.run_id AND chunk.step_key = e.step_key AND chunk.attempt = e.attempt AND chunk.kind = 'chunk'
      ) - 1
      ELSE NULL
    END
  FROM run_events e
  WHERE e.run_id = p_run_id AND e.type = 'StepStarted'
  ON CONFLICT (run_id, step_key, attempt) DO UPDATE
  SET attempt_token = EXCLUDED.attempt_token,
      fence = EXCLUDED.fence,
      state = EXCLUDED.state,
      ended_at = CASE
        WHEN EXCLUDED.state = 'running' THEN NULL
        ELSE COALESCE(step_attempts.ended_at, EXCLUDED.ended_at)
      END,
      last_batch_seq = EXCLUDED.last_batch_seq,
      final_batch_seq = EXCLUDED.final_batch_seq;

  DELETE FROM step_attempts a
  WHERE a.run_id = p_run_id
    AND NOT EXISTS (
      SELECT 1 FROM run_events e
      WHERE e.run_id = a.run_id AND e.step_key = a.step_key AND e.attempt = a.attempt AND e.type = 'StepStarted'
    )
    AND NOT EXISTS (
      SELECT 1 FROM run_stream_events stream
      WHERE stream.run_id = a.run_id AND stream.step_key = a.step_key AND stream.attempt = a.attempt
    );

  DELETE FROM steps s
  WHERE s.run_id = p_run_id
    AND NOT EXISTS (
      SELECT 1 FROM commands c
      WHERE c.run_id = s.run_id
        AND c.command_pos = s.command_pos
        AND (
          (c.kind = 'step' AND c.command_key = s.step_key)
          OR (c.kind = 'parallel' AND c.descriptor->'children' ? s.step_key)
        )
    )
    AND NOT EXISTS (
      SELECT 1 FROM step_attempts a WHERE a.run_id = s.run_id AND a.step_key = s.step_key
    );

  DELETE FROM commands c
  WHERE c.run_id = p_run_id
    AND NOT EXISTS (
      SELECT 1 FROM run_events e
      WHERE e.run_id = c.run_id AND e.type = 'CommandRecorded' AND e.command_pos = c.command_pos
    )
    AND NOT EXISTS (
      SELECT 1 FROM steps s WHERE s.run_id = c.run_id AND s.command_pos = c.command_pos
    );

  SELECT e.type, e.payload INTO v_terminal, v_terminal_payload
  FROM run_events e
  WHERE e.run_id = p_run_id AND e.type IN ('RunCompleted', 'RunFailed', 'RunCancelled');
  SELECT e.payload->'input' INTO v_created
  FROM run_events e
  WHERE e.run_id = p_run_id AND e.type = 'RunCreated';

  IF v_terminal = 'RunCompleted' THEN
    UPDATE runs
    SET state = 'completed',
        input = v_created,
        output = v_terminal_payload->'output',
        error = NULL,
        completed_at = COALESCE(completed_at, v_now)
    WHERE id = p_run_id;
  ELSIF v_terminal = 'RunFailed' THEN
    UPDATE runs
    SET state = 'failed',
        input = v_created,
        output = NULL,
        error = v_terminal_payload->'error',
        completed_at = COALESCE(completed_at, v_now)
    WHERE id = p_run_id;
  ELSIF v_terminal = 'RunCancelled' THEN
    UPDATE runs
    SET state = 'cancelled',
        input = v_created,
        output = NULL,
        error = NULL,
        completed_at = COALESCE(completed_at, v_now)
    WHERE id = p_run_id;
  ELSE
    SELECT CASE
      WHEN EXISTS (
        SELECT 1 FROM run_events scheduled
        WHERE scheduled.run_id = r.id
          AND scheduled.type = 'TimerScheduled'
          AND NOT EXISTS (
            SELECT 1 FROM run_events fired
            WHERE fired.run_id = scheduled.run_id
              AND fired.type = 'TimerFired'
              AND fired.command_pos = scheduled.command_pos
          )
      ) THEN 'sleeping'
      WHEN EXISTS (
        SELECT 1 FROM steps s
        JOIN run_queue q ON q.run_id = s.run_id
        WHERE s.run_id = r.id AND s.state = 'retry_wait' AND q.state = 'scheduled'
      ) THEN 'retrying'
      WHEN (
        SELECT e.type FROM run_events e
        WHERE e.run_id = r.id AND e.type IN ('LeaseGranted', 'LeaseExpired', 'LeaseReleased', 'RunSuspended')
        ORDER BY e.seq DESC
        LIMIT 1
      ) = 'LeaseGranted' THEN 'running'
      ELSE 'pending'
    END
    INTO v_state
    FROM runs r
    WHERE r.id = p_run_id;
    UPDATE runs
    SET state = v_state,
        input = v_created,
        output = NULL,
        error = NULL,
        completed_at = NULL
    WHERE id = p_run_id;
  END IF;

  v_check := relay_verify_projections(p_run_id);
  IF COALESCE((v_check->>'ok')::boolean, false) IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'ProjectionDrift' USING DETAIL = v_check::text;
  END IF;
  RETURN jsonb_build_object('ok', true, 'runId', p_run_id::text);
END;
$$;

REVOKE ALL ON FUNCTION relay_rebuild_projections(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_rebuild_projections(uuid) FROM relay_app;
