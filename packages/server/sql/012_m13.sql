-- M13: durable timers. The wakeup is chosen once from the database clock.
-- Firing copies that deadline. It does not add the duration again.

CREATE UNIQUE INDEX IF NOT EXISTS run_one_timer_scheduled
  ON run_events (run_id, command_pos)
  WHERE type = 'TimerScheduled';

CREATE UNIQUE INDEX IF NOT EXISTS run_one_timer_fired
  ON run_events (run_id, command_pos)
  WHERE type = 'TimerFired';

CREATE FUNCTION relay_schedule_timer(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_key text,
  p_duration bigint,
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
  v_existing_duration text;
  v_existing_wake text;
  v_fired boolean;
  v_descriptor jsonb;
  v_seq bigint;
  v_response jsonb;
BEGIN
  IF p_key IS NULL OR octet_length(p_key) < 1 OR octet_length(p_key) > 256 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'timer key is invalid';
  END IF;
  IF p_duration IS NULL OR p_duration < 0 THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'timer duration is invalid';
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

  SELECT c.kind, c.command_key, c.descriptor->>'durationMs', scheduled.payload->>'wakeAt'
    INTO v_existing_kind, v_existing_key, v_existing_duration, v_existing_wake
  FROM commands c
  LEFT JOIN run_events scheduled
    ON scheduled.run_id = c.run_id
   AND scheduled.command_pos = c.command_pos
   AND scheduled.type = 'TimerScheduled'
  WHERE c.run_id = p_run_id AND c.command_pos = p_expected_pos;
  IF FOUND THEN
    IF v_existing_kind IS DISTINCT FROM 'sleep'
      OR v_existing_key IS DISTINCT FROM p_key
      OR v_existing_duration IS DISTINCT FROM p_duration::text
      OR v_existing_wake IS NULL THEN
      RAISE EXCEPTION 'Nondeterminism' USING DETAIL = 'timer command does not match';
    END IF;
    SELECT EXISTS (
      SELECT 1 FROM run_events
      WHERE run_id = p_run_id AND command_pos = p_expected_pos AND type = 'TimerFired'
    ) INTO v_fired;
    v_response := jsonb_build_object(
      'commandPos', p_expected_pos::text,
      'wakeAt', v_existing_wake,
      'durationMs', p_duration,
      'suspended', NOT v_fired
    );
    INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
    VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
    RETURN v_response;
  END IF;

  IF p_expected_pos IS DISTINCT FROM (SELECT next_command_pos FROM runs WHERE id = p_run_id) THEN
    RAISE EXCEPTION 'Nondeterminism' USING DETAIL = 'expected command position does not match';
  END IF;
  IF EXISTS (
    SELECT 1 FROM commands
    WHERE run_id = p_run_id AND kind = 'sleep' AND command_key = p_key
  ) THEN
    RAISE EXCEPTION 'DuplicateStepKey' USING DETAIL = 'timer key already exists';
  END IF;

  BEGIN
    v_wake := date_trunc('milliseconds', v_now) + (p_duration * interval '1 millisecond');
  EXCEPTION
    WHEN datetime_field_overflow OR interval_field_overflow THEN
      RAISE EXCEPTION 'Serialization' USING DETAIL = 'timer duration is too large';
  END;
  v_wake_text := to_char(v_wake AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');
  IF v_wake_text !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'timer deadline is invalid';
  END IF;

  v_descriptor := jsonb_build_object('kind', 'sleep', 'key', p_key, 'durationMs', p_duration);
  v_seq := relay_alloc_events(p_run_id, 3);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
  VALUES (
    p_run_id, v_seq, p_fence, 'worker', 'CommandRecorded', p_expected_pos,
    jsonb_build_object('schemaVersion', 1, 'descriptor', v_descriptor)
  );
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
  VALUES (
    p_run_id, v_seq + 1, p_fence, 'worker', 'TimerScheduled', p_expected_pos,
    jsonb_build_object(
      'schemaVersion', 1,
      'key', p_key,
      'durationMs', p_duration,
      'wakeAt', v_wake_text
    )
  );
  INSERT INTO commands (run_id, command_pos, kind, command_key, descriptor, outcome, recorded_event_seq)
  VALUES (
    p_run_id, p_expected_pos, 'sleep', p_key, v_descriptor,
    CASE WHEN p_duration = 0 THEN to_jsonb(v_wake_text) ELSE NULL END,
    v_seq
  );
  UPDATE runs SET next_command_pos = next_command_pos + 1 WHERE id = p_run_id;

  IF p_duration = 0 THEN
    INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
    VALUES (
      p_run_id, v_seq + 2, p_fence, 'system', 'TimerFired', p_expected_pos,
      jsonb_build_object('schemaVersion', 1, 'wakeAt', v_wake_text)
    );
    v_response := jsonb_build_object(
      'commandPos', p_expected_pos::text,
      'wakeAt', v_wake_text,
      'durationMs', p_duration,
      'suspended', false
    );
  ELSE
    INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
    VALUES (
      p_run_id, v_seq + 2, p_fence, 'worker', 'RunSuspended', p_expected_pos,
      jsonb_build_object('schemaVersion', 1, 'reason', 'timer', 'wakeAt', v_wake_text, 'key', p_key)
    );
    UPDATE runs SET state = 'sleeping' WHERE id = p_run_id;
    UPDATE run_queue
    SET state = 'scheduled',
        lease_owner = NULL,
        lease_expires_at = NULL,
        available_at = v_wake
    WHERE run_id = p_run_id;
    UPDATE worker_sessions
    SET active_run_id = NULL,
        active_fence = NULL,
        history_watermark = NULL
    WHERE active_run_id = p_run_id;
    v_response := jsonb_build_object(
      'commandPos', p_expected_pos::text,
      'wakeAt', v_wake_text,
      'durationMs', p_duration,
      'suspended', true
    );
  END IF;

  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  PERFORM relay_test_failpoint('command.after-declaration-before-commit');
  RETURN v_response;
END;
$$;

CREATE FUNCTION relay_fire_one_timer()
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_run uuid;
  v_now timestamptz;
  v_pos bigint;
  v_fence bigint;
  v_wake text;
  v_seq bigint;
  v_queue text;
  v_state text;
  v_due timestamptz;
BEGIN
  SELECT r.id INTO v_run
  FROM run_queue q
  JOIN runs r ON r.id = q.run_id
  WHERE q.state = 'scheduled'
    AND r.state = 'sleeping'
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
  SELECT state, current_fence INTO v_state, v_fence FROM runs WHERE id = v_run;
  IF v_state IS DISTINCT FROM 'sleeping'
    OR v_queue IS DISTINCT FROM 'scheduled'
    OR v_due IS NULL
    OR v_due > v_now THEN
    RETURN 'skipped';
  END IF;

  SELECT c.command_pos, scheduled.payload->>'wakeAt'
    INTO v_pos, v_wake
  FROM commands c
  JOIN run_events scheduled
    ON scheduled.run_id = c.run_id
   AND scheduled.command_pos = c.command_pos
   AND scheduled.type = 'TimerScheduled'
  WHERE c.run_id = v_run
    AND c.kind = 'sleep'
    AND c.outcome IS NULL
  ORDER BY c.command_pos
  LIMIT 1;
  IF v_pos IS NULL OR v_wake IS NULL THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'sleeping run has no open timer';
  END IF;
  IF EXISTS (
    SELECT 1 FROM run_events
    WHERE run_id = v_run AND command_pos = v_pos AND type = 'TimerFired'
  ) THEN
    RETURN 'skipped';
  END IF;

  v_seq := relay_alloc_events(v_run, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, command_pos, payload)
  VALUES (
    v_run, v_seq, v_fence, 'system', 'TimerFired', v_pos,
    jsonb_build_object('schemaVersion', 1, 'wakeAt', v_wake)
  );
  UPDATE commands SET outcome = to_jsonb(v_wake) WHERE run_id = v_run AND command_pos = v_pos;
  UPDATE runs SET state = 'pending' WHERE id = v_run;
  UPDATE run_queue
  SET state = 'ready',
      lease_owner = NULL,
      lease_expires_at = NULL,
      available_at = v_now
  WHERE run_id = v_run;
  PERFORM relay_test_failpoint('timer.after-fire-before-commit');
  RETURN 'fired';
END;
$$;

REVOKE ALL ON FUNCTION relay_schedule_timer(uuid, uuid, uuid, bigint, text, bigint, bigint, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_schedule_timer(uuid, uuid, uuid, bigint, text, bigint, bigint, text) TO relay_app;
REVOKE ALL ON FUNCTION relay_fire_one_timer() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_fire_one_timer() TO relay_app;

DO $$
DECLARE
  src text;
  needle text;
  insert_at text;
BEGIN
  SELECT pg_get_functiondef('relay_verify_workflow_projections(uuid)'::regprocedure) INTO src;
  src := replace(src, chr(13), '');
  needle := 'e.type IN (''LeaseGranted'', ''LeaseExpired'', ''LeaseReleased'')';
  IF position(needle IN src) = 0 THEN
    RAISE EXCEPTION 'verify function did not contain the lease-state case';
  END IF;
  src := replace(
    src,
    needle,
    'e.type IN (''LeaseGranted'', ''LeaseExpired'', ''LeaseReleased'', ''RunSuspended'')'
  );
  insert_at := 'WHEN (' || chr(10) || '            SELECT e.type FROM run_events e';
  IF position(insert_at IN src) = 0 THEN
    RAISE EXCEPTION 'verify function did not contain the lease-state select';
  END IF;
  src := replace(
    src,
    insert_at,
    'WHEN EXISTS (' || chr(10)
      || '            SELECT 1 FROM run_events scheduled' || chr(10)
      || '            WHERE scheduled.run_id = r.id' || chr(10)
      || '              AND scheduled.type = ''TimerScheduled''' || chr(10)
      || '              AND NOT EXISTS (' || chr(10)
      || '                SELECT 1 FROM run_events fired' || chr(10)
      || '                WHERE fired.run_id = scheduled.run_id' || chr(10)
      || '                  AND fired.type = ''TimerFired''' || chr(10)
      || '                  AND fired.command_pos = scheduled.command_pos' || chr(10)
      || '              )' || chr(10)
      || '          ) THEN ''sleeping''' || chr(10)
      || '          WHEN (' || chr(10)
      || '            SELECT e.type FROM run_events e'
  );
  IF position('THEN ''sleeping''' IN src) = 0 THEN
    RAISE EXCEPTION 'verify function did not gain the sleeping state';
  END IF;
  EXECUTE src;
END $$;

CREATE OR REPLACE FUNCTION relay_verify_timers(p_run_id uuid)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $verify$
  WITH mismatches AS (
    SELECT 'timer-scheduled'::text AS kind, c.command_pos::text AS detail
    FROM commands c
    WHERE c.run_id = p_run_id AND c.kind = 'sleep'
      AND (
        NOT EXISTS (
          SELECT 1 FROM run_events e
          WHERE e.run_id = c.run_id AND e.command_pos = c.command_pos AND e.type = 'TimerScheduled'
            AND e.payload->>'wakeAt' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
            AND e.payload->>'durationMs' = c.descriptor->>'durationMs'
        )
        OR (
          SELECT count(*) FROM run_events e
          WHERE e.run_id = c.run_id AND e.command_pos = c.command_pos AND e.type = 'TimerScheduled'
        ) IS DISTINCT FROM 1
      )
    UNION ALL
    SELECT 'timer-deadline', c.command_pos::text
    FROM commands c
    JOIN run_events scheduled
      ON scheduled.run_id = c.run_id
     AND scheduled.command_pos = c.command_pos
     AND scheduled.type = 'TimerScheduled'
    LEFT JOIN run_events fired
      ON fired.run_id = c.run_id
     AND fired.command_pos = c.command_pos
     AND fired.type = 'TimerFired'
    WHERE c.run_id = p_run_id AND c.kind = 'sleep'
      AND (
        (fired.seq IS NULL AND c.outcome IS NOT NULL)
        OR (fired.seq IS NOT NULL AND fired.payload->>'wakeAt' IS DISTINCT FROM scheduled.payload->>'wakeAt')
        OR (fired.seq IS NOT NULL AND c.outcome IS DISTINCT FROM to_jsonb(scheduled.payload->>'wakeAt'))
      )
    UNION ALL
    SELECT 'timer-queue', r.state
    FROM runs r
    JOIN commands c ON c.run_id = r.id AND c.kind = 'sleep' AND c.outcome IS NULL
    JOIN run_queue q ON q.run_id = r.id
    WHERE r.id = p_run_id
      AND (
        r.state IS DISTINCT FROM 'sleeping'
        OR q.state IS DISTINCT FROM 'scheduled'
        OR q.lease_owner IS NOT NULL
        OR q.available_at IS DISTINCT FROM (
          SELECT (e.payload->>'wakeAt')::timestamptz
          FROM run_events e
          WHERE e.run_id = r.id AND e.command_pos = c.command_pos AND e.type = 'TimerScheduled'
        )
      )
    UNION ALL
    SELECT 'timer-step', c.command_key
    FROM commands c
    JOIN steps s ON s.run_id = c.run_id AND s.command_pos = c.command_pos
    WHERE c.run_id = p_run_id AND c.kind = 'sleep'
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
      AND (timers->>'ok')::boolean,
    'mismatches', COALESCE(core->'mismatches', '[]'::jsonb)
      || COALESCE(stream->'mismatches', '[]'::jsonb)
      || COALESCE(inputs->'mismatches', '[]'::jsonb)
      || COALESCE(helpers->'mismatches', '[]'::jsonb)
      || COALESCE(timers->'mismatches', '[]'::jsonb)
  )
  FROM (
    SELECT relay_verify_workflow_projections(p_run_id) AS core,
           relay_verify_stream(p_run_id) AS stream,
           relay_verify_inputs(p_run_id) AS inputs,
           relay_verify_helpers(p_run_id) AS helpers,
           relay_verify_timers(p_run_id) AS timers
  ) checks;
$$;

REVOKE ALL ON FUNCTION relay_verify_timers(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_verify_timers(uuid) FROM relay_app;
