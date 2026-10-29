-- M22 same-priority tenant fairness.
-- Priority still wins. last_granted_at is coordination state, updated after the run lock.

ALTER TABLE tenants ADD COLUMN IF NOT EXISTS last_granted_at timestamptz;

CREATE OR REPLACE FUNCTION relay_grant_lease(
  p_worker_session_id uuid,
  p_operation_id uuid,
  p_request_hash text,
  p_lease_duration interval
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_existing jsonb;
  v_existing_hash text;
  v_run_id uuid;
  v_now timestamptz;
  v_deadline timestamptz;
  v_fence bigint;
  v_seq bigint;
  v_watermark bigint;
  v_name text;
  v_version text;
  v_response jsonb;
  v_queue_state text;
BEGIN
  PERFORM 1 FROM worker_sessions WHERE id = p_worker_session_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'worker session not found';
  END IF;

  SELECT response, request_hash INTO v_existing, v_existing_hash
  FROM worker_session_receipts
  WHERE worker_session_id = p_worker_session_id AND operation_id = p_operation_id;
  IF FOUND THEN
    IF v_existing_hash = p_request_hash THEN
      RETURN v_existing;
    END IF;
    RAISE EXCEPTION 'IdempotencyConflict' USING DETAIL = 'lease operation id was reused';
  END IF;

  SELECT r.id INTO v_run_id
  FROM run_queue q
  JOIN runs r ON r.id = q.run_id
  JOIN tenants t ON t.id = r.tenant_id
  JOIN worker_session_versions m
    ON m.workflow_version_id = r.workflow_version_id
   AND m.worker_session_id = p_worker_session_id
  JOIN worker_sessions s ON s.id = m.worker_session_id
  WHERE q.state = 'ready'
    AND r.state = 'pending'
    AND s.expires_at > clock_timestamp()
  ORDER BY q.priority DESC, t.last_granted_at ASC NULLS FIRST, q.available_at, q.run_id
  FOR UPDATE OF r SKIP LOCKED
  LIMIT 1;

  IF v_run_id IS NULL THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'no compatible ready run';
  END IF;

  PERFORM 1 FROM run_queue WHERE run_id = v_run_id FOR UPDATE;
  PERFORM relay_test_failpoint('lease.after-lock-before-validation');
  SELECT state INTO v_queue_state FROM run_queue WHERE run_id = v_run_id;
  IF v_queue_state IS DISTINCT FROM 'ready' THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'queue changed before grant';
  END IF;
  IF NOT EXISTS (
    SELECT 1
    FROM runs r
    JOIN worker_session_versions m ON m.workflow_version_id = r.workflow_version_id
    JOIN worker_sessions s ON s.id = m.worker_session_id
    WHERE r.id = v_run_id
      AND m.worker_session_id = p_worker_session_id
      AND r.state = 'pending'
      AND s.expires_at > clock_timestamp()
  ) THEN
    RAISE EXCEPTION 'WorkflowVersionMismatch' USING DETAIL = 'worker cannot execute this version';
  END IF;

  -- Locks have returned. Do not use now() here.
  SELECT clock_timestamp() INTO v_now;
  IF NOT EXISTS (
    SELECT 1 FROM worker_sessions
    WHERE id = p_worker_session_id
      AND expires_at > v_now
  ) THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'worker session is expired';
  END IF;
  v_deadline := v_now + p_lease_duration;

  UPDATE tenants
  SET last_granted_at = v_now
  WHERE id = (SELECT tenant_id FROM runs WHERE id = v_run_id);

  UPDATE runs
  SET current_fence = current_fence + 1,
      state = 'running'
  WHERE id = v_run_id
  RETURNING current_fence INTO v_fence;

  UPDATE run_queue
  SET state = 'leased',
      lease_owner = p_worker_session_id,
      lease_expires_at = v_deadline,
      available_at = NULL
  WHERE run_id = v_run_id;

  v_seq := relay_alloc_events(v_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, payload)
  VALUES (
    v_run_id, v_seq, v_fence, 'system', 'LeaseGranted',
    jsonb_build_object(
      'schemaVersion', 1,
      'workerSessionId', p_worker_session_id,
      'databaseNow', v_now
    )
  );

  SELECT next_event_seq - 1 INTO v_watermark FROM runs WHERE id = v_run_id;
  SELECT wv.name, wv.version INTO v_name, v_version
  FROM runs r
  JOIN workflow_versions wv ON wv.id = r.workflow_version_id
  WHERE r.id = v_run_id;

  UPDATE worker_sessions
  SET active_run_id = v_run_id,
      active_fence = v_fence,
      history_watermark = v_watermark
  WHERE id = p_worker_session_id;

  v_response := jsonb_build_object(
    'runId', v_run_id,
    'fence', v_fence::text,
    'leaseExpiresAt', v_deadline,
    'historyWatermark', v_watermark::text,
    'workflowName', v_name,
    'workflowVersion', v_version,
    'databaseNow', v_now
  );
  INSERT INTO worker_session_receipts (worker_session_id, operation_id, request_hash, response)
  VALUES (p_worker_session_id, p_operation_id, p_request_hash, v_response);
  PERFORM relay_test_failpoint('lease.after-update-before-commit');
  RETURN v_response;
END;
$$;
