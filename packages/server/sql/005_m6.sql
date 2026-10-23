-- M6 admission and recovery budget.
-- Trigger identity is (tenant, idempotency key). The recovery ceiling is copied
-- onto the step at insert and is not read again from the server default.

ALTER TABLE steps
  ADD COLUMN IF NOT EXISTS recovery_budget integer NOT NULL DEFAULT 100 CHECK (recovery_budget >= 1);

CREATE OR REPLACE FUNCTION relay_apply_recovery_budget()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_raw text;
BEGIN
  v_raw := current_setting('relay.recovery_budget', true);
  IF v_raw IS NULL OR v_raw = '' THEN
    RETURN NEW;
  END IF;
  IF v_raw !~ '^[1-9][0-9]*$' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'recovery budget must be a positive integer';
  END IF;
  NEW.recovery_budget := v_raw::integer;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS steps_recovery_budget ON steps;
CREATE TRIGGER steps_recovery_budget
  BEFORE INSERT ON steps
  FOR EACH ROW
  EXECUTE FUNCTION relay_apply_recovery_budget();

CREATE OR REPLACE FUNCTION relay_create_run(
  p_tenant_id uuid,
  p_workflow_name text,
  p_workflow_version text,
  p_input jsonb,
  p_trigger_hash text,
  p_run_id uuid,
  p_idempotency_key text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_version_id uuid;
  v_now timestamptz;
  v_seq bigint;
  v_existing_id uuid;
  v_existing_version uuid;
  v_existing_input jsonb;
BEGIN
  SELECT id INTO v_version_id
  FROM workflow_versions
  WHERE tenant_id = p_tenant_id AND name = p_workflow_name AND version = p_workflow_version;
  IF v_version_id IS NULL THEN
    RAISE EXCEPTION 'WorkflowVersionMismatch' USING DETAIL = 'workflow version is not registered';
  END IF;
  IF p_idempotency_key IS NOT NULL
     AND (octet_length(p_idempotency_key) < 1 OR octet_length(p_idempotency_key) > 256) THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'idempotency key length is invalid';
  END IF;

  IF p_idempotency_key IS NOT NULL THEN
    SELECT id, workflow_version_id, input
      INTO v_existing_id, v_existing_version, v_existing_input
    FROM runs
    WHERE tenant_id = p_tenant_id AND idempotency_key = p_idempotency_key
    FOR UPDATE;
    IF FOUND THEN
      IF v_existing_version IS DISTINCT FROM v_version_id
         OR v_existing_input IS DISTINCT FROM p_input THEN
        RAISE EXCEPTION 'IdempotencyConflict' USING DETAIL = 'idempotency key was reused with a different intent';
      END IF;
      RETURN jsonb_build_object('runId', v_existing_id, 'created', false);
    END IF;
  END IF;

  BEGIN
    INSERT INTO runs (
      id, tenant_id, workflow_version_id, state, input, trigger_hash, idempotency_key, current_fence
    ) VALUES (
      p_run_id, p_tenant_id, v_version_id, 'pending', p_input, p_trigger_hash, p_idempotency_key, 0
    );
  EXCEPTION
    WHEN unique_violation THEN
      SELECT id, workflow_version_id, input
        INTO v_existing_id, v_existing_version, v_existing_input
      FROM runs
      WHERE tenant_id = p_tenant_id AND idempotency_key = p_idempotency_key;
      IF NOT FOUND THEN
        RAISE;
      END IF;
      IF v_existing_version IS DISTINCT FROM v_version_id
         OR v_existing_input IS DISTINCT FROM p_input THEN
        RAISE EXCEPTION 'IdempotencyConflict' USING DETAIL = 'idempotency key was reused with a different intent';
      END IF;
      RETURN jsonb_build_object('runId', v_existing_id, 'created', false);
  END;

  SELECT clock_timestamp() INTO v_now;
  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, payload, created_at)
  VALUES (
    p_run_id, v_seq, 0, 'client', 'RunCreated',
    jsonb_build_object(
      'schemaVersion', 1,
      'tenantId', p_tenant_id,
      'workflowName', p_workflow_name,
      'workflowVersion', p_workflow_version,
      'input', p_input,
      'triggerHash', p_trigger_hash,
      'idempotencyKey', p_idempotency_key
    ),
    v_now
  );
  INSERT INTO run_queue (run_id, state, priority, available_at)
  VALUES (p_run_id, 'ready', 0, v_now);
  RETURN jsonb_build_object('runId', p_run_id, 'created', true);
END;
$$;

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
  JOIN worker_session_versions m
    ON m.workflow_version_id = r.workflow_version_id
   AND m.worker_session_id = p_worker_session_id
  JOIN worker_sessions s ON s.id = m.worker_session_id
  WHERE q.state = 'ready'
    AND r.state = 'pending'
    AND s.expires_at > clock_timestamp()
  ORDER BY q.priority DESC, q.available_at, q.run_id
  FOR UPDATE OF r SKIP LOCKED
  LIMIT 1;

  IF v_run_id IS NULL THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'no compatible ready run';
  END IF;

  PERFORM 1 FROM run_queue WHERE run_id = v_run_id FOR UPDATE;
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

  UPDATE step_attempts
  SET state = 'abandoned', ended_at = v_now
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = v_old_attempt;
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

REVOKE ALL ON FUNCTION relay_apply_recovery_budget() FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_create_run(uuid, text, text, jsonb, text, uuid, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION relay_create_run(uuid, text, text, jsonb, text, uuid, text) TO relay_app;
