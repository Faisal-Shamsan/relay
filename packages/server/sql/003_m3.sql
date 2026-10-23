-- M3 intra-transaction failpoints. The helper is not granted to relay_app.
-- An unset relay.failpoint returns. A set value without relay.allow_failpoints = 1 is rejected.

CREATE OR REPLACE FUNCTION relay_test_failpoint(p_point text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_armed text;
  v_allow text;
BEGIN
  v_armed := current_setting('relay.failpoint', true);
  IF v_armed IS NULL OR v_armed = '' THEN
    RETURN;
  END IF;

  v_allow := current_setting('relay.allow_failpoints', true);
  IF v_allow IS DISTINCT FROM '1' THEN
    RAISE EXCEPTION 'failpoint activation is rejected';
  END IF;

  IF v_armed = p_point THEN
    RAISE EXCEPTION 'FailpointAbort' USING DETAIL = p_point;
  END IF;
  RETURN;
END;
$$;

REVOKE ALL ON FUNCTION relay_test_failpoint(text) FROM PUBLIC;

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
  PERFORM relay_test_failpoint('transition.before-commit');
  RETURN v_response;
END;
$$;
