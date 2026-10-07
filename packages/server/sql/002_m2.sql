-- M2 lease lifetime. Heartbeat is an operational update, not a workflow event.
-- Expiry remains lease_expires_at > captured clock_timestamp(); equality is expired.
-- Grant still does not reclaim an expired lease. Only expire and this reaper do.

CREATE OR REPLACE FUNCTION relay_heartbeat(
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_operation_id uuid,
  p_request_hash text,
  p_lease_duration interval
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_now timestamptz;
  v_deadline timestamptz;
  v_receipt jsonb;
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

  v_deadline := v_now + p_lease_duration;
  UPDATE run_queue
  SET lease_expires_at = v_deadline
  WHERE run_id = p_run_id;

  v_response := jsonb_build_object(
    'fence', p_fence::text,
    'leaseExpiresAt', v_deadline,
    'databaseNow', v_now
  );
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
END;
$$;

CREATE OR REPLACE FUNCTION relay_release_lease(
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_operation_id uuid,
  p_request_hash text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_now timestamptz;
  v_fence bigint;
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

  SELECT current_fence INTO v_fence FROM runs WHERE id = p_run_id;
  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, payload)
  VALUES (
    p_run_id, v_seq, v_fence, 'worker', 'LeaseReleased',
    jsonb_build_object(
      'schemaVersion', 1,
      'workerSessionId', p_worker_session_id,
      'databaseNow', v_now,
      'reason', 'release'
    )
  );

  UPDATE runs SET state = 'pending' WHERE id = p_run_id;
  UPDATE run_queue
  SET state = 'ready',
      lease_owner = NULL,
      lease_expires_at = NULL,
      available_at = v_now
  WHERE run_id = p_run_id;
  UPDATE worker_sessions
  SET active_run_id = NULL,
      active_fence = NULL,
      history_watermark = NULL
  WHERE active_run_id = p_run_id;

  v_response := jsonb_build_object(
    'released', true,
    'fence', v_fence::text,
    'databaseNow', v_now
  );
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
END;
$$;

-- One candidate per call. The server commits this transaction, then calls again,
-- so a later failure cannot roll an earlier expiry back.
CREATE OR REPLACE FUNCTION relay_reap_one_expired()
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_run_id uuid;
  v_now timestamptz;
  v_fence bigint;
  v_state text;
  v_expires timestamptz;
  v_seq bigint;
BEGIN
  SELECT r.id INTO v_run_id
  FROM run_queue q
  JOIN runs r ON r.id = q.run_id
  WHERE q.state = 'leased'
    AND q.lease_expires_at <= clock_timestamp()
  ORDER BY q.lease_expires_at, q.run_id
  FOR UPDATE OF r SKIP LOCKED
  LIMIT 1;

  IF v_run_id IS NULL THEN
    RETURN 'none';
  END IF;

  PERFORM 1 FROM run_queue WHERE run_id = v_run_id FOR UPDATE;
  SELECT clock_timestamp() INTO v_now;
  SELECT current_fence INTO v_fence FROM runs WHERE id = v_run_id;
  SELECT state, lease_expires_at INTO v_state, v_expires
  FROM run_queue WHERE run_id = v_run_id;

  IF v_state IS DISTINCT FROM 'leased' OR v_expires IS NULL OR v_expires > v_now THEN
    RETURN 'skipped';
  END IF;

  v_seq := relay_alloc_events(v_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, payload)
  VALUES (
    v_run_id, v_seq, v_fence, 'system', 'LeaseExpired',
    jsonb_build_object('schemaVersion', 1, 'reason', 'expired', 'databaseNow', v_now)
  );

  UPDATE runs SET state = 'pending' WHERE id = v_run_id;
  UPDATE run_queue
  SET state = 'ready',
      lease_owner = NULL,
      lease_expires_at = NULL,
      available_at = v_now
  WHERE run_id = v_run_id;
  UPDATE worker_sessions
  SET active_run_id = NULL,
      active_fence = NULL,
      history_watermark = NULL
  WHERE active_run_id = v_run_id;

  RETURN 'expired';
END;
$$;

CREATE OR REPLACE FUNCTION relay_reap_expired_leases(p_limit integer)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
BEGIN
  IF p_limit IS NULL OR p_limit < 1 THEN
    RETURN 'none';
  END IF;
  RETURN relay_reap_one_expired();
END;
$$;

REVOKE ALL ON FUNCTION relay_heartbeat(uuid, uuid, bigint, uuid, text, interval) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_release_lease(uuid, uuid, bigint, uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_reap_one_expired() FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_reap_expired_leases(integer) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION relay_heartbeat(uuid, uuid, bigint, uuid, text, interval) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_release_lease(uuid, uuid, bigint, uuid, text) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_reap_expired_leases(integer) TO relay_app;
