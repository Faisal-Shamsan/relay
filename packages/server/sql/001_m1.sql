-- Phase 1 tables and transitions for one sequential step.
-- Phase 2 stream columns and run_stream_events are omitted.
-- Lease deadlines use clock_timestamp() captured only after row locks return.
-- now() / transaction_timestamp() are not used for those decisions.

DO $$
BEGIN
  CREATE ROLE relay_app LOGIN PASSWORD 'relay_app_dev';
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', current_database());
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO relay_app', current_database());
END $$;

CREATE TABLE IF NOT EXISTS tenants (
  id uuid PRIMARY KEY,
  name text NOT NULL CHECK (length(name) > 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO tenants (id, name)
VALUES ('11111111-1111-4111-8111-111111111111', 'seed')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS workflow_versions (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  name text NOT NULL CHECK (length(name) > 0),
  version text NOT NULL CHECK (length(version) > 0),
  protocol_version integer NOT NULL CHECK (protocol_version > 0),
  registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (tenant_id, name, version),
  UNIQUE (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS runs (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  workflow_version_id uuid NOT NULL,
  state text NOT NULL CHECK (state IN (
    'pending','running','sleeping','retrying','waiting',
    'completed','failed','cancelled')),
  input jsonb NOT NULL,
  output jsonb,
  error jsonb,
  idempotency_key text CHECK (
    idempotency_key IS NULL OR octet_length(idempotency_key) BETWEEN 1 AND 256
  ),
  trigger_hash text NOT NULL CHECK (trigger_hash ~ '^[0-9a-f]{64}$'),
  current_fence bigint NOT NULL DEFAULT 0 CHECK (current_fence >= 0),
  next_event_seq bigint NOT NULL DEFAULT 1 CHECK (next_event_seq >= 1),
  next_command_pos bigint NOT NULL DEFAULT 0 CHECK (next_command_pos >= 0),
  next_step_ordinal bigint NOT NULL DEFAULT 0 CHECK (next_step_ordinal >= 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz,
  FOREIGN KEY (tenant_id, workflow_version_id)
    REFERENCES workflow_versions(tenant_id, id),
  UNIQUE (tenant_id, idempotency_key),
  CHECK ((state IN ('completed','failed','cancelled')) = (completed_at IS NOT NULL)),
  CHECK ((state = 'completed') = (output IS NOT NULL)),
  CHECK ((state = 'failed') = (error IS NOT NULL)),
  CHECK (error IS NULL OR jsonb_typeof(error) = 'object')
);

CREATE TABLE IF NOT EXISTS run_events (
  run_id uuid NOT NULL REFERENCES runs(id),
  seq bigint NOT NULL CHECK (seq >= 1),
  fence bigint NOT NULL CHECK (fence >= 0),
  actor text NOT NULL CHECK (actor IN ('worker','system','client')),
  type text NOT NULL CHECK (type IN (
    'RunCreated','LeaseGranted','LeaseReleased','LeaseExpired',
    'CommandRecorded','StepStarted','StepCompleted','StepFailed',
    'StepAttemptAbandoned','RunSuspended',
    'TimerScheduled','TimerFired','SignalReceived','WaitResolved',
    'RunCompleted','RunFailed','RunCancelled'
  )),
  command_pos bigint CHECK (command_pos >= 0),
  step_key text CHECK (octet_length(step_key) BETWEEN 1 AND 256),
  attempt integer CHECK (attempt >= 1),
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (run_id, seq),
  CHECK ((attempt IS NULL) OR (step_key IS NOT NULL)),
  CHECK (type NOT IN ('CommandRecorded','TimerScheduled','TimerFired','WaitResolved')
    OR command_pos IS NOT NULL),
  CHECK (type NOT IN ('StepStarted','StepCompleted','StepFailed','StepAttemptAbandoned')
    OR (command_pos IS NOT NULL AND step_key IS NOT NULL AND attempt IS NOT NULL)),
  CHECK (type NOT IN ('RunCreated','RunCompleted','RunFailed','RunCancelled')
    OR (step_key IS NULL AND attempt IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS run_one_terminal_event ON run_events(run_id)
  WHERE type IN ('RunCompleted','RunFailed','RunCancelled');
CREATE UNIQUE INDEX IF NOT EXISTS run_one_creation ON run_events(run_id)
  WHERE type = 'RunCreated';
CREATE UNIQUE INDEX IF NOT EXISTS run_one_command_event ON run_events(run_id, command_pos)
  WHERE type = 'CommandRecorded';
CREATE UNIQUE INDEX IF NOT EXISTS step_one_completion ON run_events(run_id, step_key)
  WHERE type = 'StepCompleted';
CREATE UNIQUE INDEX IF NOT EXISTS attempt_one_start ON run_events(run_id, step_key, attempt)
  WHERE type = 'StepStarted';
CREATE UNIQUE INDEX IF NOT EXISTS attempt_one_terminal ON run_events(run_id, step_key, attempt)
  WHERE type IN ('StepCompleted','StepFailed','StepAttemptAbandoned');

CREATE TABLE IF NOT EXISTS commands (
  run_id uuid NOT NULL REFERENCES runs(id),
  command_pos bigint NOT NULL CHECK (command_pos >= 0),
  kind text NOT NULL CHECK (kind IN (
    'step','parallel','now','random','uuid','sleep','wait')),
  command_key text NOT NULL CHECK (octet_length(command_key) BETWEEN 1 AND 256),
  descriptor jsonb NOT NULL CHECK (jsonb_typeof(descriptor) = 'object'),
  outcome jsonb,
  recorded_event_seq bigint NOT NULL,
  PRIMARY KEY (run_id, command_pos),
  UNIQUE (run_id, kind, command_key),
  FOREIGN KEY (run_id, recorded_event_seq) REFERENCES run_events(run_id, seq)
);

CREATE TABLE IF NOT EXISTS steps (
  run_id uuid NOT NULL REFERENCES runs(id),
  step_key text NOT NULL CHECK (octet_length(step_key) BETWEEN 1 AND 256),
  command_pos bigint NOT NULL,
  child_index integer NOT NULL DEFAULT 0 CHECK (child_index >= 0),
  first_seen_ordinal bigint NOT NULL CHECK (first_seen_ordinal >= 0),
  state text NOT NULL CHECK (state IN (
    'declared','running','interrupted','retry_wait','completed','exhausted')),
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  retry_policy jsonb NOT NULL CHECK (jsonb_typeof(retry_policy) = 'object'),
  retry_at timestamptz,
  output jsonb,
  error jsonb,
  PRIMARY KEY (run_id, step_key),
  UNIQUE (run_id, first_seen_ordinal),
  UNIQUE (run_id, command_pos, child_index),
  FOREIGN KEY (run_id, command_pos) REFERENCES commands(run_id, command_pos),
  CHECK ((state = 'completed') = (output IS NOT NULL)),
  CHECK ((state = 'retry_wait') = (retry_at IS NOT NULL)),
  CHECK ((state IN ('retry_wait','exhausted')) = (error IS NOT NULL)),
  CHECK (error IS NULL OR jsonb_typeof(error) = 'object'),
  CHECK ((state = 'declared' AND attempt = 0) OR (state <> 'declared' AND attempt >= 1))
);

CREATE TABLE IF NOT EXISTS step_attempts (
  run_id uuid NOT NULL,
  step_key text NOT NULL,
  attempt integer NOT NULL CHECK (attempt >= 1),
  attempt_token uuid NOT NULL UNIQUE,
  fence bigint NOT NULL CHECK (fence >= 1),
  state text NOT NULL CHECK (state IN ('running','completed','failed','abandoned')),
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  PRIMARY KEY (run_id, step_key, attempt),
  FOREIGN KEY (run_id, step_key) REFERENCES steps(run_id, step_key),
  CHECK ((state = 'running') = (ended_at IS NULL))
);

CREATE TABLE IF NOT EXISTS run_queue (
  run_id uuid PRIMARY KEY REFERENCES runs(id),
  state text NOT NULL CHECK (state IN ('ready','leased','scheduled','parked')),
  priority integer NOT NULL DEFAULT 0,
  available_at timestamptz,
  lease_owner uuid,
  lease_expires_at timestamptz,
  CHECK (
    (state = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR
    (state <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
  ),
  CHECK ((state IN ('ready','scheduled')) = (available_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS queue_ready ON run_queue(priority DESC, available_at, run_id)
  WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS queue_due ON run_queue(available_at, run_id)
  WHERE state = 'scheduled';
CREATE INDEX IF NOT EXISTS queue_expired ON run_queue(lease_expires_at, run_id)
  WHERE state = 'leased';
CREATE INDEX IF NOT EXISTS runs_workflow ON runs(workflow_version_id, state, id);

CREATE TABLE IF NOT EXISTS operation_receipts (
  run_id uuid NOT NULL REFERENCES runs(id),
  operation_id uuid NOT NULL,
  request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
  response jsonb NOT NULL CHECK (jsonb_typeof(response) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (run_id, operation_id)
);

CREATE TABLE IF NOT EXISTS worker_sessions (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  protocol_version integer NOT NULL CHECK (protocol_version > 0),
  expires_at timestamptz NOT NULL,
  active_run_id uuid,
  active_fence bigint,
  history_watermark bigint,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS worker_session_versions (
  worker_session_id uuid NOT NULL REFERENCES worker_sessions(id),
  workflow_version_id uuid NOT NULL,
  tenant_id uuid NOT NULL,
  PRIMARY KEY (worker_session_id, workflow_version_id),
  FOREIGN KEY (tenant_id, workflow_version_id)
    REFERENCES workflow_versions(tenant_id, id)
);

CREATE TABLE IF NOT EXISTS worker_session_receipts (
  worker_session_id uuid NOT NULL REFERENCES worker_sessions(id),
  operation_id uuid NOT NULL,
  request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
  response jsonb NOT NULL CHECK (jsonb_typeof(response) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (worker_session_id, operation_id)
);

CREATE TABLE IF NOT EXISTS registration_receipts (
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  operation_id uuid NOT NULL,
  request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
  response jsonb NOT NULL CHECK (jsonb_typeof(response) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, operation_id)
);

CREATE OR REPLACE FUNCTION relay_alloc_events(p_run_id uuid, p_count bigint)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_first bigint;
BEGIN
  UPDATE runs
  SET next_event_seq = next_event_seq + p_count
  WHERE id = p_run_id
  RETURNING next_event_seq - p_count INTO v_first;
  IF v_first IS NULL THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
  END IF;
  RETURN v_first;
END;
$$;

CREATE OR REPLACE FUNCTION relay_create_run(
  p_tenant_id uuid,
  p_workflow_name text,
  p_workflow_version text,
  p_input jsonb,
  p_trigger_hash text,
  p_run_id uuid
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_version_id uuid;
  v_now timestamptz;
  v_seq bigint;
BEGIN
  SELECT id INTO v_version_id
  FROM workflow_versions
  WHERE tenant_id = p_tenant_id AND name = p_workflow_name AND version = p_workflow_version;
  IF v_version_id IS NULL THEN
    RAISE EXCEPTION 'WorkflowVersionMismatch' USING DETAIL = 'workflow version is not registered';
  END IF;

  SELECT clock_timestamp() INTO v_now;

  INSERT INTO runs (
    id, tenant_id, workflow_version_id, state, input, trigger_hash, current_fence
  ) VALUES (
    p_run_id, p_tenant_id, v_version_id, 'pending', p_input, p_trigger_hash, 0
  );

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
      'idempotencyKey', NULL
    ),
    v_now
  );

  INSERT INTO run_queue (run_id, state, priority, available_at)
  VALUES (p_run_id, 'ready', 0, v_now);

  RETURN jsonb_build_object('runId', p_run_id);
END;
$$;

CREATE OR REPLACE FUNCTION relay_register_worker(
  p_tenant_id uuid,
  p_worker_session_id uuid,
  p_operation_id uuid,
  p_request_hash text,
  p_protocol_version integer,
  p_workflows jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_existing jsonb;
  v_existing_hash text;
  v_item jsonb;
  v_name text;
  v_version text;
  v_version_id uuid;
  v_now timestamptz;
  v_names jsonb := '[]'::jsonb;
BEGIN
  IF p_protocol_version <> 1 THEN
    RAISE EXCEPTION 'UnsupportedCapability' USING DETAIL = 'protocol version is not supported';
  END IF;
  IF jsonb_typeof(p_workflows) <> 'array' THEN
    RAISE EXCEPTION 'Serialization' USING DETAIL = 'workflows must be an array';
  END IF;

  SELECT response, request_hash INTO v_existing, v_existing_hash
  FROM registration_receipts
  WHERE tenant_id = p_tenant_id AND operation_id = p_operation_id;
  IF FOUND THEN
    IF v_existing_hash = p_request_hash THEN
      RETURN v_existing;
    END IF;
    RAISE EXCEPTION 'IdempotencyConflict' USING DETAIL = 'registration operation id was reused';
  END IF;

  SELECT clock_timestamp() INTO v_now;
  INSERT INTO worker_sessions (id, tenant_id, protocol_version, expires_at)
  VALUES (p_worker_session_id, p_tenant_id, p_protocol_version, v_now + interval '1 day');

  FOR v_item IN SELECT value FROM jsonb_array_elements(p_workflows)
  LOOP
    v_name := v_item->>'name';
    v_version := v_item->>'version';
    IF v_name IS NULL OR v_version IS NULL OR length(v_name) = 0 OR length(v_version) = 0 THEN
      RAISE EXCEPTION 'Serialization' USING DETAIL = 'workflow name and version are required';
    END IF;
    INSERT INTO workflow_versions (id, tenant_id, name, version, protocol_version)
    VALUES (gen_random_uuid(), p_tenant_id, v_name, v_version, p_protocol_version)
    ON CONFLICT (tenant_id, name, version) DO NOTHING;
    SELECT id INTO v_version_id
    FROM workflow_versions
    WHERE tenant_id = p_tenant_id AND name = v_name AND version = v_version;
    INSERT INTO worker_session_versions (worker_session_id, workflow_version_id, tenant_id)
    VALUES (p_worker_session_id, v_version_id, p_tenant_id);
    v_names := v_names || jsonb_build_array(jsonb_build_object('name', v_name, 'version', v_version));
  END LOOP;

  v_existing := jsonb_build_object(
    'workerSessionId', p_worker_session_id,
    'workflows', v_names
  );
  INSERT INTO registration_receipts (tenant_id, operation_id, request_hash, response)
  VALUES (p_tenant_id, p_operation_id, p_request_hash, v_existing);
  RETURN v_existing;
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
  WHERE q.state = 'ready'
    AND r.state = 'pending'
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
    WHERE r.id = v_run_id
      AND m.worker_session_id = p_worker_session_id
      AND r.state = 'pending'
  ) THEN
    RAISE EXCEPTION 'WorkflowVersionMismatch' USING DETAIL = 'worker cannot execute this version';
  END IF;

  -- Locks have returned. Do not use now() here.
  SELECT clock_timestamp() INTO v_now;
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

CREATE OR REPLACE FUNCTION relay_expire_lease(p_run_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_now timestamptz;
  v_fence bigint;
  v_state text;
  v_expires timestamptz;
  v_seq bigint;
BEGIN
  PERFORM 1 FROM runs WHERE id = p_run_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run not found';
  END IF;
  PERFORM 1 FROM run_queue WHERE run_id = p_run_id FOR UPDATE;

  SELECT clock_timestamp() INTO v_now;
  SELECT current_fence INTO v_fence FROM runs WHERE id = p_run_id;
  SELECT state, lease_expires_at INTO v_state, v_expires
  FROM run_queue WHERE run_id = p_run_id;

  IF v_state IS DISTINCT FROM 'leased' THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run is not leased';
  END IF;
  IF v_expires > v_now THEN
    RAISE EXCEPTION 'NotExpired' USING DETAIL = 'lease deadline is still in the future';
  END IF;

  v_seq := relay_alloc_events(p_run_id, 1);
  INSERT INTO run_events (run_id, seq, fence, actor, type, payload)
  VALUES (
    p_run_id, v_seq, v_fence, 'system', 'LeaseExpired',
    jsonb_build_object('schemaVersion', 1, 'reason', 'expired', 'databaseNow', v_now)
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

  RETURN jsonb_build_object('runId', p_run_id, 'fence', v_fence::text, 'expired', true);
END;
$$;

CREATE OR REPLACE FUNCTION relay_lookup_receipt(
  p_run_id uuid,
  p_operation_id uuid,
  p_request_hash text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_hash text;
  v_response jsonb;
BEGIN
  SELECT request_hash, response INTO v_hash, v_response
  FROM operation_receipts
  WHERE run_id = p_run_id AND operation_id = p_operation_id;
  IF NOT FOUND THEN
    RETURN NULL;
  END IF;
  IF v_hash = p_request_hash THEN
    RETURN v_response;
  END IF;
  RAISE EXCEPTION 'IdempotencyConflict' USING DETAIL = 'operation id was reused with a different request';
END;
$$;

CREATE OR REPLACE FUNCTION relay_require_lease(
  p_run_id uuid,
  p_worker_session_id uuid,
  p_fence bigint,
  p_now timestamptz
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_run_state text;
  v_fence bigint;
  v_queue_state text;
  v_owner uuid;
  v_expires timestamptz;
BEGIN
  SELECT state, current_fence INTO v_run_state, v_fence FROM runs WHERE id = p_run_id;
  SELECT state, lease_owner, lease_expires_at
    INTO v_queue_state, v_owner, v_expires
  FROM run_queue WHERE run_id = p_run_id;

  IF v_run_state IN ('completed', 'failed', 'cancelled') THEN
    RAISE EXCEPTION 'InvalidRunState' USING DETAIL = 'run is terminal';
  END IF;
  IF v_queue_state IS DISTINCT FROM 'leased' OR v_owner IS DISTINCT FROM p_worker_session_id THEN
    RAISE EXCEPTION 'LeaseLost' USING DETAIL = 'worker does not own the lease';
  END IF;
  IF v_fence IS DISTINCT FROM p_fence THEN
    RAISE EXCEPTION 'FenceViolation' USING DETAIL = 'fence does not match';
  END IF;
  IF v_expires IS NULL OR NOT (v_expires > p_now) THEN
    RAISE EXCEPTION 'LeaseLost' USING DETAIL = 'lease is expired';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION relay_declare_and_start(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_step_key text,
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
  v_pos bigint;
  v_ordinal bigint;
  v_seq bigint;
  v_token uuid;
  v_descriptor jsonb;
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
    'explicitInput', false
  );
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
    run_id, step_key, command_pos, child_index, first_seen_ordinal, state, attempt, retry_policy
  ) VALUES (
    p_run_id, p_step_key, v_pos, 0, v_ordinal, 'running', 1, '{"maxAttempts":1}'::jsonb
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
  v_new_attempt integer;
  v_token uuid;
  v_seq bigint;
  v_response jsonb;
  v_step_state text;
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

  SELECT state, command_pos, first_seen_ordinal
    INTO v_step_state, v_command_pos, v_ordinal
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

CREATE OR REPLACE FUNCTION relay_complete_step(
  p_operation_id uuid,
  p_worker_session_id uuid,
  p_run_id uuid,
  p_fence bigint,
  p_step_key text,
  p_attempt integer,
  p_attempt_token uuid,
  p_result jsonb,
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
  SELECT attempt_token, fence, state
    INTO v_token, v_attempt_fence, v_attempt_state
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
    p_run_id, v_seq, p_fence, 'worker', 'StepCompleted', v_command_pos, p_step_key, p_attempt,
    jsonb_build_object('schemaVersion', 1, 'result', p_result)
  );
  UPDATE step_attempts
  SET state = 'completed', ended_at = v_now
  WHERE run_id = p_run_id AND step_key = p_step_key AND attempt = p_attempt;
  UPDATE steps
  SET state = 'completed', output = p_result
  WHERE run_id = p_run_id AND step_key = p_step_key;

  v_response := jsonb_build_object(
    'attempt', p_attempt,
    'attemptToken', p_attempt_token,
    'result', p_result
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
  UPDATE runs
  SET state = 'completed',
      output = p_output,
      completed_at = v_now
  WHERE id = p_run_id;
  DELETE FROM run_queue WHERE run_id = p_run_id;
  UPDATE worker_sessions
  SET active_run_id = NULL,
      active_fence = NULL,
      history_watermark = NULL
  WHERE id = p_worker_session_id AND active_run_id = p_run_id;

  v_response := jsonb_build_object('state', 'completed', 'output', p_output);
  INSERT INTO operation_receipts (run_id, operation_id, request_hash, response)
  VALUES (p_run_id, p_operation_id, p_request_hash, v_response);
  RETURN v_response;
END;
$$;

REVOKE ALL ON FUNCTION relay_alloc_events(uuid, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_lookup_receipt(uuid, uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_require_lease(uuid, uuid, bigint, timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION relay_expire_lease(uuid) FROM PUBLIC;

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM relay_app;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO relay_app;

GRANT EXECUTE ON FUNCTION relay_create_run(uuid, text, text, jsonb, text, uuid) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_register_worker(uuid, uuid, uuid, text, integer, jsonb) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_grant_lease(uuid, uuid, text, interval) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_declare_and_start(uuid, uuid, uuid, bigint, text, bigint, text) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_restart_attempt(uuid, uuid, uuid, bigint, text, text) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_complete_step(uuid, uuid, uuid, bigint, text, integer, uuid, jsonb, text) TO relay_app;
GRANT EXECUTE ON FUNCTION relay_finish_run(uuid, uuid, uuid, bigint, jsonb, text) TO relay_app;
