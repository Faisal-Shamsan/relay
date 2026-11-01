# Approval example

This worker runs outside the Relay server. Compose does not start it, and the server image does not contain it.

Build the SDK from the repository root, then start the worker against the server:

```sh
npm run build
node examples/approval/worker.mjs
```

The worker key is the local `relay_dev_worker_key`. The client key below is the local `relay_dev_client_key`.

Create a run after the worker is polling:

```sh
curl -s -X POST http://127.0.0.1:8080/v1/runs -H "authorization: Bearer relay_dev_client_key" -H "content-type: application/json" -d "{\"workflowName\":\"approval\",\"workflowVersion\":\"approval-v1\",\"input\":{}}"
```

The worker parks. Signal that run:

```sh
curl -s -X POST http://127.0.0.1:8080/v1/runs/<run-id>/signal -H "authorization: Bearer relay_dev_client_key" -H "content-type: application/json" -d "{\"name\":\"approval\",\"payload\":{\"approved\":true},\"idempotencyKey\":\"approval-1\"}"
```

The worker then applies the decision and prints the result. Cancel is the other ending. It leaves the run cancelled and does not apply the step:

```sh
curl -s -X POST http://127.0.0.1:8080/v1/runs/<run-id>/cancel -H "authorization: Bearer relay_dev_client_key" -H "content-type: application/json" -d "{\"protocolVersion\":1}"
```

Read the history:

```sh
curl -s "http://127.0.0.1:8080/v1/runs/<run-id>/events?limit=100" -H "authorization: Bearer relay_dev_client_key"
```
