import { readJson, type HistoryEvent, type RunDetail, type StepView } from "../../../lib/api";
import { StreamView } from "./stream-view";

export default async function RunPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  let problem = "";
  let run: RunDetail | null = null;
  let steps: StepView[] = [];
  let events: HistoryEvent[] = [];
  try {
    run = await readJson<RunDetail>(`/v1/runs/${id}`);
    const stepPage = await readJson<{ steps: StepView[] }>(`/v1/runs/${id}/steps`);
    const eventPage = await readJson<{ events: HistoryEvent[] }>(`/v1/runs/${id}/events?limit=1000`);
    steps = stepPage.steps;
    events = eventPage.events;
  } catch (error) {
    problem = error instanceof Error ? error.message : "the run could not be read";
  }
  if (!run) return <p>{problem || "Run not found"}</p>;
  return (
    <>
      <h1>{run.runId}</h1>
      <p>{run.workflowName} {run.workflowVersion} · {run.state}</p>
      <p>
        Fence {run.ownership.fence}
        {run.ownership.queueState ? ` · queue ${run.ownership.queueState}` : ""}
        {run.ownership.leaseOwner ? ` · owner ${run.ownership.leaseOwner}` : ""}
        {run.ownership.leaseExpiresAt ? ` · lease until ${run.ownership.leaseExpiresAt}` : ""}
      </p>
      {run.output !== null ? <pre>{JSON.stringify(run.output, null, 2)}</pre> : null}
      {run.error !== null ? <pre>{JSON.stringify(run.error, null, 2)}</pre> : null}
      <h2>Steps</h2>
      <table>
        <thead>
          <tr>
            <th>Step</th>
            <th>State</th>
            <th>Input</th>
            <th>Output</th>
            <th>Attempts</th>
          </tr>
        </thead>
        <tbody>
          {steps.map((step) => (
            <tr key={step.stepKey}>
              <td>{step.stepKey}</td>
              <td>{step.state}</td>
              <td>{step.explicitInput ? JSON.stringify(step.input) : "omitted"}</td>
              <td>{step.output === null ? "" : JSON.stringify(step.output)}</td>
              <td>
                {step.attempts.map((attempt) => (
                  <div key={attempt.attempt}>
                    {attempt.attempt} {attempt.state} fence {attempt.fence}
                  </div>
                ))}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <h2>Timeline</h2>
      <ol>
        {events.map((event) => (
          <li key={event.seq}>
            {event.seq} {classify(event)}
            {event.stepKey ? ` ${event.stepKey}` : ""}
            {event.attempt === null ? "" : ` attempt ${event.attempt}`}
            {` fence ${event.fence}`}
          </li>
        ))}
      </ol>
      <h2>Stream</h2>
      <StreamView runId={run.runId} />
    </>
  );
}

function classify(event: HistoryEvent): string {
  if (event.type === "LeaseGranted" || event.type === "LeaseExpired" || event.type === "LeaseReleased") {
    return "lease epoch";
  }
  if (event.type === "StepAttemptAbandoned") return "abandoned attempt";
  if (event.type === "RunSuspended" && event.payload.reason === "retry") return "retry";
  if (event.type === "StepFailed" && event.payload.exhausted === false) return "retry";
  if (event.type === "StepStarted" || event.type === "StepCompleted" || event.type === "StepFailed") {
    return "business attempt";
  }
  return event.type;
}
