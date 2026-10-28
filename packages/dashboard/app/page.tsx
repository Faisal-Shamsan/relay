import { readJson, type RunSummary } from "../lib/api";

export default async function Home({
  searchParams,
}: {
  searchParams: Promise<{ state?: string; workflow?: string; version?: string }>;
}) {
  const query = await searchParams;
  const params = new URLSearchParams();
  if (query.state) params.set("state", query.state);
  if (query.workflow) params.set("workflow", query.workflow);
  if (query.version) params.set("version", query.version);
  const suffix = params.size === 0 ? "" : `?${params.toString()}`;
  let runs: RunSummary[] = [];
  let problem = "";
  try {
    const page = await readJson<{ runs: RunSummary[] }>(`/v1/runs${suffix}`);
    runs = page.runs;
  } catch (error) {
    problem = error instanceof Error ? error.message : "the run list could not be read";
  }
  return (
    <>
      <h1>Runs</h1>
      <form action="/" method="get">
        <label>
          State
          <input name="state" defaultValue={query.state ?? ""} />
        </label>
        <label>
          Workflow
          <input name="workflow" defaultValue={query.workflow ?? ""} />
        </label>
        <label>
          Version
          <input name="version" defaultValue={query.version ?? ""} />
        </label>
        <button type="submit">Filter</button>
      </form>
      {problem ? <p>{problem}</p> : null}
      <table>
        <thead>
          <tr>
            <th>Run</th>
            <th>State</th>
            <th>Workflow</th>
            <th>Version</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <tr key={run.runId}>
              <td><a href={`/runs/${run.runId}`}>{run.runId}</a></td>
              <td>{run.state}</td>
              <td>{run.workflowName}</td>
              <td>{run.workflowVersion}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}
