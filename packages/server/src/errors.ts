export class HttpError extends Error {
  readonly details: Record<string, unknown>;

  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    details: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = "HttpError";
    this.details = details;
  }
}

const STATUS: Record<string, number> = {
  IdempotencyConflict: 409,
  FenceViolation: 409,
  LeaseLost: 409,
  AttemptMismatch: 409,
  InvalidRunState: 404,
  WorkflowVersionMismatch: 404,
  Serialization: 422,
  UnsupportedCapability: 422,
  DuplicateStepKey: 409,
  Nondeterminism: 409,
  NotExpired: 409,
  StreamBatchConflict: 409,
  StreamGap: 409,
  StreamManifest: 409,
  StreamExpired: 410,
  Unauthorized: 401,
};

export function fromDatabaseError(error: unknown): HttpError | null {
  if (typeof error !== "object" || error === null || !("message" in error)) return null;
  const message = String((error as { message: string }).message);
  const status = STATUS[message];
  if (status === undefined) return null;
  const detail =
    "detail" in error && typeof (error as { detail?: unknown }).detail === "string"
      ? (error as { detail: string }).detail
      : message;
  return new HttpError(status, message, detail);
}

export function decimalString(value: unknown, field: string): string {
  if (typeof value !== "string" || !/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new HttpError(422, "Serialization", `${field} must be a decimal string`);
  }
  return value;
}
