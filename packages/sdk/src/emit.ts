const FLUSH_BYTES = 4096;
const FLUSH_MS = 100;

export interface AttemptEmit {
  emit(text: string): Promise<void>;
  flush(): Promise<void>;
  stop(): void;
  finalBatchSeq(): string;
}

export function createAttemptEmit(send: (text: string, batchSeq: number) => Promise<void>): AttemptEmit {
  let buffer = "";
  let lastBatch = -1;
  let sealed = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let chain: Promise<void> = Promise.resolve();

  const exclusive = (run: () => Promise<void>): Promise<void> => {
    const next = chain.then(run, run);
    chain = next.then(() => undefined, () => undefined);
    return next;
  };

  const clearTimer = (): void => {
    if (timer) clearTimeout(timer);
    timer = undefined;
  };

  const drain = async (): Promise<void> => {
    clearTimer();
    while (buffer.length > 0) {
      const split = takeUtf8Prefix(buffer, FLUSH_BYTES);
      const batchSeq = lastBatch + 1;
      await send(split.chunk, batchSeq);
      lastBatch = batchSeq;
      buffer = split.rest;
    }
  };

  const armTimer = (): void => {
    if (timer || buffer.length === 0 || sealed) return;
    timer = setTimeout(() => {
      timer = undefined;
      void exclusive(async () => {
        if (!sealed) await drain();
      }).catch(() => undefined);
    }, FLUSH_MS);
  };

  return {
    emit(text: string): Promise<void> {
      return exclusive(async () => {
        if (text.length === 0) return;
        if (sealed) {
          const error = new Error("emit is closed");
          error.name = "Serialization";
          throw error;
        }
        buffer += text;
        while (Buffer.byteLength(buffer, "utf8") >= FLUSH_BYTES) {
          const split = takeUtf8Prefix(buffer, FLUSH_BYTES);
          const batchSeq = lastBatch + 1;
          await send(split.chunk, batchSeq);
          lastBatch = batchSeq;
          buffer = split.rest;
        }
        armTimer();
      });
    },
    flush(): Promise<void> {
      return exclusive(async () => {
        sealed = true;
        await drain();
      });
    },
    stop(): void {
      clearTimer();
    },
    finalBatchSeq(): string {
      return String(lastBatch);
    },
  };
}

function takeUtf8Prefix(text: string, maxBytes: number): { chunk: string; rest: string } {
  if (Buffer.byteLength(text, "utf8") <= maxBytes) return { chunk: text, rest: "" };
  let bytes = 0;
  let units = 0;
  for (const char of text) {
    const size = Buffer.byteLength(char, "utf8");
    if (bytes + size > maxBytes) break;
    bytes += size;
    units += char.length;
  }
  if (units === 0) return { chunk: text.slice(0, 1), rest: text.slice(1) };
  return { chunk: text.slice(0, units), rest: text.slice(units) };
}
