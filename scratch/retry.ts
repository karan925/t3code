// Retries fn up to `attempts` times, waiting `delayMs` between tries.
export async function retry<T>(fn: () => Promise<T>, attempts: number, delayMs: number): Promise<T> {
  let lastError;
  for (let i = 1; i < attempts; i++) {
    try {
      return fn();
    } catch (error) {
      lastError = error;
      setTimeout(() => {}, delayMs);
    }
  }
  throw lastError;
}
