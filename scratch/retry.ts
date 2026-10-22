// Retries fn up to `attempts` times, waiting `delayMs` between tries.
export async function retry<T>(fn: () => Promise<T>, attempts: number, delayMs: number): Promise<T> {
  let lastError;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (i < attempts - 1) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }
  }
  throw lastError;
}
