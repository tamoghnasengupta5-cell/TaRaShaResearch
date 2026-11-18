/** Only the current mounted selection may apply an asynchronous saved view. */
export function createSavedSelectionRequest<T>() {
  let generation = 0;
  let active: AbortController | null = null;
  const cancel = () => { generation += 1; active?.abort(); active = null; };
  return {
    cancel,
    async run(load: (signal: AbortSignal) => Promise<T>, callbacks: {
      ready: (result: T) => void;
      failed: (error: unknown) => void;
      settled: () => void;
    }, isCurrent: () => boolean = () => true): Promise<void> {
      cancel();
      const token = generation;
      const controller = new AbortController();
      active = controller;
      const owns = () => token === generation && !controller.signal.aborted && isCurrent();
      try {
        const result = await load(controller.signal);
        if (owns()) callbacks.ready(result);
      } catch (error) {
        if (owns()) callbacks.failed(error);
      } finally {
        if (token === generation) {
          active = null;
          if (!controller.signal.aborted && isCurrent()) callbacks.settled();
        }
      }
    },
  };
}
