import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchSecWithBackoff } from "../functions/api/[[path]]";

describe("SEC proxy backoff", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("retries transient SEC refusals and returns the successful response", async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(null, { status: 403 }))
      .mockResolvedValueOnce(new Response(null, { status: 429 }))
      .mockResolvedValueOnce(new Response('{"entityName":"Example"}', { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const responsePromise = fetchSecWithBackoff("https://data.sec.gov/example.json", "TaRaSha admin@example.com");
    await vi.runAllTimersAsync();
    const response = await responsePromise;

    expect(response.status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(fetchMock.mock.calls[0][1]?.headers).toMatchObject({
      "User-Agent": "TaRaSha admin@example.com",
      Accept: "application/json",
    });
  });
});
