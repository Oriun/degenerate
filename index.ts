/*
  A single-file TypeScript helper for making HTTP requests with
  two-level rate limiting:
    1) API-level (endpoint groups)
    2) Custom-level (e.g., by userId, token, header, IP, etc.)

  Features
  - Sliding window limits (N requests per interval)
  - Optional max concurrency per group
  - Endpoint grouping via method+URL pattern matching
  - Pluggable fetch (defaults to global fetch)
  - Retries with exponential backoff + jitter on retryable errors
  - AbortController support, timeouts, and hooks

  Usage (example at bottom of file, commented out)
*/

// ----------------------------- Types -----------------------------
export type HttpMethod =
  | "GET"
  | "POST"
  | "PUT"
  | "PATCH"
  | "DELETE"
  | "HEAD"
  | "OPTIONS";

export type EndpointPattern = {
  /** HTTP method to match. Use "*" to match all methods. */
  method: HttpMethod | "*";
  /** URL pattern to match; supports RegExp or simple wildcard string ("*", "?", "**"). */
  url: RegExp | string;
  /** Name of the API group (bucket) this pattern maps to. */
  group: string;
};

export interface SlidingWindowLimit {
  /** Max number of requests allowed in the interval. */
  max: number;
  /** Interval window size in ms. */
  intervalMs: number;
  /** Optional max number of concurrently running requests. */
  maxConcurrent?: number;
}

export interface GroupLimitsConfig {
  /** Default limits applied if no group-specific override exists. */
  default: SlidingWindowLimit;
  /** Per-group overrides. */
  groups?: Record<string, Partial<SlidingWindowLimit>>;
}

export interface TwoLevelLimits {
  /** API (endpoint groups) level limits. */
  api: GroupLimitsConfig;
  /** Custom level limits (e.g., by user/token/header). */
  custom: GroupLimitsConfig;
}

export interface RetryPolicy {
  /** How many retry attempts on retryable failures. */
  retries: number;
  /** Base delay in ms for exponential backoff. */
  baseDelayMs?: number; // default 200ms
  /** Max backoff delay in ms. */
  maxDelayMs?: number; // default 5_000ms
  /** Which HTTP status codes are considered retryable. */
  retryOnStatuses?: number[]; // default [429, 502, 503, 504]
}

export interface RequestHookArgs {
  url: string;
  method: HttpMethod;
  init: RequestInit & { method?: string };
  apiGroup: string;
  customGroup: string;
  attempt: number;
}

export interface ClientHooks {
  /** Called just before dispatching the network request (after permits acquired). */
  beforeRequest?: (args: RequestHookArgs) => void | Promise<void>;
  /** Called after receiving a response (success or failure). */
  afterResponse?: (
    args: RequestHookArgs & { response?: Response; error?: unknown }
  ) => void | Promise<void>;
}

export interface ClientConfig {
  limits: TwoLevelLimits;
  /** Patterns mapping endpoints to API groups. First match wins; fallback to "default". */
  endpointGroups?: EndpointPattern[];
  /** Function to compute the custom group key from request. Default: "default". */
  customKey?: (args: { url: string; method: HttpMethod; init: RequestInit }) => string;
  /** Optional per-request timeout, in ms. */
  defaultTimeoutMs?: number;
  /** Provide a custom fetch implementation (e.g., node-fetch). */
  fetchImpl?: typeof fetch;
  /** Optional retry policy. */
  retry?: RetryPolicy;
  /** Optional hooks. */
  hooks?: ClientHooks;
}

export interface RequestOptions {
  method?: HttpMethod;
  init?: RequestInit;
  /** Override the computed API group. */
  apiGroupOverride?: string;
  /** Override the computed custom group. */
  customGroupOverride?: string;
  /** Per-request retry policy override. */
  retry?: Partial<RetryPolicy>;
  /** Per-request timeout override (ms). */
  timeoutMs?: number;
}

// ---------------------- Utility: wildcard match ----------------------
function wildcardToRegExp(pattern: string): RegExp {
  // Support ** (any path), * (any segment), ? (single char). Escape regex significant chars.
  const escaped = pattern
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*\*/g, ".*")
    .replace(/\*/g, "[^/]*")
    .replace(/\?/g, ".");
  return new RegExp(`^${escaped}$`);
}

function matchEndpoint(
  url: string,
  method: HttpMethod,
  patterns: EndpointPattern[] | undefined
): string {
  if (!patterns || patterns.length === 0) return "default";
  for (const p of patterns) {
    if (p.method !== "*" && p.method !== method) continue;
    const re = typeof p.url === "string" ? wildcardToRegExp(p.url) : p.url;
    if (re.test(url)) return p.group;
  }
  return "default";
}

// ---------------------- Sliding Window Limiter ----------------------
class SlidingWindowLimiter {
  private max: number;
  private intervalMs: number;
  private maxConcurrent: number;

  private timestamps: number[] = []; // request start times
  private running = 0;
  private queue: Array<{
    resolve: () => void;
    reject: (err: unknown) => void;
  }> = [];

  constructor(cfg: SlidingWindowLimit) {
    this.max = cfg.max;
    this.intervalMs = cfg.intervalMs;
    this.maxConcurrent = cfg.maxConcurrent ?? cfg.max;
  }

  private prune(now: number) {
    const cutoff = now - this.intervalMs;
    while (this.timestamps.length && this.timestamps[0] <= cutoff) {
      this.timestamps.shift();
    }
  }

  /** Acquire a permit; resolves when request may start. */
  acquire(): Promise<void> {
    return new Promise((resolve, reject) => {
      const tryAcquire = () => {
        const now = Date.now();
        this.prune(now);
        const withinRate = this.timestamps.length < this.max;
        const withinConcurrency = this.running < this.maxConcurrent;
        if (withinRate && withinConcurrency) {
          this.timestamps.push(now);
          this.running += 1;
          resolve();
          return;
        }
        // Need to wait; compute next eligible time based on oldest timestamp and/or running
        this.queue.push({ resolve: tryAcquire, reject });
        this.scheduleWake();
      };
      tryAcquire();
    });
  }

  /** Release a running slot and wake queued waiters if capacity is available. */
  release() {
    this.running = Math.max(0, this.running - 1);
    this.scheduleWake();
  }

  private wakeQueued() {
    if (this.queue.length === 0) return;
    // Attempt to acquire for as many waiters as possible
    let progressed = false;
    let i = 0;
    while (i < this.queue.length) {
      const waiter = this.queue[i];
      const now = Date.now();
      this.prune(now);
      const withinRate = this.timestamps.length < this.max;
      const withinConcurrency = this.running < this.maxConcurrent;
      if (withinRate && withinConcurrency) {
        this.timestamps.push(now);
        this.running += 1;
        // Remove waiter and resolve by calling its resolve (which re-checks constraints)
        this.queue.splice(i, 1);
        progressed = true;
        waiter.resolve();
      } else {
        i++;
      }
    }
    if (!progressed) this.scheduleWake();
  }

  private wakeTimer: ReturnType<typeof setTimeout> | null = null;
  private scheduleWake() {
    if (this.wakeTimer) return;
    const now = Date.now();
    this.prune(now);
    const oldest = this.timestamps[0];
    let delay = 25; // small poll if no timestamps
    if (oldest != null) {
      const until = oldest + this.intervalMs - now + 1;
      delay = Math.max(1, until);
    }
    this.wakeTimer = setTimeout(() => {
      this.wakeTimer = null;
      this.wakeQueued();
    }, delay);
  }
}

// ---------------------- Grouped Limiters (bucket manager) ----------------------
class GroupLimiter {
  private defaults: SlidingWindowLimit;
  private overrides: Record<string, Partial<SlidingWindowLimit>>;
  private instances = new Map<string, SlidingWindowLimiter>();

  constructor(cfg: GroupLimitsConfig) {
    this.defaults = cfg.default;
    this.overrides = cfg.groups ?? {};
  }

  private getCfgFor(group: string): SlidingWindowLimit {
    const over = this.overrides[group] ?? {};
    return {
      max: over.max ?? this.defaults.max,
      intervalMs: over.intervalMs ?? this.defaults.intervalMs,
      maxConcurrent: over.maxConcurrent ?? this.defaults.maxConcurrent,
    };
  }

  get(group: string): SlidingWindowLimiter {
    let lim = this.instances.get(group);
    if (!lim) {
      lim = new SlidingWindowLimiter(this.getCfgFor(group));
      this.instances.set(group, lim);
    }
    return lim;
  }
}

// --------------------------- HTTP Client ---------------------------
export class RateLimitedHttpClient {
  private apiLimiter: GroupLimiter;
  private customLimiter: GroupLimiter;
  private endpointGroups?: EndpointPattern[];
  private customKeyFn: Required<ClientConfig>["customKey"];
  private fetchImpl: typeof fetch;
  private retryDefaults: Required<RetryPolicy>;
  private hooks?: ClientHooks;
  private defaultTimeoutMs?: number;

  constructor(cfg: ClientConfig) {
    this.apiLimiter = new GroupLimiter(cfg.limits.api);
    this.customLimiter = new GroupLimiter(cfg.limits.custom);
    this.endpointGroups = cfg.endpointGroups;
    this.customKeyFn = cfg.customKey ?? (() => "default");
    this.fetchImpl = cfg.fetchImpl ?? fetch.bind(globalThis);
    this.hooks = cfg.hooks;
    this.defaultTimeoutMs = cfg.defaultTimeoutMs;
    this.retryDefaults = {
      retries: cfg.retry?.retries ?? 2,
      baseDelayMs: cfg.retry?.baseDelayMs ?? 200,
      maxDelayMs: cfg.retry?.maxDelayMs ?? 5_000,
      retryOnStatuses: cfg.retry?.retryOnStatuses ?? [429, 502, 503, 504],
    };
  }

  private computeGroups(url: string, method: HttpMethod, init: RequestInit, opts?: RequestOptions) {
    const apiGroup =
      opts?.apiGroupOverride ?? matchEndpoint(url, method, this.endpointGroups);
    const customGroup =
      opts?.customGroupOverride ?? this.customKeyFn({ url, method, init });
    return { apiGroup, customGroup };
  }

  private async backoffDelay(attempt: number, policy: Required<RetryPolicy>) {
    const exp = Math.min(
      policy.baseDelayMs * Math.pow(2, attempt - 1),
      policy.maxDelayMs
    );
    // full jitter
    const jitter = Math.random() * exp;
    return jitter;
  }

  private isRetryableStatus(status: number, policy: Required<RetryPolicy>) {
    return policy.retryOnStatuses.includes(status);
  }

  async request(url: string, options: RequestOptions = {}): Promise<Response> {
    const method = (options.method ?? (options.init?.method as HttpMethod) ?? "GET").toUpperCase() as HttpMethod;
    const init: RequestInit = { ...options.init, method };
    const { apiGroup, customGroup } = this.computeGroups(url, method, init, options);

    const apiBucket = this.apiLimiter.get(apiGroup);
    const customBucket = this.customLimiter.get(customGroup);

    const retryPolicy: Required<RetryPolicy> = {
      ...this.retryDefaults,
      ...options.retry,
      retries: options.retry?.retries ?? this.retryDefaults.retries,
      baseDelayMs: options.retry?.baseDelayMs ?? this.retryDefaults.baseDelayMs,
      maxDelayMs: options.retry?.maxDelayMs ?? this.retryDefaults.maxDelayMs,
      retryOnStatuses:
        options.retry?.retryOnStatuses ?? this.retryDefaults.retryOnStatuses,
    } as Required<RetryPolicy>;

    const timeoutMs = options.timeoutMs ?? this.defaultTimeoutMs;

    let attempt = 0;
    let lastError: unknown;

    while (attempt <= retryPolicy.retries) {
      attempt++;
      await apiBucket.acquire();
      await customBucket.acquire();

      let controller: AbortController | undefined;
      let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
      try {
        if (timeoutMs && !init.signal) {
          controller = new AbortController();
          init.signal = controller.signal;
          timeoutHandle = setTimeout(() => controller?.abort(), timeoutMs);
        }

        await this.hooks?.beforeRequest?.({
          url,
          method,
          init: init as RequestInit & { method?: string },
          apiGroup,
          customGroup,
          attempt,
        });

        const res = await this.fetchImpl(url, init);

        await this.hooks?.afterResponse?.({
          url,
          method,
          init: init as RequestInit & { method?: string },
          apiGroup,
          customGroup,
          attempt,
          response: res,
        });

        if (!res.ok && this.isRetryableStatus(res.status, retryPolicy) && attempt <= retryPolicy.retries) {
          // Retryable HTTP error
          const delay = await this.backoffDelay(attempt, retryPolicy);
          lastError = new Error(`HTTP ${res.status} on attempt ${attempt}`);
          apiBucket.release();
          customBucket.release();
          if (timeoutHandle) clearTimeout(timeoutHandle);
          await new Promise((r) => setTimeout(r, delay));
          continue;
        }

        return res; // success or non-retryable failure (let caller handle)
      } catch (err) {
        lastError = err;
        await this.hooks?.afterResponse?.({
          url,
          method,
          init: init as RequestInit & { method?: string },
          apiGroup,
          customGroup,
          attempt,
          error: err,
        });
        // Retry on AbortError only if caused by timeout and we still have attempts left
        const isAbort = (err as any)?.name === "AbortError";
        if (isAbort && attempt <= retryPolicy.retries) {
          const delay = await this.backoffDelay(attempt, retryPolicy);
          apiBucket.release();
          customBucket.release();
          if (timeoutHandle) clearTimeout(timeoutHandle);
          await new Promise((r) => setTimeout(r, delay));
          continue;
        }
        throw err;
      } finally {
        if (timeoutHandle) clearTimeout(timeoutHandle);
        apiBucket.release();
        customBucket.release();
      }
    }

    // Exhausted retries
    throw lastError ?? new Error("Request failed after retries");
  }
}

// --------------------------- Factory ---------------------------
export function createRateLimitedClient(cfg: ClientConfig) {
  const client = new RateLimitedHttpClient(cfg);
  return {
    request: client.request.bind(client),
  };
}

// --------------------------- Example ---------------------------
/*
// Example: Create a client with two-level rate limiting
const client = createRateLimitedClient({
  limits: {
    api: {
      default: { max: 10, intervalMs: 1000, maxConcurrent: 5 },
      groups: {
        auth: { max: 5, intervalMs: 1000 },
        search: { max: 20, intervalMs: 1000 },
      },
    },
    custom: {
      default: { max: 50, intervalMs: 1000 },
      groups: {
        premium: { max: 100, intervalMs: 1000 },
        free: { max: 10, intervalMs: 1000 },
      },
    },
  },
  endpointGroups: [
    { method: "POST", url: "/v1/auth/**", group: "auth" },
    { method: "GET", url: "/v1/search/**", group: "search" },
    { method: "*", url: "/v1/**", group: "default" },
  ],
  customKey: ({ init }) =>
    (init.headers instanceof Headers
      ? init.headers.get("X-User-Tier")
      : (init.headers as Record<string, string> | undefined)?.["X-User-Tier"]) ||
    "free",
  defaultTimeoutMs: 7000,
  retry: { retries: 3 },
});

// Later: perform requests; buckets applied by endpoint+custom key
const res = await client.request("https://api.example.com/v1/search/items", {
  method: "GET",
  init: { headers: { "X-User-Tier": "premium" } },
});

if (!res.ok) throw new Error(`Bad status ${res.status}`);
const data = await res.json();
*/
