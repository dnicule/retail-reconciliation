/**
 * @file Exercises continuous listener wakeups and shutdown with deterministic substitutes.
 * No product/database writes occur; barriers stage overlapping signals without timing guesses.
 */
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { test } from 'node:test';
import { setImmediate } from 'node:timers/promises';
import { runContinuous } from '../../src/runtime.js';
import type { RuntimeDependencies, ListenerConnection } from '../../src/runtime.js';
import { SystemicError } from '../../src/model/diagnostic.js';
import type { Job } from '../../src/model/job.js';

/** Build an internal claim fixture whose fields model already committed ownership. */
function job(id: string): Job {
  return {
    jobId: id,
    productId: 'SKU-X',
    kind: 'update',
    revision: 1,
    approvedPayload: { name: 'Product', description: '', category: 'Test' },
    status: 'processing',
    attemptCount: 1,
    leaseToken: 'private',
    leaseExpiresAt: new Date(Date.now() + 30000).toISOString(),
    nextAttemptAt: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    completedAt: null,
    outcomes: { sql: 'unknown', mongo: 'unknown' },
    lastError: null,
    conflictDetails: null,
  };
}

/** Create minimal queue dependencies; individual scenarios replace only their relevant boundary. */
function dependencies(): RuntimeDependencies {
  return {
    jobs: {
      /** Empty queues terminate the current drain. */ async claim() {
        return { kind: 'none' };
      },
    },
    /** Mark confirmed completion without touching a real database. */ async process(value) {
      return { status: 'completed', job: { ...value, status: 'completed' } };
    },
    /** Readiness has no side effects in these scheduler tests. */ async ready() {},
    /** Discard test logs. */ log() {},
  };
}

/** A dedicated connection substitute emits the same lifecycle signals as pg.Client. */
class Listener extends EventEmitter implements ListenerConnection {
  readonly calls: string[] = [];
  /** Acknowledge connection establishment after recording the ordering boundary. */ async connect(): Promise<void> {
    this.calls.push('connect');
  }
  /** Acknowledge subscription; no SQL executes in unit tests. */ async query(
    text: string,
  ): Promise<unknown> {
    this.calls.push(text);
    return {};
  }
  /** Record resource closure so stopped listeners cannot be forgotten. */ async end(): Promise<void> {
    this.calls.push('end');
  }
}

test('continuous subscribes before scanning and stops on a systemic error', /** Authentication/schema faults must not exhaust the rest of the queue. */ async () => {
  const deps = dependencies(),
    listener = new Listener(),
    signal = new AbortController();
  deps.jobs.claim = /** Subscription is confirmed before any row can be claimed. */ async () => {
    assert.deepEqual(listener.calls, ['connect', 'LISTEN reconciliation_ready']);
    throw new SystemicError('TEST_SCHEMA', 'Incompatible test schema.');
  };
  await assert.rejects(
    runContinuous(deps, {
      listenerFactory: /** Borrow one fake connection. */ () => listener,
      signal: signal.signal,
    }),
    SystemicError,
  );
  assert.equal(listener.calls.at(-1), 'end');
});

test('periodic scan discovers missed notifications after 30 seconds and shutdown closes the listener', /** Advance only the polling clock while preserving asynchronous drain ordering. */ async (t) => {
  const deps = dependencies(),
    listener = new Listener(),
    controller = new AbortController();
  let scans = 0,
    checks = 0;
  deps.ready =
    /** Compatibility is established once, even when an idle queue keeps polling. */ async () => {
      checks += 1;
    };
  deps.jobs.claim = /** Stop only after polling discovers the second scan. */ async () => {
    scans += 1;
    if (scans === 2) controller.abort();
    return { kind: 'none' };
  };
  t.mock.timers.enable({ apis: ['setInterval'] });
  const running = runContinuous(deps, {
    listenerFactory: /** Return a connected test surface. */ () => listener,
    signal: controller.signal,
  });
  try {
    // Let subscription, the immediate startup scan, and interval registration finish.
    await setImmediate();
    assert.equal(scans, 1);
    t.mock.timers.tick(29_999);
    await setImmediate();
    assert.equal(scans, 1);
    t.mock.timers.tick(1);
    await setImmediate();
    assert.equal(scans, 2);
  } finally {
    controller.abort();
    await running;
  }
  assert.equal(scans, 2);
  assert.equal(checks, 1);
  assert.equal(listener.calls.at(-1), 'end');
});

test('overlapping notifications do not claim while an attempt is running', /** A promise barrier holds the only concurrency slot. */ async () => {
  const deps = dependencies(),
    listener = new Listener(),
    controller = new AbortController();
  const entered = barrier(),
    release = barrier();
  let claims = 0;
  deps.jobs.claim = /** Only the first request owns a job. */ async () => {
    claims += 1;
    return claims === 1 ? { kind: 'claimed', job: job('one') } : { kind: 'none' };
  };
  deps.process = /** Hold the attempt while many wakeups arrive. */ async () => {
    entered.resolve();
    await release.promise;
    controller.abort();
    return { status: 'completed', job: null };
  };
  const running = runContinuous(deps, {
    listenerFactory: /** Return the observable listener. */ () => listener,
    signal: controller.signal,
  });
  await entered.promise;
  for (let i = 0; i < 10; i += 1)
    listener.emit('notification', { channel: 'reconciliation_ready', payload: 'ignored' });
  assert.equal(claims, 1);
  release.resolve();
  await running;
  assert.equal(claims, 1);
});

/** Create an explicit promise barrier without requiring newer TypeScript library declarations. */
function barrier(): { promise: Promise<void>; resolve: () => void } {
  let resolve = /** Assigned during promise creation before use. */ (): void => {};
  const promise = new Promise<void>(
    /** Expose resolution to the deterministic test boundary. */ (done) => {
      resolve = done;
    },
  );
  return { promise, resolve };
}

test('listener disconnect resubscribes and rescans while polling remains available', /** Reconnect uses fresh connections and acknowledges LISTEN before waking. */ async () => {
  const deps = dependencies(),
    first = new Listener(),
    second = new Listener(),
    controller = new AbortController();
  let connections = 0,
    scans = 0,
    checks = 0;
  deps.ready =
    /** Listener reconnection must not invalidate successful startup compatibility. */ async () => {
      checks += 1;
    };
  deps.jobs.claim = /** Trigger a disconnect after the initial healthy scan. */ async () => {
    scans += 1;
    if (scans === 1) first.emit('error', new Error('transient'));
    if (connections === 2 && second.calls.includes('LISTEN reconciliation_ready'))
      controller.abort();
    return { kind: 'none' };
  };
  await runContinuous(deps, {
    listenerFactory: /** A replacement owns its own event handlers. */ () =>
      ++connections === 1 ? first : second,
    signal: controller.signal,
    reconnectDelayMs: 1,
    scanIntervalMs: 100,
  });
  assert.equal(connections, 2);
  assert.ok(scans >= 2);
  assert.equal(checks, 1);
  assert.equal(second.calls.at(-1), 'end');
});

test('temporary PostgreSQL claim outage is retried by polling', /** A disconnected query pool must not terminate continuous operation. */ async () => {
  const deps = dependencies(),
    listener = new Listener(),
    controller = new AbortController();
  let scans = 0,
    checks = 0;
  deps.ready =
    /** Later operational failures must not restart full schema validation. */ async () => {
      checks += 1;
    };
  deps.jobs.claim = /** The first failure cannot establish an empty durable queue. */ async () => {
    scans += 1;
    if (scans === 1) throw new Error('temporary connection failure');
    controller.abort();
    return { kind: 'none' };
  };
  await runContinuous(deps, {
    listenerFactory: /** Listener remains healthy independently of the pool. */ () => listener,
    signal: controller.signal,
    scanIntervalMs: 5,
  });
  assert.equal(scans, 2);
  assert.equal(checks, 1);
});

test(
  'startup readiness retries transient failures before claiming work',
  { timeout: 2000 },
  /** Failed startup probes must not consume a job attempt. */ async () => {
    const deps = dependencies(),
      listener = new Listener(),
      controller = new AbortController();
    let checks = 0,
      claims = 0;
    deps.ready =
      /** Model an unavailable dependency becoming compatible on the third poll. */ async () => {
        checks += 1;
        assert.equal(claims, 0);
        if (checks < 3) throw new Error('temporary connection failure');
      };
    deps.jobs.claim =
      /** Begin queue access only after successful startup validation. */ async () => {
        claims += 1;
        assert.equal(checks, 3);
        controller.abort();
        return { kind: 'none' };
      };
    await runContinuous(deps, {
      listenerFactory: /** Return the connected notification listener. */ () => listener,
      signal: controller.signal,
      scanIntervalMs: 5,
    });
    assert.equal(claims, 1);
    assert.equal(checks, 3);
  },
);

test(
  'notifications after validation scan the queue without repeating compatibility checks',
  { timeout: 2000 },
  /** Notification wakeups discover work using the established startup state. */ async () => {
    const deps = dependencies(),
      listener = new Listener(),
      controller = new AbortController();
    const firstScan = barrier();
    let checks = 0,
      claims = 0;
    deps.ready = /** Count full compatibility validation calls. */ async () => {
      checks += 1;
    };
    deps.jobs.claim =
      /** Observe one startup scan and one notification-driven scan. */ async () => {
        claims += 1;
        if (claims === 1) firstScan.resolve();
        else controller.abort();
        return { kind: 'none' };
      };
    const running = runContinuous(deps, {
      listenerFactory: /** Provide the listener whose notification is emitted below. */ () =>
        listener,
      signal: controller.signal,
      scanIntervalMs: 10000,
    });
    await firstScan.promise;
    listener.emit('notification', { channel: 'reconciliation_ready' });
    await running;
    assert.equal(claims, 2);
    assert.equal(checks, 1);
  },
);

test(
  'shutdown during startup validation prevents claims after the probe completes',
  { timeout: 2000 },
  /** An in-flight read may finish after SIGTERM but cannot authorize new work. */ async () => {
    const deps = dependencies(),
      listener = new Listener(),
      controller = new AbortController();
    const entered = barrier(),
      release = barrier();
    let claims = 0,
      stopping = 0;
    deps.ready = /** Hold the startup probe across the shutdown signal. */ async () => {
      entered.resolve();
      await release.promise;
    };
    deps.jobs.claim = /** Record any incorrect claim after the shutdown boundary. */ async () => {
      claims += 1;
      return { kind: 'none' };
    };
    const running = runContinuous(deps, {
      listenerFactory: /** Return the owned lifecycle surface. */ () => listener,
      signal: controller.signal,
      onStopping: /** Observe the process shutdown watchdog being armed. */ () => {
        stopping += 1;
      },
    });
    await entered.promise;
    controller.abort();
    assert.equal(stopping, 1);
    release.resolve();
    await running;
    assert.equal(claims, 0);
    assert.equal(listener.calls.at(-1), 'end');
  },
);

test('systemic startup validation failure stops before claims', /** Incompatible structures require intervention rather than retrying every queued job. */ async () => {
  const deps = dependencies(),
    listener = new Listener(),
    controller = new AbortController();
  let checks = 0;
  deps.ready = /** Reject the existing schema using a sanitized systemic failure. */ async () => {
    checks += 1;
    throw new SystemicError('SCHEMA_INVALID', 'Incompatible schema.');
  };
  deps.jobs.claim = /** No durable attempt may begin under an incompatible schema. */ async () =>
    assert.fail('Claimed despite incompatible schema');
  await assert.rejects(
    runContinuous(deps, {
      listenerFactory: /** Supply the otherwise healthy listener. */ () => listener,
      signal: controller.signal,
    }),
    SystemicError,
  );
  assert.equal(checks, 1);
  assert.equal(listener.calls.at(-1), 'end');
});

test('an already stopped worker performs no startup probes or claims', /** Shutdown requested before entry must not open new work. */ async () => {
  const deps = dependencies(),
    controller = new AbortController();
  controller.abort();
  deps.ready =
    /** Compatibility is irrelevant once shutdown has already been requested. */ async () =>
      assert.fail('Probed after shutdown');
  deps.jobs.claim = /** A stopped process must leave the durable queue untouched. */ async () =>
    assert.fail('Claimed after shutdown');
  await runContinuous(deps, {
    listenerFactory: /** A stopped process must not open a notification connection. */ () =>
      assert.fail('Connected after shutdown'),
    signal: controller.signal,
  });
});

test('failed initial listener does not disable table scans', /** A missed subscription must not hide already durable work. */ async () => {
  const deps = dependencies(),
    listener = new Listener(),
    controller = new AbortController();
  listener.connect = /** Simulate a transient dedicated-listener outage. */ async () => {
    throw new Error('connection lost');
  };
  let scans = 0;
  deps.jobs.claim =
    /** The independent query pool is still able to inspect the queue. */ async () => {
      scans += 1;
      controller.abort();
      return { kind: 'none' };
    };
  await runContinuous(deps, {
    listenerFactory: /** Supply the unavailable dedicated connection. */ () => listener,
    signal: controller.signal,
  });
  assert.equal(scans, 1);
  assert.equal(listener.calls.at(-1), 'end');
});

test('listener authentication failure stops before claims', /** Authentication is systemic even when reported during dedicated connection setup. */ async () => {
  const deps = dependencies(),
    listener = new Listener(),
    controller = new AbortController();
  listener.connect =
    /** PostgreSQL's password rejection is permanent until configuration changes. */ async () => {
      throw Object.assign(new Error('hidden credential'), { code: '28P01' });
    };
  deps.jobs.claim = /** Systemic startup errors must leave the queue untouched. */ async () =>
    assert.fail('Claimed despite authentication failure');
  await assert.rejects(
    runContinuous(deps, {
      listenerFactory: /** Supply the rejected listener. */ () => listener,
      signal: controller.signal,
    }),
    SystemicError,
  );
});

test('systemic failure starts the executable shutdown bound before closing the listener', /** A stuck driver close must be covered by the same watchdog as a signal shutdown. */ async () => {
  const deps = dependencies(),
    listener = new Listener(),
    controller = new AbortController();
  const order: string[] = [];
  deps.jobs.claim = /** Force a fatal stop after successful subscription. */ async () => {
    throw new SystemicError('SCHEMA_INVALID', 'Incompatible schema.');
  };
  listener.end =
    /** Record the point where a real driver's close could become stuck. */ async () => {
      order.push('end');
    };
  await assert.rejects(
    runContinuous(deps, {
      listenerFactory: /** Return the observed lifecycle surface. */ () => listener,
      signal: controller.signal,
      onStopping: /** Arm the executable's watchdog before cleanup. */ () => {
        order.push('stopping');
      },
    }),
    SystemicError,
  );
  assert.deepEqual(order, ['stopping', 'end']);
});
