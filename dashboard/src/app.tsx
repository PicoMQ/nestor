import { useEffect, useState } from 'preact/hooks'
import {
  fetchNamespaces,
  fetchReady,
  fetchStatus,
  type NamespaceInfo,
  type Readiness,
  type StatusInfo,
} from './api'

const POLL_INTERVAL_MS = 2000

function Pill({ state, label }: { state: 'ok' | 'warn' | 'err'; label: string }) {
  return (
    <span class={`ss-pill ${state}`}>
      <span class="dot" />
      {label}
    </span>
  )
}

function Stat({ label, value, small }: { label: string; value: unknown; small?: boolean }) {
  return (
    <div class="ss-card">
      <div class="label">{label}</div>
      <div class={small ? 'value small' : 'value'}>{String(value ?? '—')}</div>
    </div>
  )
}

function bytes(n: number | null | undefined): string {
  if (n == null) {
    return '—'
  }
  const gib = 1024 * 1024 * 1024
  const mib = 1024 * 1024
  if (n >= gib) {
    return `${(n / gib).toFixed(1)} GiB`
  }
  if (n >= mib) {
    return `${(n / mib).toFixed(1)} MiB`
  }
  if (n >= 1024) {
    return `${(n / 1024).toFixed(1)} KiB`
  }
  return `${n} B`
}

function fill(used: number, cap: number): string {
  if (cap === 0) {
    return bytes(used)
  }
  return `${bytes(used)} / ${bytes(cap)}`
}

function percent(n: number | undefined): string {
  if (n == null) {
    return '—'
  }
  return `${(n * 100).toFixed(1)}%`
}

function blockSize(n: number): string {
  return bytes(n)
}

function consistency(ns: NamespaceInfo): string {
  if (ns.consistency.mode === 'etag' && ns.consistency.ttlSeconds != null) {
    return `etag ${ns.consistency.ttlSeconds}s`
  }
  return ns.consistency.mode
}

export function App() {
  const [status, setStatus] = useState<StatusInfo | null>(null)
  const [namespaces, setNamespaces] = useState<NamespaceInfo[]>([])
  const [ready, setReady] = useState<Readiness | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null)

  useEffect(() => {
    let alive = true

    async function poll() {
      try {
        const [s, n, r] = await Promise.all([fetchStatus(), fetchNamespaces(), fetchReady()])
        if (!alive) {
          return
        }
        setStatus(s)
        setNamespaces(n.namespaces)
        setReady(r)
        setError(null)
        setUpdatedAt(new Date())
      } catch (e) {
        if (!alive) {
          return
        }
        setError(e instanceof Error ? e.message : String(e))
      }
    }

    poll()
    const timer = setInterval(poll, POLL_INTERVAL_MS)
    return () => {
      alive = false
      clearInterval(timer)
    }
  }, [])

  const readyState = ready?.ready ? 'ok' : ready ? 'warn' : 'err'
  const readyLabel = ready?.ready ? 'ready' : ready ? 'not ready' : 'unknown'
  const cache = status?.cache

  return (
    <div class="ss-app">
      <header class="ss-topbar">
        <span class="name">Nestor</span>
        <span class="tag">admin</span>
        <span class="spacer" />
        {error ? <Pill state="err" label="unreachable" /> : <Pill state={readyState} label={readyLabel} />}
      </header>

      <div class="ss-grid">
        <Stat
          label="RAM"
          value={cache ? fill(cache.memoryUsed, cache.memoryCap) : undefined}
          small
        />
        <Stat label="Disk" value={cache ? bytes(cache.diskCap) : undefined} small />
        <Stat
          label="Meta LRU"
          value={cache ? `${cache.metaUsed} / ${cache.metaCap}` : undefined}
          small
        />
        <Stat label="Hit ratio" value={percent(status?.hitRatio)} />
        <Stat label="Origin errors" value={status?.totals.originErrors} />
        <Stat label="Inflight" value={cache?.inflight} />
      </div>

      <section class="ss-section">
        <h2>This node</h2>
        <table>
          <tbody>
            <tr>
              <th>S3 listen</th>
              <td class="mono">{status?.listen ?? '—'}</td>
            </tr>
            <tr>
              <th>Origin</th>
              <td class="mono">{status?.origin ?? '—'}</td>
            </tr>
            <tr>
              <th>Metrics</th>
              <td class="mono">{status?.metrics ?? '—'}</td>
            </tr>
            <tr>
              <th>Amplification</th>
              <td class="mono">{status ? status.amplification.toFixed(2) : '—'}</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="ss-section">
        <h2>Namespaces</h2>
        {namespaces.length === 0 ? (
          <div class="ss-empty">No namespaces</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Namespace</th>
                <th>Consistency</th>
                <th>Block size</th>
                <th>Hits</th>
                <th>Misses</th>
                <th>Hit ratio</th>
                <th>Origin errors</th>
              </tr>
            </thead>
            <tbody>
              {namespaces.map((ns) => (
                <tr key={ns.id}>
                  <td class="mono">{ns.name}</td>
                  <td class="mono">{consistency(ns)}</td>
                  <td class="mono">{blockSize(ns.blockSize)}</td>
                  <td class="mono">{ns.counters.hits}</td>
                  <td class="mono">{ns.counters.misses}</td>
                  <td class="mono">{percent(ns.hitRatio)}</td>
                  <td class="mono">{ns.counters.originErrors}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <footer class="ss-footer">
        {error
          ? `Last error: ${error}`
          : updatedAt
            ? `Updated ${updatedAt.toLocaleTimeString()} · polling every ${POLL_INTERVAL_MS / 1000}s`
            : 'Loading…'}
      </footer>
    </div>
  )
}
