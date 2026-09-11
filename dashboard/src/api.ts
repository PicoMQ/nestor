export interface CacheInfo {
  memoryUsed: number
  memoryCap: number
  diskCap: number | null
  metaUsed: number
  metaCap: number
  inflight: number
}

export interface Counters {
  hits: number
  misses: number
  joined: number
  stale: number
  originRequests: number
  originBytes: number
  originErrors: number
  originRetries: number
  originTimeouts: number
  hedges: number
  hedgeWins: number
  readaheadBlocks: number
  metaHeads: number
  bytesServed: number
}

export interface StatusInfo {
  listen: string
  metrics: string | null
  origin: string
  cache: CacheInfo
  totals: Counters
  hitRatio: number
  amplification: number
}

export interface Consistency {
  mode: string
  ttlSeconds?: number
}

export interface NamespaceInfo {
  name: string
  id: number
  blockSize: number
  fetchWindow: number
  readWindow: number
  consistency: Consistency
  readahead: number
  counters: Counters
  hitRatio: number
}

export interface Readiness {
  ready: boolean
  serving: boolean
  s3: string
  metrics: string | null
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(path, { headers: { Accept: 'application/json' } })
  const body = (await res.json()) as T
  if (!res.ok && path !== '/ready') {
    throw new Error(`GET ${path} failed: ${res.status}`)
  }
  return body
}

export const fetchStatus = () => get<StatusInfo>('/admin/status')
export const fetchNamespaces = () => get<{ namespaces: NamespaceInfo[] }>('/admin/namespaces')
export const fetchReady = () => get<Readiness>('/ready')
