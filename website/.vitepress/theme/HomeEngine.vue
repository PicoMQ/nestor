<script setup lang="ts">
import { computed, ref } from 'vue';

const features = [
  {
    name: 'Block-level caching',
    body: 'Objects are cached as fixed-size blocks, <code>64 KiB</code> to <code>16 MiB</code> per namespace. A small read in the middle of a large file costs one block from the origin, a full sequential read streams blocks with a bounded window in flight. Both patterns are fast from the same cache.',
  },
  {
    name: 'Hybrid RAM and disk',
    body: 'Hot blocks live in memory, the long tail spills to local NVMe with <code>O_DIRECT</code>. Capacity for each tier is a config value, so a single node fronts terabytes of object storage while the origin sees only misses.',
  },
  {
    name: 'Origin protection',
    body: 'Concurrent readers of a missing block share one request. Slow <code>GET</code>s are hedged once latency crosses an adaptive threshold, failures are retried with backoff, and concurrency to the origin is bounded. The origin never sees a thundering herd.',
  },
  {
    name: 'Strong consistency',
    body: 'Every block is keyed by a content tag derived from the object <code>ETag</code>. When an object changes at the origin, its old blocks become unreachable and age out. Namespaces with immutable data skip revalidation entirely.',
  },
  {
    name: 'S3 endpoint or library',
    body: 'Run the <code>nestor</code> binary and set <code>AWS_ENDPOINT_URL_S3</code>: <code>GET</code> and <code>HEAD</code> are served from cache, <code>PUT</code>, <code>DELETE</code> and <code>LIST</code> are re-signed with SigV4 and forwarded. In Rust, embed the engine behind the <code>object_store</code> trait.',
  },
] as const;

const active = ref(0);
const current = computed(() => features[active.value]);

function select(index: number) {
  active.value = index;
}
</script>

<template>
  <section class="engine kakapo-wrap" aria-label="The architecture">
    <p class="engine__eyebrow">the architecture</p>

    <div class="engine__plate">
      <div class="engine__copy">
        <h3>{{ current.name }}</h3>
        <p class="engine__body" v-html="current.body" />
      </div>

      <div class="engine__tabs" role="tablist" aria-label="Engine features">
        <button
          v-for="(feature, index) in features"
          :key="feature.name"
          class="engine__tab"
          type="button"
          role="tab"
          :class="{ active: index === active }"
          :aria-selected="index === active"
          @click="select(index)"
        >
          <span class="engine__tab-name">{{ feature.name }}</span>
        </button>
      </div>
    </div>
  </section>
</template>

<style scoped>
.engine {
  position: relative;
  z-index: 1;
  margin: 0 auto 5rem;
}

.engine__eyebrow {
  margin: 0 0 1.25rem;
  font-family: var(--kakapo-font-serif);
  font-size: 1.5rem;
  font-weight: 400;
  letter-spacing: -0.005em;
  color: var(--kakapo-ink-1);
}

.engine__plate {
  border: 1px solid var(--kakapo-ink-6);
  background: var(--kakapo-surface-0);
}

.engine__copy {
  display: flex;
  flex-direction: column;
  gap: 0.9rem;
  min-width: 0;
  padding: 2rem 1.5rem 1.75rem;
}

.engine__copy h3 {
  margin: 0;
  font-family: var(--kakapo-font-serif);
  font-size: 1.6rem;
  font-weight: 400;
  line-height: 1.15;
  letter-spacing: 0;
  color: var(--kakapo-ink-1);
}

.engine__body {
  margin: 0;
  max-width: 36rem;
  min-height: 4.8rem;
  font-size: 1rem;
  line-height: 1.62;
  color: var(--kakapo-ink-3);
}

.engine__body :deep(code) {
  padding: 0.1em 0.35em;
  font-family: var(--vp-font-family-mono);
  font-size: 0.85em;
  color: var(--kakapo-ink-1);
  background: var(--kakapo-inline-code-bg);
}

.engine__tabs {
  display: grid;
  grid-template-columns: 1fr;
  gap: 1px;
  border-top: 1px solid var(--kakapo-ink-6);
  background: var(--kakapo-ink-6);
}

.engine__tab {
  display: grid;
  align-content: center;
  min-width: 0;
  padding: 0.95rem 1rem;
  border: 0;
  border-radius: 0;
  background: var(--kakapo-surface-0);
  color: var(--kakapo-ink-3);
  text-align: left;
  cursor: pointer;
}

.engine__tab:hover {
  background: var(--kakapo-surface-1);
  color: var(--kakapo-ink-1);
}

.engine__tab.active {
  background: var(--kakapo-ink-1);
  color: var(--kakapo-surface-0);
}

.engine__tab-name {
  min-width: 0;
  font-family: var(--vp-font-family-mono);
  font-size: 0.8125rem;
  font-weight: 400;
  line-height: 1.25;
}

@media (min-width: 860px) {
  .engine__copy {
    padding: 2.25rem 2rem;
  }

  .engine__tabs {
    grid-template-columns: repeat(5, minmax(0, 1fr));
  }
}
</style>
