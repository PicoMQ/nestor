<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue';

const IMG_ASPECT = 720 / 1084;
const BEAK_FX = 0.5;
const BEAK_FY = 0.19;

const bird = ref<HTMLElement | null>(null);
const active = ref(false);
const mx = ref('50%');
const my = ref('50%');
const faceX = ref('50%');
const faceY = ref('19%');

function placeFace() {
  const el = bird.value;
  if (!el) {
    return;
  }
  const rect = el.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0) {
    return;
  }
  let drawW = rect.width;
  let drawH = rect.width / IMG_ASPECT;
  if (rect.width / rect.height > IMG_ASPECT) {
    drawH = rect.height;
    drawW = rect.height * IMG_ASPECT;
  }
  const drawLeft = rect.width - drawW;
  faceX.value = `${((drawLeft + BEAK_FX * drawW) / rect.width) * 100}%`;
  faceY.value = `${((BEAK_FY * drawH) / rect.height) * 100}%`;
}

function onMove(event: PointerEvent) {
  const el = bird.value;
  if (!el) {
    return;
  }
  const rect = el.getBoundingClientRect();
  const pad = 48;
  const inside =
    event.clientX >= rect.left - pad &&
    event.clientX <= rect.right + pad &&
    event.clientY >= rect.top - pad &&
    event.clientY <= rect.bottom + pad;
  if (!inside) {
    active.value = false;
    return;
  }
  active.value = true;
  mx.value = `${((event.clientX - rect.left) / rect.width) * 100}%`;
  my.value = `${((event.clientY - rect.top) / rect.height) * 100}%`;
}

function hide() {
  active.value = false;
}

onMounted(() => {
  if (window.matchMedia('(max-width: 639px)').matches) {
    return;
  }
  placeFace();
  window.addEventListener('resize', placeFace, { passive: true });
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    return;
  }
  window.addEventListener('pointermove', onMove, { passive: true });
  document.addEventListener('mouseleave', hide);
});

onBeforeUnmount(() => {
  window.removeEventListener('resize', placeFace);
  window.removeEventListener('pointermove', onMove);
  document.removeEventListener('mouseleave', hide);
});
</script>

<template>
  <div ref="bird" class="kakapo-bird" aria-hidden="true" />
  <div
    class="kakapo-bird-reveal kakapo-bird-reveal--face active"
    aria-hidden="true"
    :style="{ '--kakapo-bird-mx': faceX, '--kakapo-bird-my': faceY }"
  />
  <div
    class="kakapo-bird-reveal"
    aria-hidden="true"
    :class="{ active }"
    :style="{ '--kakapo-bird-mx': mx, '--kakapo-bird-my': my }"
  />
</template>
