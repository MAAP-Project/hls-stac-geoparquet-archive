<template>
  <div class="slidev-layout title-layout">
    <div class="title-logo"><slot name="logo" /></div>
    <div class="title-main">
      <div class="title-heading"><slot /></div>
      <div class="title-subtitle"><slot name="subtitle" /></div>
    </div>
    <div class="title-image"><slot name="image" /></div>
    <div class="title-tint" aria-hidden="true" />
    <div class="title-event"><slot name="event" /></div>
  </div>
</template>

<style scoped>
/* Adapt the ds-slidev-template title composition without importing its theme. */
.title-layout {
  position: relative;
  padding: 0;
  overflow: hidden;
  background: var(--maap-surface);
}

.title-logo {
  position: absolute;
  top: 2.25rem;
  left: 3.5rem;
}

.title-logo :deep(img) {
  display: block;
  height: 2rem;
  width: auto;
  padding: 0.3rem 0.5rem;
  border-radius: 0.2rem;
  background: #fff;
}

.title-main {
  position: absolute;
  top: 7.25rem;
  right: 3.5rem;
  bottom: calc(100% / 3 + 3.25rem);
  left: 3.5rem;
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 3rem;
  align-items: end;
}

.title-heading :deep(h1) {
  margin: 0;
  font-size: 3.5rem;
  line-height: 1.06;
}

.title-subtitle :deep(p) {
  margin: 0 0 0.15rem;
  color: var(--maap-muted);
  font-size: 1.65rem;
  line-height: 1.35;
}

.title-image {
  position: absolute;
  right: 0;
  bottom: 0;
  left: 0;
  height: calc(100% / 3);
  overflow: hidden;
}

.title-image :deep(img) {
  width: 100%;
  height: 100%;
  object-fit: cover;
  object-position: center 55%;
}

.title-image :deep(a) {
  position: absolute;
  bottom: 0.65rem;
  left: 1rem;
  padding: 0.2rem 0.4rem;
  color: #fff;
  background: #0b112099;
  font-size: 0.65rem;
}

.title-tint,
.title-event {
  position: absolute;
  right: 2%;
  bottom: 2%;
  width: 50%;
  height: 40%;
  border-radius: 0.35rem;
}

.title-tint {
  background: var(--maap-blue);
  mix-blend-mode: multiply;
}

.title-event {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  justify-content: flex-end;
  padding: 1.3rem 1.6rem;
  color: #fff;
  text-align: right;
}

.title-event :deep(p) { margin: 0; color: #fff; }
.title-event :deep(.event-name) { font-size: 1.9rem; line-height: 1.2; }
.title-event :deep(.event-date) {
  margin: 0.75rem 0 0.5rem;
  font-family: var(--slidev-font-family-mono);
  font-size: 0.95rem;
}
.title-event :deep(.event-presenter) { font-size: 1.05rem; line-height: 1.4; }
.title-event :deep(.event-affiliation) { color: var(--maap-sky-light); font-size: 0.85rem; }
</style>
