# LiveValidator — Comprehensive Testing & Release Strategy

**Author:** Ruslan Dautkhanov (Databricks)
**Date:** 2026-04-22
**For:** Dan Z (LiveValidator maintainer, PS)
**Scope:** Testing strategy, CI/CD, release engineering, and observability for LiveValidator v0.1.0 through v1.0

---

## Executive Summary

LiveValidator is a React + FastAPI tool running as a **Databricks Apps application**, executing three-tier data validations (schema, row-count, row-level) between **Databricks, Netezza, Teradata, SQL Server, MySQL, Postgres, Snowflake, and custom JDBC sources** (including Oracle). It uses **LakeBase (managed Postgres)** as its control plane and UC Delta for results. A long-running **Job Sentinel** worker (a separate Databricks job) polls a Postgres-backed queue and launches validation runs on Spark. It is currently deployed at NVIDIA and in demand across 5–10 Databricks Professional Services engagements. A single regression can impact every downstream customer simultaneously.

> **Reader note.** This document was originally drafted with the assumption that the backend targeted Oracle/Delta/MySQL/Teradata. A subsequent review of the actual codebase at [`databricks-solutions/livevalidator`](https://github.com/databricks-solutions/livevalidator) revealed a broader system list (seven+ engines), a LakeBase-Postgres control plane, Databricks-notebook-style jobs, user-supplied Python code exec in type transformations, and zero frontend tests today. **See §11 — "Codebase Reality Check & Revised Plan" — at the end for the delta between this document's original draft and the as-built repo.** Earlier sections are left intact for context; §11 is authoritative where they conflict.

Dan's stated concerns:

1. **"If I introduce a bug it can potentially impact many PS projects."** — blast-radius anxiety.
2. **"These are front-end tests with React using Playwright etc. They need to mock the customer going through and clicking different buttons. There are always edge-cases I don't think of."** — E2E gap on the imagination axis.
3. **"They are jobs that run on Oracle/Delta/MySQL/Teradata etc. I don't have a test instance of those source systems."** — database test-instance gap.

This document addresses all three with concrete tooling, code patterns, and a sequenced implementation plan. The headline recommendations — prioritized by anxiety-reduction per hour invested — are:

| # | Action | Why | Effort |
|---|--------|-----|--------|
| 1 | **Ephemeral Databricks Apps per PR** (DABs `pr` target, deploy on PR open, destroy on close) | Every change demoed on a live app before merge; single biggest mitigation for "bug hits NVIDIA" | ~1 day |
| 2 | **Three-layer feature flag system** (env var → UC table → Unleash) | Turn regressions from deploys into config flips; kill-switch any new validator | ~4 hours |
| 3 | **Testcontainers for MySQL + Oracle Free + Delta-Spark, per-PR** | Replace "no test instance" with an ephemeral real instance on every PR | ~1–2 days |
| 4 | **Property-based testing with fast-check and Hypothesis** | Directly attacks "edge cases I don't think of" by generating inputs you'd never write | ~1 day |
| 5 | **Differential tests across dialects** (same input, 4 backends, diff the results) | Turns multi-DB architecture into a built-in oracle | ~1 day |
| 6 | **Pre-minted Playwright `storageState`** for Databricks SSO | Eliminates the largest class of E2E flake | ~2 hours |
| 7 | **Self-hosted Sentry + managed OTel in UC** with PII scrubbing | Learn about NVIDIA crashes before NVIDIA tells you | ~3 hours |
| 8 | **release-please + Conventional Commits** | Removes human error from version/changelog | ~2 hours |
| 9 | **48-hour internal dogfood soak** before any stable tag | Catches ~60% of regressions for almost zero engineering cost | ~2 hours |
| 10 | **Playwright MCP + weekly AI exploratory sweep** | Converts "edge cases I don't think of" into a process | ~4 hours |

The rest of the document expands each of these and surrounds them with supporting practice.

---

## Context: What LiveValidator Is and Why It Matters

LiveValidator is a **Databricks Apps** application: a FastAPI backend + React frontend packaged as a container that runs behind the Databricks workspace's reverse proxy, which enforces SSO and injects identity headers on every request (`X-Forwarded-User`, `X-Forwarded-Email`, `X-Forwarded-Access-Token`). Each deployment gets its own service principal. The app is installed per-workspace, meaning NVIDIA runs their own copy, each PS engagement runs their own copy, and a bug in the shared source tree hits every one of them on the next upgrade.

The backend dispatches validation jobs that connect to customer data sources (Oracle, Delta Lake on Databricks, MySQL, Teradata) via JDBC/ODBC or Spark, then summarizes results in the UI. The frontend is a React SPA with Playwright-based E2E tests.

Three testing surfaces exist:

1. **Frontend (React)** — the interactive UI Dan's users click through. Edge cases come from real user behavior and unexpected states.
2. **Backend (validator jobs)** — SQL generation, driver interop, dialect-specific syntax. Edge cases come from source-system quirks across four database families.
3. **Integration (Databricks Apps runtime)** — auth, proxy headers, UC grants, service principals, OAuth scopes. Edge cases come from per-workspace configuration drift.

A testing strategy that addresses only one of these is insufficient. This document covers all three, plus CI/CD and release engineering that ties them together.

---

## Mental Model: Test Pyramid + Safety Nets

```
                      /\      e2e (Playwright)        few, slow — golden paths only
                     /  \     integration (real DBs)  per-PR, contract + schema
                    /----\    component (RTL/Vitest)  fast, where edge cases live
                   /------\   unit (pure fns, hooks)  every commit, <10s
                  /________\  static (ts, eslint, mypy, ruff)  pre-commit
```

80% of bug-escape risk disappears once unit + component tests cover business logic. Playwright guards the 5–10 flows that would be embarrassing at NVIDIA. Property-based tests catch the edges none of these layers enumerate. Differential tests catch dialect divergence. Feature flags and canary rollout catch the bugs every layer missed.

Additionally, for a tool with high blast radius:

- **Observability is testing in production.** Every customer workspace is a node in your test matrix; OTel + Sentry turn that into a feedback loop.
- **Release engineering is testing at the release boundary.** release-please, feature flags, cohort rollout, one-command rollback.
- **Culture is testing over time.** Every escaped bug becomes a regression test in the tier where it should have been caught.

---

## 1. Frontend Testing Stack (Non-Playwright Layers)

The React testing ecosystem has consolidated between 2024 and 2026: **Vitest** overtook Jest, **MSW 2.x** became the canonical API-mocking layer, **Storybook 9** folded interaction, a11y, and coverage into a unified "Storybook Test" workflow, and **Chromatic** established itself as the path-of-least-resistance for visual regression.

### 1.1 Vitest vs Jest in 2026

Stable: **Vitest 4.1.4** (March 2026). Jest is at 30.x. On a ~50,000-test enterprise monorepo, Vitest cold-starts in ~38s vs Jest's ~214s (5.6×) and watch-mode re-runs drop from ~8.4s to ~0.3s (~28×). For PS engineers iterating locally while customizing validators, watch-mode responsiveness determines whether good testing habits stick.

Vitest's edge:

1. **Shared Vite pipeline** — no separate Babel/ts-jest stack.
2. **Native ESM** — no `--experimental-vm-modules` flag.
3. **Browser Mode** — run tests in real Chromium via Playwright, bypassing jsdom limitations (IntersectionObserver, ResizeObserver, CSS containment).
4. **Workspaces** — monorepo-aware.

Migration: `npx jest-to-vitest` codemod plus minor API differences (`vi.importActual` vs `jest.requireActual`). Realistic migration of a 400-test suite: 1–3 focused days.

```typescript
// vitest.config.ts
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    globals: true,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html', 'lcov'],
      thresholds: { lines: 80, functions: 80, branches: 75, statements: 80 },
    },
  },
})
```

Docs: https://vitest.dev/ · https://jestjs.io/

### 1.2 React Testing Library Best Practices

Current: `@testing-library/react` 16.x, `user-event` 14.x. Every `userEvent` method in v14 is async — always `await`.

```tsx
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect } from 'vitest'
import { ValidationRuleEditor } from './ValidationRuleEditor'

describe('ValidationRuleEditor', () => {
  it('saves a new rule when the form is submitted', async () => {
    const user = userEvent.setup()
    const onSave = vi.fn()
    render(<ValidationRuleEditor onSave={onSave} />)

    await user.type(screen.getByLabelText(/rule name/i), 'non_null_customer_id')
    await user.selectOptions(screen.getByLabelText(/severity/i), 'error')
    await user.click(screen.getByRole('button', { name: /save rule/i }))

    expect(onSave).toHaveBeenCalledWith({
      name: 'non_null_customer_id',
      severity: 'error',
    })
  })
})
```

**Query priority** — use in this order:

1. `getByRole` (with `name`) — matches how assistive tech reads the page.
2. `getByLabelText` — form inputs.
3. `getByPlaceholderText`, `getByText`.
4. `getByDisplayValue`, `getByAltText`, `getByTitle`.
5. `getByTestId` — last resort.

Frequent reach for `data-testid` is a signal your markup isn't accessible. Fix the markup.

**Anti-patterns to avoid:**

- Snapshot overuse — snapshots of entire trees rot instantly and nobody reads the diffs.
- Testing implementation details — a test that breaks when you rename a state variable is measuring the wrong thing.
- Querying by class name or DOM structure — leaks markup choices into tests.
- Mocking React hooks — usually means the component is too coupled.

### 1.3 Mock Service Worker (MSW) v2

**Why MSW beats `fetch`-mocks:** mocks at the service-worker / `undici` layer. Same mocks run in Vitest, Playwright, and Storybook. Stops you from writing three mock layers that drift.

**Current: MSW 2.13.4** (Node 18+).

```typescript
// src/mocks/handlers.ts
import { http, HttpResponse, delay } from 'msw'

export const handlers = [
  http.get('/api/validations/:runId', async ({ params }) => {
    await delay(150)
    return HttpResponse.json({
      runId: params.runId,
      status: 'completed',
      rules: [
        { id: 'r1', name: 'non_null_id', passed: 98, failed: 2 },
        { id: 'r2', name: 'unique_pk', passed: 100, failed: 0 },
      ],
    })
  }),

  http.post('/api/validations', async ({ request }) => {
    const body = (await request.json()) as { tableName: string }
    if (!body.tableName) {
      return HttpResponse.json({ error: 'tableName required' }, { status: 400 })
    }
    return HttpResponse.json({ runId: 'run_123' }, { status: 201 })
  }),

  http.get('/api/validations/:runId/status', () =>
    HttpResponse.error(),
  ),
]
```

```typescript
// src/test/setup.ts
import '@testing-library/jest-dom/vitest'
import { afterAll, afterEach, beforeAll } from 'vitest'
import { setupServer } from 'msw/node'
import { handlers } from '../mocks/handlers'

export const server = setupServer(...handlers)

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }))
afterEach(() => server.resetHandlers())
afterAll(() => server.close())
```

`onUnhandledRequest: 'error'` is load-bearing: it fails the test if any code makes a real network call, forcing you to acknowledge every endpoint.

Docs: https://mswjs.io/

### 1.4 Storybook 9

Storybook 9.1.x collapsed interaction, accessibility, and coverage into a unified "Storybook Test" workflow. Install footprint roughly halved vs Storybook 8. Play functions = interaction tests embedded in a story:

```tsx
// ValidationRuleEditor.stories.tsx
import type { Meta, StoryObj } from '@storybook/react'
import { expect, userEvent, within, fn } from '@storybook/test'
import { ValidationRuleEditor } from './ValidationRuleEditor'

const meta: Meta<typeof ValidationRuleEditor> = {
  component: ValidationRuleEditor,
  args: { onSave: fn() },
}
export default meta

export const SubmitsValidInput: StoryObj<typeof ValidationRuleEditor> = {
  play: async ({ canvasElement, args }) => {
    const canvas = within(canvasElement)
    await userEvent.type(canvas.getByLabelText(/rule name/i), 'unique_pk')
    await userEvent.click(canvas.getByRole('button', { name: /save/i }))
    await expect(args.onSave).toHaveBeenCalledOnce()
  },
}
```

The test-runner turns every story into an executable test; the a11y add-on runs axe-core against each story in a real browser (so color-contrast bugs — invisible in jsdom — are caught here).

Docs: https://storybook.js.org/ · https://storybook.js.org/docs/writing-tests/test-runner

### 1.5 Visual Regression Options

| Tool | Model | Strength | Weakness | 2026 pricing |
|---|---|---|---|---|
| **Chromatic** | Story-based cloud | Storybook-native, TurboSnap skips unchanged, review UI, component-level diffs | Requires Storybook | Free 5,000 snapshots/mo; paid from **$149/mo** |
| **Percy** | Page/snapshot | Any runner integration, good full-page | Weaker Storybook UX | Free 5,000 screenshots/mo; paid ~$549/mo |
| **Applitools** | AI visual grid | "Visual AI" flags only meaningful diffs | Opaque sales-led pricing, expensive | Small-team ~$1.2k–3.6k/yr, enterprise $25k+/yr |
| **Playwright `toHaveScreenshot`** | Built-in | Free, lives in repo | DIY baseline management | $0 |

**Recommendation for LiveValidator:** Chromatic at $149/mo with TurboSnap. The review UI lets a PS engineer accept an intentional change in 3 clicks. Visual regression is the single highest-leverage tool for preventing "looks wrong in front of the NVIDIA PS lead" incidents.

Docs: https://www.chromatic.com/ · https://playwright.dev/docs/test-snapshots

### 1.6 Accessibility Testing

WCAG 2.2 (October 2023) is the current normative standard. **Automated tools catch 30–50% of WCAG issues** — plan for manual testing with real assistive tech for the rest.

Tooling layers:

- `vitest-axe` / `jest-axe` — RTL-integrated axe-core matcher. Note: color-contrast doesn't work in jsdom.
- `@storybook/addon-a11y` — axe-core per story in a real browser (contrast *does* work here).
- `@axe-core/playwright` — full-page axe-core in E2E tests (covered in §2).
- `@axe-core/react` — dev-time runtime checks, not CI.

```typescript
import { render } from '@testing-library/react'
import { axe, toHaveNoViolations } from 'vitest-axe'
import { expect, it } from 'vitest'
import { ValidationResultsTable } from './ValidationResultsTable'

expect.extend({ toHaveNoViolations })

it('has no accessibility violations', async () => {
  const { container } = render(<ValidationResultsTable rows={sampleRows} />)
  const results = await axe(container)
  expect(results).toHaveNoViolations()
})
```

Make a11y violations fail CI, not warn.

### Recommendations for LiveValidator

1. **Migrate Jest → Vitest this sprint.** 1–3 days, material speed win, deletes Babel/ts-jest config.
2. **Adopt MSW 2.x as the single source of API mocks** used by Vitest, Storybook, and Playwright. Set `onUnhandledRequest: 'error'`.
3. **Stand up Storybook 9** for every validation UI primitive (empty, loading, error, 100k failures). Use the Test runner in CI.
4. **Chromatic at $149/mo with TurboSnap.** Turn on component-level visual regression.
5. **Enforce `vitest-axe` at component level AND `@storybook/addon-a11y` at story level.** Both fail CI.
6. **Publish `src/mocks/` and Storybook as the onboarding artifact** for new PS engagements — it becomes the API contract and UX spec.
7. **Coverage gates by directory** — `src/components/**` and `src/validators/**` at 85%+, shared utilities at 70%.
8. **Define the test-layer contract in the repo** — unit for logic, component+MSW for UI behavior, Storybook+Chromatic for visual/a11y, Playwright only for full journeys.

---

## 2. Playwright Deep Dive

### 2.1 What Is Playwright?

Playwright is Microsoft's browser-automation testing framework (2020). It drives **real Chromium, Firefox, and WebKit** via a single API. By April 2026, it holds ~45% of E2E market share vs Selenium (~22%) and Cypress (~14%), is ~42% faster than Selenium in public benchmarks, and produces ~67% fewer flaky tests than Cypress. **Stable: Playwright 1.59.1.**

**Why Playwright won:**

- **Real WebKit** — Cypress can't drive Safari; NVIDIA PS engineers on macOS Safari would be uncovered.
- **Multi-tab / multi-origin** — Cypress's same-origin restriction breaks the moment LiveValidator authenticates against a Databricks workspace.
- **Trace Viewer** — time-travel debugger recording every action, network request, console log, and DOM snapshot. Selenium and WebdriverIO have no equivalent.
- **First-class TypeScript + MCP/AI integration** — Playwright 1.59 ships official Planner/Generator/Healer AI agents that drive a real browser.

### 2.2 Codegen Workflow

`npx playwright codegen https://livevalidator.example.com` launches Chromium plus an inspector. Every click and type becomes a `getByRole(...)` / `getByLabel(...)` line. **Never commit raw codegen output** — the consensus workflow is **record → harden → extract**:

```typescript
// BEFORE: raw codegen output
test('recorded happy path', async ({ page }) => {
  await page.goto('https://livevalidator.example.com/');
  await page.getByPlaceholder('Email').fill('ps-engineer@nvidia.com');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await page.waitForTimeout(2000);                       // smell
  await page.locator('#root > div > div:nth-child(3)')   // brittle
      .getByText('Upload ruleset').click();
});

// AFTER: fixture-driven, web-first assertions
test('ruleset upload and validation', async ({ authedApp, rulesetFixture }) => {
  await authedApp.gotoDashboard();
  await authedApp.uploadRuleset(rulesetFixture.path);
  await expect(authedApp.validationResult).toHaveText(/Passed: \d+ rules/);
  await expect(authedApp.errorToast).toBeHidden();
});
```

Hardening checklist: replace `waitForTimeout` with web-first assertions; promote brittle CSS to `getByRole`/`getByTestId`; move login into a fixture; make each test verify one behavior.

### 2.3 Trace Viewer

```typescript
use: {
  trace: 'on-first-retry',  // recommended default
  // 'on-all-retries' records every retry attempt when any fails
}
```

CI artifact upload:

```yaml
- uses: actions/upload-artifact@v4
  if: always()
  with:
    name: playwright-traces-${{ matrix.shard }}
    path: test-results/
    retention-days: 7
```

When a customer reports a bug, the failing trace often makes diagnosis a 5-minute job.

### 2.4 Sharding and Parallelism

Two axes: **workers** (intra-machine, `workers: 4`) and **shards** (inter-machine, `--shard=2/4`). Always pair sharding with `fullyParallel: true` or Playwright assigns whole files to workers, producing lopsided shards.

```typescript
// playwright.config.ts
import { defineConfig } from '@playwright/test';

export default defineConfig({
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 4 : undefined,
  reporter: process.env.CI
    ? [['blob'], ['github']]
    : [['html', { open: 'never' }], ['list']],
  use: {
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
});
```

```yaml
# .github/workflows/e2e.yml
jobs:
  e2e:
    strategy:
      fail-fast: false
      matrix:
        shard: [1, 2, 3, 4]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: '22' }
      - run: npm ci
      - run: npx playwright install --with-deps
      - run: npx playwright test --shard=${{ matrix.shard }}/4
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: blob-report-${{ matrix.shard }}
          path: blob-report
  merge:
    needs: e2e
    if: always()
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v4
        with: { path: all-blob-reports, pattern: blob-report-* }
      - run: npx playwright merge-reports --reporter html ./all-blob-reports
      - uses: actions/upload-artifact@v4
        with: { name: html-report, path: playwright-report }
```

Four shards take a 12-minute E2E suite to ~3 minutes wall-clock.

### 2.5 Visual Regression with `toHaveScreenshot`

```typescript
test('dashboard renders after ruleset load', async ({ authedApp }) => {
  await authedApp.gotoDashboard();
  await expect(authedApp.page).toHaveScreenshot('dashboard-loaded.png', {
    fullPage: true,
    mask: [
      authedApp.page.getByTestId('last-updated-timestamp'),
      authedApp.page.getByTestId('user-avatar'),
      authedApp.page.locator('[data-testid="session-id"]'),
    ],
    maxDiffPixelRatio: 0.01,
    animations: 'disabled',
  });
});
```

**Cross-OS gotchas:** font rendering differs across macOS/Linux/Windows. Run visual tests inside the Playwright container (`mcr.microsoft.com/playwright:v1.59.1-noble`) so baselines are OS-stable.

**When to use vs Chromatic:** use built-in for a handful of critical full-page shots; use Chromatic when you have hundreds of components and a non-engineering design reviewer.

### 2.6 Component Testing with `@playwright/experimental-ct-react`

Still officially labeled "experimental" in 2026 but production-viable. Trade-offs:

| Dimension | Playwright CT | React Testing Library |
|---|---|---|
| Environment | Real Chromium/Firefox/WebKit | jsdom (no real layout) |
| Speed | 2–5 min for 100 tests | <30s for 100 tests |
| Fidelity | High — real CSS, focus, scroll, IntersectionObserver | Low — jsdom skips layout |
| Debugging | Trace Viewer | console.log + stack traces |

**Recommendation for LiveValidator:** keep RTL for pure logic/markup components; reach for Playwright CT for components depending on real browser behavior — Monaco editor for rule YAML, virtualized result tables, drag-to-reorder rulesets, IntersectionObserver-driven lazy loading.

### 2.7 Fixtures and Page Objects

Modern pattern: **fixtures as DI for page objects**.

```typescript
// tests/pages/RulesetEditor.page.ts
import { type Page, type Locator, expect } from '@playwright/test';

export class RulesetEditorPage {
  readonly page: Page;
  readonly uploadInput: Locator;
  readonly validateButton: Locator;
  readonly resultPanel: Locator;
  readonly errorToast: Locator;

  constructor(page: Page) {
    this.page = page;
    this.uploadInput = page.getByTestId('ruleset-file-input');
    this.validateButton = page.getByRole('button', { name: 'Validate' });
    this.resultPanel = page.getByTestId('validation-result');
    this.errorToast = page.getByRole('alert');
  }

  async goto() {
    await this.page.goto('/editor');
    await expect(this.page.getByRole('heading', { name: 'Ruleset Editor' }))
      .toBeVisible();
  }

  async uploadRuleset(path: string) {
    await this.uploadInput.setInputFiles(path);
    await this.validateButton.click();
  }

  async expectValidationSuccess(rulesPassed: number) {
    await expect(this.resultPanel).toContainText(`Passed: ${rulesPassed} rules`);
    await expect(this.errorToast).toBeHidden();
  }
}

// tests/fixtures/app.ts
import { test as base } from '@playwright/test';
import { RulesetEditorPage } from '../pages/RulesetEditor.page';
import { DashboardPage } from '../pages/Dashboard.page';

type AppFixtures = {
  editor: RulesetEditorPage;
  dashboard: DashboardPage;
};

export const test = base.extend<AppFixtures>({
  editor: async ({ page }, use) => { await use(new RulesetEditorPage(page)); },
  dashboard: async ({ page }, use) => { await use(new DashboardPage(page)); },
});
export { expect } from '@playwright/test';
```

### 2.8 Network Stubbing

```typescript
// per-test stubbing
await page.route('**/api/rulesets/validate', async route => {
  const body = route.request().postDataJSON();
  if (body.rules.length === 0) {
    await route.fulfill({ status: 400, json: { error: 'empty_ruleset' } });
  } else {
    await route.continue();
  }
});

// HAR record/replay — the gold standard for real customer traffic
await page.routeFromHAR('fixtures/nvidia-session.har', { update: true });   // capture once
await page.routeFromHAR('fixtures/nvidia-session.har', { update: false });  // replay forever
```

HAR is gold for LiveValidator: capture a real customer session at NVIDIA (with consent + PII scrubbing) and replay it deterministically in CI.

### 2.9 Authentication with `storageState`

Canonical 2026 pattern: a **setup project** writes `storageState` to disk; all other projects read it. Critical for Databricks Apps — see §5 for the concrete `auth.setup.ts`.

```typescript
projects: [
  { name: 'setup', testMatch: /.*\.setup\.ts/ },
  { name: 'chromium',
    use: { ...devices['Desktop Chrome'], storageState: 'playwright/.auth/user.json' },
    dependencies: ['setup'] },
]
```

### 2.10 API Testing with Playwright

`request` is a first-class fixture. Compose API + UI:

```typescript
test('newly created ruleset appears in dashboard', async ({ page, request }) => {
  const created = await request.post('/api/rulesets', {
    data: { name: 'pii-check-v3', rules: [{ type: 'not_null', column: 'ssn' }] },
  });
  const { id } = await created.json();

  await page.goto('/dashboard');
  await expect(page.getByRole('row', { name: /pii-check-v3/ })).toBeVisible();

  await request.delete(`/api/rulesets/${id}`);
});
```

UI tests stop carrying 30-second sign-up/seed preambles. Edge cases (empty ruleset, 10,000-rule ruleset, Unicode rule names) become trivial to exercise because you compose them via API rather than click-typing.

### 2.11 Playwright MCP + AI-Driven Exploratory Testing

**`microsoft/playwright-mcp`** exposes Playwright's browser-control surface to any MCP-capable agent (Claude Code, Cursor, ChatGPT Desktop). Uses the accessibility tree, not screenshots — fast, deterministic, token-efficient.

Three built-in agents ship with Playwright 1.59:

- **Planner** — turns a feature description into a test plan.
- **Generator** — drives a live browser, produces runnable Playwright code.
- **Healer** — on CI failure, inspects the failing trace, proposes a selector or assertion patch.

Use pattern for LiveValidator:

1. **Generator for scaffolding** — give Claude Code the story "a PS engineer uploads a malformed YAML and expects a friendly error". Claude drives a browser and emits a first draft.
2. **Healer for diagnosis only** — post hypotheses in PR comments. Do *not* auto-commit fixes — a "healed" test can be lobotomized.
3. **Exploratory sweeps** — weekly cron: "find 5 surprising behaviors in LiveValidator." Review the generated tests manually.

This is the strongest direct answer to Dan's "edge cases I don't think of" anxiety.

Docs: https://github.com/microsoft/playwright-mcp

### 2.12 Flaky-Test Patterns

| Pitfall | Fix |
|---|---|
| `waitForTimeout(ms)` | Replace with `expect(...).toBeVisible()` / `toHaveText()` |
| CSS selectors `#root > div:nth-child(3)` | Use `getByRole` / `getByTestId` |
| `elementHandle.click()` | Use `Locator` (re-queries on each action) |
| Toast/animation race | `animations: 'disabled'` + assert on final state |
| Shared test data | Fixtures with unique IDs per test |
| Time-of-day dependencies | `page.clock.setFixedTime()` (Playwright 1.45+) |
| Network races | `await page.waitForResponse(url)` before assertion |

### Recommendations for LiveValidator (Playwright)

Eight actions directly addressing Dan's edge-case anxiety:

1. **Adopt Playwright MCP + Claude Code for exploratory generation.** Weekly scheduled sweep: "explore LiveValidator and find 3 flows that aren't covered." Review and commit the useful ones.
2. **Build a HAR-replay corpus from real NVIDIA usage.** With consent + PII scrubbing, capture ~10 real customer sessions and replay them in CI as regression guards.
3. **Require `getByRole`/`getByTestId`-only locators** via lint rule or PR template checklist. CSS selectors become a blocker.
4. **Standard project structure now:** `setup` → `chromium` + `webkit` → `fullyParallel: true` → 4-shard matrix → blob-report merge.
5. **`trace: 'on-first-retry'`** baseline; `on-all-retries` for the flakiest 10%. 7-day retention on branches, 30 on main.
6. **Property-based testing with fast-check** on the ruleset-validation engine (covered in §3). Highest-leverage edge-case-catcher for a validator.
7. **Use Playwright CT selectively** — only where jsdom lies (Monaco, virtualized tables, drag-drop, IntersectionObserver).
8. **Wire Healer into PR comments, not auto-commits.** Human decides whether to accept.

---

## 3. Generative & Advanced Testing — The Answer to "Edge Cases I Don't Think Of"

> *"There are always edge-cases I don't think of." — Dan*

Every other testing layer in this document tests what you can imagine. Generative testing tests what you **can't**. If you skip this section, LiveValidator will keep shipping bugs triggered by inputs no reasonable human would have enumerated — the Oracle identifier with a trailing space, the schema with a column named `SELECT`, the form field with a zero-width joiner, the regex that backtracks on exactly 37 characters.

### 3.1 Property-Based Testing Philosophy

Invented by John Hughes & Koen Claessen (1999) with Haskell's QuickCheck. The idea: stop writing `assert f(3) == 6`. Instead, write `assert forall x, f(x) * 2 == f(x) + f(x)` and let a framework generate thousands of `x` values — including ones you'd never think of.

**Shrinking** is what makes PBT debuggable: when a property fails on `[{"name":"foo","age":-2147483648,"email":"a@b.\u0000c"}]`, the framework simplifies while preserving failure, producing `[{"age":-2147483648}]` as the minimal counterexample.

A 2024 ICSE study found that in production codebases, round-trip properties and differential/model-based properties dominate real usage — and PBT reliably surfaces bugs that survived years of traditional testing.

### 3.2 fast-check (JS/TS)

Current: **fast-check 4.5.3** (Feb 2026).

```ts
import { test } from 'vitest';
import * as fc from 'fast-check';
import { it } from '@fast-check/vitest';

it.prop([fc.string()])('parse(render(x)) === x for any string', (x) => {
  expect(parse(render(x))).toBe(x);
});
```

**Concrete example: SQL identifier validator**

```ts
import fc from 'fast-check';

const ORACLE_RESERVED = ['SELECT', 'FROM', 'WHERE', /* ... */];

const sqlIdentifier = fc.oneof(
  { weight: 5, arbitrary: fc.stringMatching(/^[A-Za-z_][A-Za-z0-9_]{0,29}$/) },
  { weight: 2, arbitrary: fc.constantFrom(...ORACLE_RESERVED) },
  { weight: 2, arbitrary: fc.string({ minLength: 0, maxLength: 300 }) },
  { weight: 1, arbitrary: fc.unicodeString({ maxLength: 64 }) },
  { weight: 1, arbitrary: fc.string().map(s => `"${s.replace(/"/g, '""')}"`) },
  { weight: 1, arbitrary: fc.constantFrom('', ' ', '\t', '\n', '\0', '0abc', '1', '--x', '/*x*/') },
);

test.prop([sqlIdentifier, fc.constantFrom('oracle', 'delta', 'mysql', 'teradata')])(
  'validateIdentifier(id, dialect) never throws, returns Ok | Err',
  (id, dialect) => {
    const r = validateIdentifier(id, dialect);
    expect(r.kind === 'ok' || r.kind === 'err').toBe(true);
  },
);
```

That single property hits: empty, pure whitespace, reserved words, unicode lookalikes, SQL-comment-injection attempts, over-length names per dialect, quoted-identifier escape-doubling — all inputs Dan has never typed.

**Model-based testing** via `fc.commands` is the right tool for LiveValidator's form state machine: define a correct-by-construction model, generate random command sequences, assert real and model stay in sync. Catches "the 17th click sequence" bugs no human enumerates.

### 3.3 Hypothesis (Python)

Current: **Hypothesis 6.152.x** (April 2026).

```python
from hypothesis import given, strategies as st, example, settings

column_schema = st.fixed_dictionaries({
    "name": st.text(min_size=1, max_size=128),
    "type": st.sampled_from(["STRING", "INT", "DECIMAL(10,2)", "TIMESTAMP", "BINARY"]),
    "nullable": st.booleans(),
    "default": st.one_of(st.none(), st.text(max_size=64), st.integers(), st.floats(allow_nan=False)),
})

@given(column_schema)
@example({"name": "order", "type": "STRING", "nullable": False, "default": None})  # regression seed
@settings(max_examples=500, deadline=None)
def test_column_roundtrip(col):
    rendered = render_column_ddl(col, dialect="delta")
    parsed = parse_column_ddl(rendered, dialect="delta")
    assert parsed == col
```

**`@example` seeds known-bad inputs** so regressions can never silently recur.

`RuleBasedStateMachine` generates sequences of method calls and verifies invariants after every step. `target()` guides search toward values that maximize/minimize a metric — use to hunt for slow paths and stack-depth pathologies random sampling rarely finds.

Hypothesis keeps a **failure database** (`.hypothesis/examples/`) that persists shrunk failures across runs. **Commit it.** Otherwise you rediscover the same bug.

### 3.4 Mutation Testing

Answers a question line coverage cannot: *if your production code were buggy, would your tests notice?*

**Stryker (JS/TS)** — `@stryker-mutator/core` 9.x:

```json
{
  "packageManager": "pnpm",
  "testRunner": "vitest",
  "reporters": ["clear-text", "html", "dashboard"],
  "coverageAnalysis": "perTest",
  "mutate": [
    "src/validators/**/*.{ts,tsx}",
    "src/sqlgen/**/*.ts",
    "!src/**/*.test.{ts,tsx}"
  ],
  "incremental": true,
  "incrementalFile": ".stryker-tmp/incremental.json",
  "concurrency": 4,
  "thresholds": { "high": 85, "low": 70, "break": 65 },
  "checkers": ["typescript"],
  "tsconfigFile": "tsconfig.json"
}
```

Runtime is the honest obstacle — mutation runs take 10–100× longer. Mitigations: (1) `incremental: true` for PRs; (2) nightly full runs against `main`.

**mutmut vs cosmic-ray (Python):**

- **mutmut** — simpler CLI, more Python-specific operators, active maintenance (~88.5% detection rate recent benchmarks). Best default for LiveValidator's Python backend.
- **cosmic-ray** — AST-based, better for very large suites, distributed via Celery.

Scope to core validator/SQL-gen modules, never the whole repo.

**What mutation score actually means:** a real 2026 case study showed a codebase with **93.1% line coverage had a 58.6% mutation score**. Over a third could be mutated without any test noticing. Targets: 60–70% baseline, 80%+ for core validators/parsers. 100% is neither achievable nor desirable.

### 3.5 Fuzz Testing

- **Atheris** (Google) — coverage-guided Python fuzzer on libFuzzer. Apply to SQL parser, YAML/JSON config parsers, regex construction. `atheris.FuzzedDataProvider` gives typed accessors.
- **When fuzz vs PBT:** PBT for semantic properties (`parse ∘ render = id`); fuzz for parsers/deserializers where you want the engine to discover coverage on its own.

### 3.6 Snapshot / Golden-File Testing

Excellent for deterministic pure-function output: generated SQL, rendered markup, exported JSON, AST dumps.

```python
# pytest-regressions pattern
def test_select_with_cast_oracle(data_regression):
    sql = generate_sql(
        table="sales",
        columns=["id", "amount"],
        casts={"amount": "NUMBER(18,2)"},
        dialect="oracle",
    )
    data_regression.check({"sql": sql})
```

**Review workflow:**

1. Golden files live in `tests/golden/`, committed.
2. CI fails on diff. Regenerate locally: `pytest --regen-all` or `UPDATE_GOLDENS=1 pytest`.
3. **Every regenerated golden appears in the PR diff** and is reviewed as code.
4. Normalize noise at generation (UTC timestamps, sorted keys, stable IDs).
5. One golden per semantic scenario; a 800-line golden is a liability.

Goldens go toxic when reviewers reflexively regenerate on any failure. Mitigate: keep them small, name them descriptively, comment the scenario at the top.

### 3.7 Differential Testing: The Multi-Dialect Superpower

LiveValidator runs against Oracle, Delta, MySQL, Teradata. **Run the same logical input through each backend and cross-check invariants across the outputs.**

```python
DIALECTS = ["oracle", "delta", "mysql", "teradata"]

@given(column_schema)
def test_null_check_semantics_equivalent(col):
    outputs = {d: run_null_check(col, dialect=d) for d in DIALECTS}
    row_sets = {d: frozenset(out.failing_row_ids) for d, out in outputs.items()}
    assert len(set(row_sets.values())) == 1, f"Dialect disagreement: {row_sets}"
```

This is the approach behind RAGS (Slutz, 1998), SQLancer, SQLRight, and 2025's "Enhanced Differential Testing in Emerging Database Systems." For every validation rule expected to agree across backends, generate inputs, run all four, diff the results. Disagreement is either a real semantic gap (document as known divergence) or a bug (fix it).

Encode known divergences as part of the property: "for all inputs, `mysql_result == delta_result`, and `oracle_result` differs only when the column is CHAR/VARCHAR2 and contains `''`".

### 3.8 Metamorphic Testing

Relations between related inputs, not absolute outputs. Useful relations for LiveValidator:

- **Permutation invariance** — reordering rows doesn't change a null-check count.
- **Scale invariance** — 10× subsample yields consistent results within tolerance.
- **Projection monotonicity** — adding an irrelevant column doesn't change row-level validation.
- **Inverse** — `parse(render(x)) == x`.
- **Restriction** — filtering `WHERE x > 0` can only reduce or keep equal the count of failures of a non-negativity rule.
- **Rewrite equivalence** — `col IN (1,2,3)` and `col=1 OR col=2 OR col=3` produce identical failure sets.

Powerful for LiveValidator specifically because ground truth is often unknown but relationships between runs are cheap to check.

### 3.9 AI-Assisted Test Generation

Prompting patterns that work:

- "Here is `validateIdentifier(id, dialect)`. Write fast-check arbitraries and 5 properties covering empty input, unicode, reserved words, over-length, dialect-specific quoting. Output runnable Vitest code."
- "Given this `<RuleEditor />`, generate 10 hostile user-input scenarios for RTL tests: rapid typing, paste with control chars, blur while mid-edit, undo-after-submit, IME composition."
- "Here's the diff for PR #412. List every edge case the new code handles and every edge case it silently doesn't. For each missing case, suggest the Hypothesis strategy that would expose it."
- "For the SQL generator, propose 5 metamorphic relations across Oracle/Delta/MySQL/Teradata. Write as pytest parametrized tests."

Store in `.claude/prompts/`; the agent + library combination outperforms any single-tool approach.

**Critical:** AI-generated tests are a starting point, not a deliverable. Tests that lock in current behavior (including bugs) are worse than no tests. Every AI-authored test needs human review asking: "what property is this asserting, and is that property actually a requirement?"

### 3.10 Coverage Metrics

| Metric | Measures | Gameable? | LiveValidator signal |
|---|---|---|---|
| Line coverage | Lines executed | Trivially | Weak |
| Branch coverage | Both sides of every condition | Mostly | Medium |
| Mutation score | Whether tests *detect* injected faults | Hard | **Strong** |
| Property count + shrunk regressions | Breadth of generated exploration | Hard | **Strong** |

**Gate releases on mutation score for core modules.** 95% line coverage + 45% mutation score means shipping a half-tested product.

### Recommendations for LiveValidator (Generative Testing)

1. **Install fast-check (JS) and Hypothesis (Py) this week.** One property per validator module: `parse ∘ render = id`, `validate(x)` never throws, `validate(x)` is deterministic. Commit `.hypothesis/` and fast-check seed files.
2. **Build differential tests across the four dialects for every rule.** One parametrized test per rule, four executions, cross-check failing-row sets. Single highest-leverage tactic given LV's multi-DB architecture.
3. **Wrap the SQL generator in golden-file tests per dialect and feature** with a `--regen` workflow that forces every change to surface in PR diffs.
4. **Stateful testing on the form/rule editor** — `fc.commands` for React state, `hypothesis.stateful` for backend job state. Generate sequences of create/edit/cancel/retry/delete/undo.
5. **Stryker (incremental on PR, full nightly) + mutmut (nightly) on core validator packages.** Commit a baseline mutation score and enforce PRs must not drop it.
6. **Atheris fuzz campaigns nightly** — SQL parser, rule DSL parser, YAML config ingestion. 4-hour campaigns with persisted corpus in object storage.
7. **Codify metamorphic relations per validator class** (permutation, scale, rewrite equivalence, restriction monotonicity). Compose with Hypothesis at near-zero cost.

---

## 4. Multi-DB Testing with Testcontainers

> *"They are jobs that run on Oracle/Delta/MySQL/Teradata etc. I don't have a test instance of those source systems." — Dan*

This is **the** blocker. The ecosystem has solved 90% in 2026. Teradata — the harder 10% — has workable-if-imperfect answers.

**Three-layer defense:**

- **Layer A — Fakes for unit tests.** Wrap DB access behind a narrow interface (`Source.read`, `Source.describe`, `Source.explain`). Most of the codepath — orchestration, result comparison, report generation — tests against in-memory fakes. No DB. Fast, deterministic, every commit.
- **Layer B — Testcontainers for integration.** Ephemeral, real database engines per test run. Catches dialect-specific SQL, driver quirks, schema-drift bugs fakes cannot. Per-PR for fast engines, nightly for slow ones.
- **Layer C — Golden-file + contract tests.** SQL generator input pinned to checked-in `.sql` files. Any diff reviewed in PR. Cheap, fast, prevents silent semantic drift.

### 4.1 Testcontainers Framework

Testcontainers is a JVM-born library that programmatically launches Docker containers for a test's lifetime, tears them down, handles wait-strategy and port-mapping. The de facto integration-testing standard.

| Binding | Status 2026 | Notes |
|---|---|---|
| `testcontainers-java` | Mature 1.x | Deepest module catalog |
| `testcontainers-python` | Mature 4.x | PyPI `testcontainers[oracle-free]`, `[mysql]`. Good pytest fixture story |
| `testcontainers-node` | Mature | `@testcontainers/mysql`, `@testcontainers/oracle-free`. TS typings |
| `testcontainers-go` | Mature | Active 2026 releases |
| `testcontainers-rs` | Pre-1.0 but usable | Community modules |

For LiveValidator (Python-heavy) → **testcontainers-python**. If any piece is JVM (Spark), Java bindings share the Docker daemon.

**Testcontainers Cloud** — managed Docker-in-cloud backend. Frees CI runners from Docker-in-Docker, parallelizes startup. Costs minutes (Docker Pro 100/mo, Team 500/mo, Business 1,500/mo). Stay on GitHub Actions' built-in Docker for per-PR; reach for Testcontainers Cloud only if memory becomes a bottleneck.

**Singleton / reuse pattern — the 10× speedup:** default behavior is one container per test class. For 50 integration tests against Oracle, that's 50 × 60s = 50 min. Unacceptable. Singleton containers (module-level static) give cold start ~60s, subsequent tests ~1s each.

### 4.2 Per-Database Deep Dives

#### Oracle

**`gvenzl/oracle-free:23.7-slim-faststart`** — Oracle Database Free Edition 23ai. Multi-arch (Apple Silicon supported since 23.5). Startup ~45–90s, RAM ~1.5GB. Service name `FREEPDB1` (legacy XE was `XEPDB1`). Init scripts via `/container-entrypoint-initdb.d/`.

```python
# tests/conftest.py
import pytest
from testcontainers.oracle import OracleDbContainer

@pytest.fixture(scope="session")
def oracle():
    with OracleDbContainer("gvenzl/oracle-free:23.7-slim-faststart") as ora:
        yield ora
```

Catches: MERGE syntax, ROWNUM vs ROW_NUMBER() OVER, hierarchical queries (CONNECT BY), NVL vs COALESCE semantics, empty-string-vs-NULL behavior, sequence vs IDENTITY columns.

Docs: https://github.com/gvenzl/oci-oracle-free

#### MySQL

Official `mysql:8`. Trivial. <10s startup, ~400MB RAM. **Don't substitute MariaDB** — subtle dialect divergences (JSON storage, sequences, GTID) will burn you.

#### PostgreSQL (for completeness)

`postgres:16`. The reference for "how easy it could be." If LV ever adds Postgres support, cheapest to integrate.

#### Teradata

The hardest case. Options in April 2026:

1. **Teradata Vantage Express** (VMware OVA) — free for dev, but VMware-only and ~15GB. Not CI-friendly. Use as a local dev fallback.
2. **ClearScape Analytics Experience (CSAE)** — Teradata's managed free trial. Tokened endpoint, rate-limited. Usable for smoke tests but not per-PR.
3. **Vantage Cloud Lake Dev Tier** — paid but cheap. Single-endpoint nightly CI viable.
4. **`sqlglot` parse-only** — at minimum, transpile generated Teradata SQL through sqlglot's Teradata dialect as a syntax-validity check. Cheapest.
5. **Mocked JDBC** (Acolyte) — useful for driver-level contracts. Won't catch semantic bugs.
6. **TTU (Teradata Tools & Utilities)** — for command-line validation of DDL.

**Realistic recommendation:** per-PR → sqlglot parse validation only. Nightly → one shared Vantage Cloud Lake endpoint with concurrency serialization. Document which Teradata features are untested; flag regressions via customer telemetry fastest.

#### Delta Lake

**`delta-spark` 4.2.0** (April 2026, Spark 4.x). Runs in-process — no container needed.

```python
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@pytest.fixture(scope="session")
def spark():
    builder = (SparkSession.builder.appName("lv-test")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    s = configure_spark_with_delta_pip(builder).getOrCreate()
    yield s
    s.stop()
```

For Databricks-specific features (DBR runtime behaviors, Photon): **Databricks Connect** against a shared dev workspace. Iceberg/UniForm interop: Iceberg v3 preview on DBR 18+; Unity Catalog Iceberg write support GA April 2026.

### 4.3 Dialect-Safe Query Abstractions

**sqlglot** (https://github.com/tobymao/sqlglot) — transpile SQL across 25+ dialects, parse, optimize. **Core primitive for LiveValidator.**

```python
import sqlglot

oracle_sql = """
SELECT customer_id, MAX(created_at) KEEP (DENSE_RANK FIRST ORDER BY order_total DESC) OVER (PARTITION BY region) AS top_date
FROM orders
WHERE ROWNUM < 100
"""
delta_sql = sqlglot.transpile(oracle_sql, read="oracle", write="databricks")[0]
# → uses LIMIT 100 and ROW_NUMBER()
```

Databricks dialect extends Spark. SQLAlchemy Core/ORM for DDL portability; jOOQ for JVM paths. **When dialect abstraction is a trap:** if it masks real semantic differences, you get false confidence. Use abstractions to generate, differential tests to verify.

### 4.4 Why H2/SQLite Aren't Enough

- **Oracle-specific:** MERGE, ROWNUM, CONNECT BY, hierarchical queries, PL/SQL.
- **Teradata-specific:** QUALIFY, REPLICATE semantics, VT tables.
- **Delta-specific:** MERGE with deletion vectors, time travel, CDF.

H2/SQLite give false confidence. Use fakes for unit tests, real containers for integration. Never pretend H2 is Oracle.

### 4.5 CI Cost/Time Trade-offs

- **Per-PR fast** — MySQL (~10s), Postgres (~10s), Delta-Spark (~15s, in-process).
- **Per-PR optional** — Oracle Free (~60–90s, acceptable with singleton caching).
- **Nightly** — Teradata shared endpoint, bigger fixture datasets, cross-version matrices.
- **External** — customer workspaces via staging app (see §5).

Parallelize across matrix jobs; singleton within each.

### 4.6 Fixture Data Management

- Checked-in SQL seed scripts per dialect (`fixtures/oracle/seed.sql`).
- Anonymized golden datasets (100s of rows) in the repo.
- Synthetic schemas via Faker / Hypothesis for diversity.
- Reset between tests with `TRUNCATE` or schema-level drops.

### 4.7 Concrete Code

**Testcontainers-Python + Oracle Free:**

```python
import pytest
import oracledb
from testcontainers.oracle import OracleDbContainer

@pytest.fixture(scope="session")
def oracle_conn():
    with OracleDbContainer("gvenzl/oracle-free:23.7-slim-faststart") as ora:
        dsn = ora.get_connection_url().replace("oracle+oracledb://", "")
        conn = oracledb.connect(user="test", password="test", dsn=dsn)
        # seed schema
        with conn.cursor() as cur, open("fixtures/oracle/seed.sql") as f:
            for stmt in f.read().split(";"):
                if stmt.strip():
                    cur.execute(stmt)
            conn.commit()
        yield conn
        conn.close()

def test_oracle_null_check(oracle_conn):
    from livevalidator.checks import null_check
    result = null_check(oracle_conn, table="customers", column="email")
    assert result.failed == 2
```

**Testcontainers-Node + MySQL:**

```typescript
import { MySqlContainer } from '@testcontainers/mysql';
import { describe, beforeAll, afterAll, it, expect } from 'vitest';
import mysql from 'mysql2/promise';

describe('mysql integration', () => {
  let container: Awaited<ReturnType<MySqlContainer['start']>>;
  let conn: mysql.Connection;

  beforeAll(async () => {
    container = await new MySqlContainer('mysql:8').start();
    conn = await mysql.createConnection({
      host: container.getHost(),
      port: container.getPort(),
      user: 'test',
      password: 'test',
      database: 'test',
    });
    await conn.query(`CREATE TABLE customers (id INT, email VARCHAR(255))`);
  }, 60_000);

  afterAll(async () => {
    await conn.end();
    await container.stop();
  });

  it('finds null emails', async () => {
    const [rows] = await conn.query(`SELECT COUNT(*) as n FROM customers WHERE email IS NULL`);
    expect((rows as any)[0].n).toBe(0);
  });
});
```

**sqlglot transpile:**

```python
import sqlglot
oracle = "SELECT * FROM t WHERE ROWNUM <= 10"
delta  = sqlglot.transpile(oracle, read="oracle", write="databricks")[0]
# → SELECT * FROM t LIMIT 10
```

**Golden-file test skeleton:**

```python
# tests/golden/test_sql_gen.py
import json, os, pytest
from livevalidator.sqlgen import generate

UPDATE = os.environ.get("UPDATE_GOLDENS") == "1"

@pytest.mark.parametrize("dialect", ["oracle", "delta", "mysql", "teradata"])
@pytest.mark.parametrize("rule", ["not_null", "unique", "fk", "range"])
def test_rule_sql(dialect, rule, data_regression):
    sql = generate(rule, dialect=dialect, table="customers", column="email")
    path = f"tests/golden/{dialect}_{rule}.sql"
    if UPDATE:
        with open(path, "w") as f:
            f.write(sql)
    else:
        with open(path) as f:
            expected = f.read()
        assert sql.strip() == expected.strip()
```

### 4.8 DB Testing Matrix

| Database | Recommended approach | Startup cost | CI tier | Fallback if container unavailable |
|---|---|---|---|---|
| MySQL 8 | `mysql:8` Testcontainer | <10s | Per-PR | In-memory fake + sqlglot parse |
| Postgres 16 | `postgres:16` Testcontainer | <10s | Per-PR | In-memory fake + sqlglot parse |
| Oracle 23ai | `gvenzl/oracle-free:23.7-slim-faststart` singleton | ~60–90s | Per-PR | sqlglot Oracle dialect parse-only |
| Delta | `delta-spark` in-process | ~15s | Per-PR | In-memory DataFrame fake |
| Iceberg | Local PyIceberg + MinIO container | ~20s | Per-PR | sqlglot parse + SDK mock |
| Teradata | Vantage Cloud Lake dev endpoint, concurrency serialized | N/A (remote) | Nightly | sqlglot Teradata dialect parse-only |
| Databricks SQL warehouse | Databricks Connect against shared dev workspace | N/A | Nightly | Mocked SDK |

### Recommendations for LiveValidator (Multi-DB)

1. **Add testcontainers-python with MySQL + Oracle Free as per-PR, Delta-Spark in-process.** This alone eliminates ~70% of "works here, broken at NVIDIA" bugs.
2. **Use sqlglot as the primary SQL-generation primitive.** Write once, transpile. Test parse-validity for all four dialects at zero cost even without containers.
3. **Golden-file tests per (dialect × rule) pair** with `UPDATE_GOLDENS=1` review workflow.
4. **Differential tests** (cross-reference §3.7) across the four dialects for every rule.
5. **Teradata:** sqlglot parse-only per-PR, one shared Vantage Cloud Lake endpoint nightly.
6. **Testcontainers singleton fixtures** (session-scoped) for 10× speedup on repeated tests.
7. **Fixtures as SQL seed scripts** in `fixtures/<dialect>/seed.sql`, with Faker-based generation for diversity.
8. **Databricks Connect nightly** for Spark/Photon-specific validation.
9. **Dedicated CI runner pool** or larger GitHub runner for Oracle tests if they dominate PR time.
10. **Publish the DB testing matrix** (above) in the repo README so PS engineers know which DBs are tested at which tier.

---

## 5. Databricks Apps Integration

### 5.1 What Databricks Apps Means for Testing

Databricks Apps is the container-based web-app platform on Databricks: Python 3.11 / Ubuntu 22.04, 2 vCPU / 6 GB RAM per app. Natively supports FastAPI, Streamlit, Dash, Gradio, Flask, Reflex, Shiny, Node.js — any HTTP process binding `DATABRICKS_APP_PORT`. Every app sits behind a Databricks-managed reverse proxy that enforces workspace SSO and injects identity headers on every request:

- `X-Forwarded-User`, `X-Forwarded-Email`, `X-Forwarded-Preferred-Username`
- `X-Forwarded-Access-Token`, `X-Forwarded-Host`, `X-Real-Ip`, `X-Request-Id`

Each app has a dedicated **service principal** with auto-injected `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`. Apps act as that SP (**app auth**) or as the requesting user (**on-behalf-of** / OBO) by forwarding the user token. Dependencies declared in `app.yaml` (`valueFrom`); deploy via `databricks apps deploy` or Declarative Automation Bundles (DABs). As of April 2026: user-auth scopes, DABs-native app resources, managed OTel telemetry, and `databricks apps run-local` are all GA.

**Implications for testing LiveValidator:**

- Local dev needs the reverse-proxy shim to reach OBO codepaths.
- CI needs ephemeral deployments per PR to avoid step-on-each-other collisions.
- Playwright needs pre-minted OAuth tokens — do not drive SSO in CI.
- Observability piggybacks on managed OTel to UC Delta tables.

### 5.2 Local Dev Loop

`databricks apps run-local` starts a local reverse-proxy shim on `localhost:8001` that forwards to your app process and injects the same `X-Forwarded-*` headers as production. Single most important tool for testing OBO codepaths locally.

```bash
# Once
databricks apps run-local --prepare-environment

# Dev loop
databricks apps run-local --debug --port 5678
# Proxy -> http://localhost:8001, debugpy -> :5678
```

For LiveValidator's React + FastAPI split, Vite proxies API calls to FastAPI while `run-local` wraps FastAPI:

```json
// frontend/vite.config.ts
{
  "server": {
    "proxy": {
      "/api": { "target": "http://localhost:8001", "changeOrigin": true }
    }
  }
}
```

```bash
# Terminal 1 — FastAPI behind the Databricks proxy shim
databricks apps run-local

# Terminal 2 — React dev server with HMR
cd frontend && npm run dev
```

Config in `app.yaml` env vars; secrets in Databricks secret scopes referenced by `valueFrom: { secret: ... }`. `.env.local` mirrors env vars (never tokens) for local parity.

### 5.3 Three Service Principals, Three Purposes

| Identity | Used by | Source |
|---|---|---|
| `livevalidator-ci-pr` | GitHub Actions deploying PR apps | GitHub OIDC federation → Databricks (no long-lived secret) |
| `livevalidator-test-user` | Playwright acting as end user | Committed-encrypted `storageState.json` from M2M token exchange |
| `livevalidator-app-<env>` | Each deployed app (dev, pr, staging, prod) | Auto-created by Databricks; client_id/secret auto-injected |

```bash
# Create once per environment
databricks service-principals create --display-name "livevalidator-ci-pr"
SP_ID=$(databricks service-principals list --filter "displayName eq 'livevalidator-ci-pr'" | jq -r '.[0].id')
databricks service-principal-secrets create "$SP_ID"
```

Grant the CI SP `CAN_MANAGE` on the app bundle path, `USAGE` on the test UC catalog, `CAN_USE` on the test SQL warehouse. Grant the test-user SP only `CAN_USE` on the app. **Rotate the test-user secret quarterly;** the CI SP uses OIDC with no secret to rotate.

### 5.4 Playwright + Databricks SSO

**Do not drive Azure AD / Okta / Google SSO in CI.** Device trust, MFA, captchas make this the single largest source of E2E flake. Instead: pre-mint an OAuth M2M token for the test-user SP, stash in `storageState.json`, commit as encrypted GitHub Actions secret.

```ts
// tests/auth.setup.ts — runs once, locally, on rotation
import { test as setup, expect } from '@playwright/test';
import { writeFileSync, mkdirSync } from 'node:fs';

setup('authenticate databricks sp', async ({ page, request }) => {
  const host   = process.env.DATABRICKS_HOST!;
  const cid    = process.env.TEST_USER_SP_CLIENT_ID!;
  const secret = process.env.TEST_USER_SP_SECRET!;

  // Mint OAuth M2M token
  const resp = await request.post(`${host}/oidc/v1/token`, {
    headers: {
      'Authorization': `Basic ${Buffer.from(`${cid}:${secret}`).toString('base64')}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    form: { grant_type: 'client_credentials', scope: 'all-apis' },
  });
  expect(resp.ok()).toBeTruthy();
  const { access_token } = await resp.json();

  // Seed cookie the Databricks proxy recognises
  const appUrl = new URL(process.env.PR_APP_URL!);
  await page.context().addCookies([{
    name: 'access-token', value: access_token,
    domain: appUrl.hostname, path: '/',
    httpOnly: true, secure: true, sameSite: 'Lax',
  }]);
  await page.goto('/');
  await expect(page.getByTestId('app-ready')).toBeVisible({ timeout: 120_000 });

  mkdirSync('tests/.auth', { recursive: true });
  await page.context().storageState({ path: 'tests/.auth/user.json' });
});
```

```typescript
// playwright.config.ts (DB Apps specific)
import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: false,                // cold-start serialises better
  retries: process.env.CI ? 2 : 0,
  reporter: [['html'], ['github']],
  use: {
    baseURL: process.env.PR_APP_URL ?? 'http://localhost:5173',
    trace: 'on-first-retry',
    video: 'retain-on-failure',
    screenshot: 'only-on-failure',
    storageState: 'tests/.auth/user.json',
    extraHTTPHeaders: {
      'X-Request-Id': `pw-${process.env.GITHUB_RUN_ID ?? 'local'}`,
    },
  },
  projects: [
    { name: 'setup', testMatch: /auth\.setup\.ts/ },
    { name: 'chromium', dependencies: ['setup'], use: { ...devices['Desktop Chrome'] } },
  ],
  globalTimeout: 20 * 60 * 1000,        // cold starts can be 90s
  expect: { timeout: 15_000 },
});
```

### 5.5 Ephemeral Test Apps per PR

The strongest pattern for LiveValidator given NVIDIA-blast-radius risk: **one separate Databricks App per PR** that lives and dies with the PR.

1. PR open/synchronize → deploy `livevalidator-pr-${PR}` via DABs target `pr`.
2. Playwright runs against `https://livevalidator-pr-1234-abcd.cloud.databricksapps.com`.
3. PR close/merge → `databricks bundle destroy -t pr`.

Cost: per-request + modest keep-warm; idle PR apps near zero. Deploy time ~30–90s — amortized over PR lifetime this is cheaper than one cross-PR contamination incident.

Shared test UC schema `ruslan_test.livevalidator_pr` with per-PR table prefixes (`pr_1234_customers`), seeded from fixtures at deploy time. Share expensive resources (vector index, serving endpoint); isolate cheap ones (tables, volumes).

### 5.6 Databricks Asset Bundle Configuration

```yaml
# databricks.yml
bundle:
  name: livevalidator

include:
  - resources/*.yml

variables:
  warehouse_id:
    description: SQL warehouse ID for the app

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dev.cloud.databricks.com
    variables:
      warehouse_id: 1111aaaa2222bbbb

  pr:
    mode: development
    workspace:
      host: https://dev.cloud.databricks.com
      root_path: /Workspace/Users/ci@livevalidator/.bundle/${bundle.name}/pr-${var.pr_number}
    variables:
      pr_number:
        description: GitHub PR number
      warehouse_id: 1111aaaa2222bbbb
    resources:
      apps:
        livevalidator:
          name: livevalidator-pr-${var.pr_number}

  staging:
    mode: production
    workspace: { host: https://staging.cloud.databricks.com }

  prod:
    mode: production
    workspace: { host: https://prod.cloud.databricks.com }
    permissions:
      - group_name: livevalidator-admins
        level: CAN_MANAGE
```

```yaml
# resources/app.yml
resources:
  apps:
    livevalidator:
      name: livevalidator
      source_code_path: ../src/app
      description: "Cross-DB data validation app"
      resources:
        - name: warehouse
          description: SQL warehouse for validation queries
          sql_warehouse:
            id: ${var.warehouse_id}
            permission: CAN_USE
        - name: oracle_secrets
          description: Oracle connection string
          secret:
            scope: livevalidator
            key:   oracle_dsn
            permission: READ
```

```yaml
# app.yaml
command:
  - "uvicorn"
  - "app.main:app"
  - "--host"
  - "0.0.0.0"
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: warehouse
  - name: LIVEVALIDATOR_UC_CATALOG
    value: "ruslan_test"
  - name: LIVEVALIDATOR_UC_SCHEMA
    value: "livevalidator"
  - name: ORACLE_DSN
    valueFrom: oracle_secrets
  - name: SENTRY_DSN
    valueFrom: sentry_secret
  - name: LIVEVALIDATOR_ENV
    value: "pr"
```

### 5.7 Full PR-Ephemeral Workflow

```yaml
# .github/workflows/pr-ephemeral.yml
name: pr-ephemeral
on:
  pull_request:
    types: [opened, synchronize, reopened]

permissions:
  id-token: write
  contents: read
  pull-requests: write

jobs:
  deploy-pr-app:
    runs-on: ubuntu-latest
    environment: pr
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - name: OIDC to Databricks
        uses: databricks/auth-action@v1
        with:
          host: ${{ vars.DATABRICKS_HOST }}
          oidc: true
      - name: Deploy PR bundle
        run: |
          databricks bundle deploy -t pr \
            --var="pr_number=${{ github.event.pull_request.number }}"
          databricks bundle run livevalidator -t pr \
            --var="pr_number=${{ github.event.pull_request.number }}"
      - name: Wait for app ready
        run: |
          URL=$(databricks apps get livevalidator-pr-${{ github.event.pull_request.number }} | jq -r .url)
          for i in {1..30}; do
            curl -fsS "$URL/healthz" && break || sleep 10
          done
          echo "PR_APP_URL=$URL" >> $GITHUB_ENV
      - name: Seed test data
        run: uv run python scripts/seed_pr.py
        env:
          PR_NUMBER: ${{ github.event.pull_request.number }}
          WAREHOUSE_ID: ${{ vars.WAREHOUSE_ID }}
      - name: Playwright E2E
        env:
          PR_APP_URL: ${{ env.PR_APP_URL }}
          TEST_USER_SP_CLIENT_ID: ${{ secrets.TEST_USER_SP_CLIENT_ID }}
          TEST_USER_SP_SECRET:    ${{ secrets.TEST_USER_SP_SECRET }}
        run: |
          npm ci
          npx playwright install --with-deps chromium
          npx playwright test
      - name: Comment PR with URL
        uses: marocchino/sticky-pull-request-comment@v2
        with:
          message: |
            Preview app: ${{ env.PR_APP_URL }}
            (auto-destroyed on PR close)
```

```yaml
# .github/workflows/pr-teardown.yml
name: pr-teardown
on:
  pull_request:
    types: [closed]

jobs:
  destroy:
    runs-on: ubuntu-latest
    permissions: { id-token: write, contents: read }
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - uses: databricks/auth-action@v1
        with: { host: ${{ vars.DATABRICKS_HOST }}, oidc: true }
      - run: |
          databricks bundle destroy -t pr --auto-approve \
            --var="pr_number=${{ github.event.pull_request.number }}"
```

### 5.8 Test Data Seeding

```python
# scripts/seed_pr.py
from databricks.sdk import WorkspaceClient
from databricks import sql
from faker import Faker
import os, uuid

pr = os.environ["PR_NUMBER"]
fake = Faker()
fake.seed_instance(int(pr))      # deterministic per PR

w = WorkspaceClient()
conn = sql.connect(
    server_hostname=w.config.host,
    http_path=f"/sql/1.0/warehouses/{os.environ['WAREHOUSE_ID']}",
    credentials_provider=lambda: w.config.authenticate,
)
c = conn.cursor()
c.execute(f"CREATE SCHEMA IF NOT EXISTS ruslan_test.livevalidator_pr_{pr}")
c.execute(f"""
CREATE OR REPLACE TABLE ruslan_test.livevalidator_pr_{pr}.customers AS
SELECT * FROM VALUES {','.join(
  f"('{uuid.uuid4()}','{fake.name()}','{fake.email()}')" for _ in range(1000)
)} AS t(id, name, email)
""")
```

### 5.9 Observability for Databricks Apps

`stdout`/`stderr` from the app container is captured by Databricks:

```bash
databricks apps logs livevalidator-pr-1234 --follow
databricks apps logs livevalidator-pr-1234 --since 10m | grep ERROR
```

**Managed OpenTelemetry collector (GA 2026).** Enable per app in Settings → App telemetry → pick a UC schema. Databricks writes three Delta tables: `otel_metrics`, `otel_spans`, `otel_logs`. `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_PROTOCOL` auto-injected. Instrument with:

```yaml
# app.yaml
command: ["opentelemetry-instrument", "uvicorn", "app:app",
          "--host", "0.0.0.0", "--port", "8000"]
env:
  - name: OTEL_TRACES_SAMPLER
    value: "parentbased_traceidratio"
  - name: OTEL_TRACES_SAMPLER_ARG
    value: "0.1"
```

```
# requirements.txt
opentelemetry-distro
opentelemetry-exporter-otlp-proto-grpc
opentelemetry-instrumentation-fastapi
opentelemetry-instrumentation-requests
```

Query p95 latency per route:

```sql
SELECT attributes['http.route'] AS route,
       percentile_approx(duration_ms, 0.95) p95,
       count(*) AS n
FROM   livevalidator_telemetry.otel_spans
WHERE  event_time > current_timestamp() - INTERVAL 1 HOUR
GROUP  BY 1 ORDER BY p95 DESC;
```

**Sentry** works via normal egress (Databricks Apps have outbound internet by default, customer workspace network rules permitting). Use `environment=` to tag PR/dev/staging/prod; biggest value is grouping across customer workspaces when LiveValidator crashes at NVIDIA.

### 5.10 Common Gotchas

| Gotcha | Fix |
|---|---|
| First request after idle is 30–90s | Readiness probe + warmup curl in CI before Playwright. Keep-alive pings for prod. |
| Deploy takes 30–90s | Parallelise: deploy PR app while Playwright builds & installs. |
| Local file writes disappear on redeploy | Never persist state to local FS. Use UC volumes or Lakebase. |
| Only one HTTP port, no sockets | Long-running jobs don't live inside the app — invoke Databricks Jobs via SDK. |
| `DATABRICKS_APP_PORT` auto-set | Don't hard-code 8000; read env. Streamlit auto-wired, FastAPI/uvicorn must honor. |
| 10 MB per-file deploy limit | Bundle large assets in UC volumes. |
| `X-Forwarded-Access-Token` empty locally | `run-local` injects from CLI profile; pure `uvicorn` doesn't. |
| OAuth scope mismatch breaks OBO at runtime not dev | Integration test calling UC with user token checking scope errors. |
| Customer workspace blocks egress | Verify Sentry/OTel endpoint reachability at engagement kickoff. Default OTel-to-UC (always in-workspace). |
| Cross-workspace SP reuse | App SPs are workspace-scoped. Document per-workspace onboarding. |

### Recommendations for LiveValidator as a Databricks Apps App

1. **Adopt `databricks apps run-local` as the canonical local dev command.** Document in README. OBO code paths outside `run-local` test something that doesn't exist in production.
2. **Ship DABs-based deploys with four targets** (`dev`, `pr`, `staging`, `prod`) this sprint. DABs is the supported multi-env path.
3. **Ephemeral per-PR apps** — deploy on PR open, destroy on close. Single biggest mitigation for "bug hits NVIDIA" fear.
4. **Three service principals, three purposes** — deploy (OIDC, no secret), test-user (rotated quarterly), per-app-runtime (Databricks-managed). Least-privilege grants.
5. **Pre-minted Playwright `storageState`.** Never drive SSO in CI.
6. **Shared test UC schema, PR-scoped table prefixes**, Faker-seeded in `seed_pr.py` before Playwright.
7. **Enable managed OTel telemetry on staging and prod.** Traces land in UC — per-customer latency/error dashboards free.
8. **Smoke-test matrix: 5–7 Playwright journeys per PR**; full E2E + visual + a11y nightly against staging. Don't run full suite per-PR — cold starts + 1h token expiry.
9. **Health endpoint + warmup step in CI** before Playwright. `GET /healthz` loop up to 2 min.
10. **Document "Deploying LiveValidator at a new PS engagement" as a runbook** — per-workspace app SP, required UC grants, secret-scope entries, egress for Sentry/OTel. Converts NVIDIA tribal knowledge into 45-minute onboarding.

---

## 6. CI/CD Pipeline Design

Target: PR signal in under 10 minutes; catch Oracle/Delta/MySQL/Teradata regressions before customers; do not wake engineers with flaky 03:00 alerts.

### 6.1 Workflow Structure

LiveValidator is React + FastAPI + small Node auxiliaries. That maps to a **multi-workflow layout** rather than one 800-line YAML. 2026 consensus: many small focused workflows sharing code via reusable workflows (`workflow_call`) and composite actions.

Layout:

- `ci-pr.yml` — `pull_request` + `merge_group`: fast lint + unit + typed checks + SAST
- `ci-integration.yml` — path-filtered PR: DB matrix against ephemeral containers
- `ci-e2e.yml` — path-filtered PR: Playwright sharded (see §2.4, §5.7)
- `nightly.yml` — `schedule` + `workflow_dispatch`: full matrix incl. Teradata, mutation, long fuzzing
- `release.yml` — tag push: publish artifacts + PS installer
- `_reusable-node-setup.yml`, `_reusable-python-setup.yml`, `_reusable-db-matrix.yml`
- `pr-ephemeral.yml` + `pr-teardown.yml` — Databricks Apps per-PR (see §5.7)

### 6.2 Triggers (Critical: `merge_group`)

```yaml
# ci-pr.yml
on:
  pull_request:
    branches: [main, release/*]
  merge_group:            # REQUIRED once merge queue adopted
    types: [checks_requested]
  push:
    branches: [main]      # post-merge smoke
  workflow_dispatch:
```

Without `merge_group`, required status checks don't report on queued PRs and the queue fails.

### 6.3 Matrix Builds

Three orthogonal axes: database, OS, language runtime. Full cross-product = 48+ jobs; prune with `include`/`exclude`:

```yaml
strategy:
  fail-fast: false           # see all DB failures, not just first
  matrix:
    db: [mysql-8, oracle-free-23, delta-3]
    os: [ubuntu-latest]
    python: ["3.11", "3.12"]
    node: ["20", "22"]
    include:
      - { db: mysql-8, os: macos-latest, python: "3.12", node: "22" }
    exclude:
      - { db: delta-3, python: "3.11" }
```

`fail-fast: false` is essential — need to know whether other DBs also fail to distinguish driver bug from shared-logic bug. Teradata excluded: no free container → `nightly.yml`.

### 6.4 Caching Strategy

Cache hit rates drive pipeline cost more than any other factor. Cold cache PR ~12 min; warm ~3.5 min.

```yaml
# Node (pnpm)
- uses: pnpm/action-setup@v4
- uses: actions/setup-node@v4
  with:
    node-version: 22
    cache: pnpm
    cache-dependency-path: pnpm-lock.yaml

# Python (uv — 10× faster than pip)
- uses: astral-sh/setup-uv@v4
  with:
    enable-cache: true
    cache-dependency-glob: "uv.lock"
- run: uv sync --frozen

# Docker layer cache
- uses: docker/setup-buildx-action@v3
- uses: docker/build-push-action@v6
  with:
    cache-from: type=gha,scope=livevalidator-backend
    cache-to:   type=gha,scope=livevalidator-backend,mode=max

# Playwright browsers (~400 MB)
- uses: actions/cache@v4
  id: pw-cache
  with:
    path: ~/.cache/ms-playwright
    key: pw-${{ runner.os }}-${{ hashFiles('pnpm-lock.yaml') }}
- run: pnpm playwright install --with-deps
  if: steps.pw-cache.outputs.cache-hit != 'true'
```

### 6.5 Parallelism

- Playwright sharding with matrix (see §2.4).
- Vitest `--maxWorkers=2` on 2-vCPU GitHub runners; `--maxWorkers=75%` on 4-vCPU bigger runners.
- Split tests by duration once suite > 200 tests (`pytest-split --splits 4 --group $i`).

### 6.6 Pre-commit Hooks

For mixed JS + Python repo: **lefthook** (Go, single binary, parallel by default) as orchestrator, **pre-commit** (Python) for its richer Python tool catalog. Avoid husky (JS-only, slow on large repos).

```yaml
# lefthook.yml
pre-commit:
  parallel: true
  commands:
    eslint:
      glob: "*.{ts,tsx,js,jsx}"
      run: pnpm eslint --fix {staged_files}
      stage_fixed: true
    ruff:
      glob: "*.py"
      run: uv run ruff check --fix {staged_files}
      stage_fixed: true
    ruff-format:
      glob: "*.py"
      run: uv run ruff format {staged_files}
      stage_fixed: true
    typecheck:
      glob: "*.{ts,tsx}"
      run: pnpm tsc --noEmit
commit-msg:
  commands:
    commitlint:
      run: pnpm commitlint --edit {1}
```

Run same hooks in CI (`lefthook run pre-commit --all-files`) so bypasses still fail the gate.

### 6.7 Branch Protection + Merge Queue

Enable GitHub rulesets (2026 replacement for classic branch protection) on `main` and `release/*`:

- Require PR with ≥1 approving review, dismiss stale on push
- **One required status check: `summary`** — internally rolls up all required jobs. Lets you reshape pipeline without touching branch protection.
- Require branches up to date before merge
- Require linear history (rebase/squash only)
- Require signed commits via SSH or GPG
- Restrict who can push — release bot + repo admins only
- **Require merge queue** for `main`

```yaml
jobs:
  lint:        { ... }
  typecheck:   { ... }
  unit:        { needs: [lint, typecheck], ... }
  integration: { needs: unit, strategy: { matrix: { db: [mysql, oracle-free, delta] } }, ... }
  e2e:         { needs: unit, strategy: { matrix: { shard: [1,2,3,4] } }, ... }
  summary:
    needs: [unit, integration, e2e]
    if: always()
    steps:
      - run: |
          [ "${{ needs.unit.result }}" = "success" ] || exit 1
          [ "${{ needs.integration.result }}" = "success" ] || exit 1
          [ "${{ needs.e2e.result }}" = "success" ] || exit 1
```

### 6.8 PR Gates vs Nightly

Rule: PRs get everything fast and deterministic; nightly gets slow or flaky-prone.

**On every PR (target: <10 min):**

- Lint + typecheck (JS + Python)
- Unit tests (Vitest, pytest)
- Integration against MySQL + Oracle Free (2 DBs, not 4)
- Component tests + Playwright smoke (10–20 critical paths)
- Contract tests (Pact consumer verification)
- Coverage diff (Codecov, fail if delta < −1%)
- SAST (CodeQL, `pnpm audit --prod`, `uv pip audit`)
- Container build + Trivy scan (no push)
- **Ephemeral Databricks App per PR** (see §5.7)

**Nightly on `main` (budget: up to 90 min):**

- Full Playwright matrix × Chromium/Firefox/WebKit
- Teradata integration (licensed endpoint, concurrency serialized)
- Delta Lake at scale (large fixtures too heavy for PR)
- Mutation testing (Stryker, mutmut)
- Long fuzzing (Hypothesis/fast-check, 10-min budget per property)
- Bundle size + Lighthouse budgets
- Renovate dashboard dependency freshness

### 6.9 Test Reporting

- **Job summary**: counts, coverage delta, top-10 slowest, appended to `$GITHUB_STEP_SUMMARY`.
- **Playwright HTML report**: artifact on PRs (7-day); on `main` push to S3 keyed by commit SHA.
- **Coverage**: Codecov with OIDC tokenless auth. Patch ≥ 80%, project no worse than −1%.
- **Test history**: Allure (classic dashboard) or Testmo (Jira integration). For flakiness, Trunk Flaky Tests.

### 6.10 Flaky Test Handling

- In-framework retries: `retries: 2` on CI only in Playwright; `jest.retryTimes(1, { logErrorsBeforeRetry: true })` for component tests. **Don't retry unit tests** — fix them.
- **Trunk Flaky Tests** — AI-grouped detection, auto-quarantine on `main` only, Jira tickets assigned to author.
- Manual quarantine: `test.fixme('flaky, see LV-1234', ...)` with open ticket, weekly review.
- Dashboards: Datadog CI Visibility or Trunk's free tier.

### 6.11 Runners

- GitHub-hosted `ubuntu-latest` (2 vCPU / 7 GB): default. $0.008/min.
- GitHub-hosted larger (`ubuntu-latest-8-core` / 16 GB): E2E shards + Docker-heavy integration.
- macOS: one matrix cell for dev-laptop path.
- **Self-hosted via ARC on EKS**: once monthly spend > ~$2–3K, or long-lived Oracle image caches.

### 6.12 Secrets and Identity

GitHub Environments (`staging`, `production`, `customer-nvidia`) with protection rules. **OIDC federation to Databricks / AWS / GCP / Azure** — rotate zero keys:

```yaml
permissions:
  id-token: write
  contents: read
jobs:
  deploy:
    environment: production
    steps:
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::123456789012:role/gha-livevalidator-deploy
          aws-region: us-west-2
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target production
```

Customer DB passwords, Teradata credentials stay in per-Environment secrets, not repo-level.

### 6.13 Dependency Management

**Renovate > Dependabot** for multi-ecosystem repo: groups updates, handles uv/pnpm lockfiles natively, automerge patch-that-passes-CI:

```json5
// renovate.json
{
  "extends": ["config:recommended", ":semanticCommits"],
  "packageRules": [
    { "matchUpdateTypes": ["patch"], "automerge": true, "automergeType": "branch" },
    { "groupName": "react", "matchPackagePatterns": ["^react", "^@react"] },
    { "groupName": "playwright", "matchPackagePatterns": ["^@playwright"] }
  ],
  "timezone": "America/Los_Angeles",
  "schedule": ["before 6am on monday"]
}
```

### 6.14 Security

- **CodeQL** on JS/TS + Python, default pack + `security-extended`.
- `pnpm audit --prod` in PR; `pip-audit` (preferred over `safety` since PyPA took ownership).
- **Trivy** on every container image build, fail on HIGH+.
- **Secret scanning + push protection** enabled.
- **SBOM**: `actions/attest-build-provenance` to sign + attach SBOM.

### Recommendations for LiveValidator (CI/CD)

1. **Consolidate to 7-workflow layout + one composite action.** 40–50% YAML shrink, faster onboarding.
2. **GitHub native merge queue on `main`** with single `summary` required check rolling up lint/unit/integration/E2E smoke.
3. **Shard Playwright into 4 on PRs, 6 on nightly** with blob reporter + merge job.
4. **lefthook** as pre-commit orchestrator; `pre-commit` for Python tools. `lefthook run pre-commit --all-files` as first CI step.
5. **Move Teradata, mutation, WebKit/Firefox E2E to `nightly.yml`.** Slack notify on failure; auto-Jira if two nights fail.
6. **Trunk Flaky Tests with auto-quarantine on `main` only.** Target <1% flake rate.
7. **Renovate with grouped updates + patch automerge.** Retire Dependabot. ~60% fewer dep PRs.
8. **OIDC federation to Databricks + AWS.** Remove all long-lived cloud keys. Per-environment secret scopes.

---

## 7. Release Engineering & Observability

Frame: **the build succeeding is not evidence the release is safe.** Production telemetry, staged rollouts, and one-command rollback are what turn "I shipped" into "I know it's fine."

### 7.1 Feature Flags

Single highest-leverage tool for an anxious maintainer. Turns every risky change from a deploy event to a config event. For LiveValidator: enables per-customer overrides (NVIDIA gets a conservative flag set while an internal engagement gets the new validator).

| Tool | Model | Pricing 2026 | NVIDIA fit |
|---|---|---|---|
| LaunchDarkly | SaaS (self-host Relay) | Pro $12/seat/mo | Relay in customer VPC — complex |
| Split.io (Harness) | SaaS | ~$35/seat/mo | Same VPC problem |
| Statsig | SaaS, usage-based | Generous free | SaaS-first; internal dogfood only |
| **Unleash** | **OSS self-host + SaaS** | **Free self-host; Pro ~$80/mo** | **Best OSS fit** — mature, SDK evaluates locally |
| Flipt | OSS, Go binary | Free | Excellent for simple deploys |
| Flagsmith | OSS + SaaS | Free OSS | Good for flags + remote config |
| GrowthBook | OSS + SaaS | Free OSS | Overkill for flags-only |
| ConfigCat | SaaS | $99/mo | SaaS-only — disqualified |

**NVIDIA constraint:** data cannot leave their VPC. Disqualifies pure-SaaS. Narrows to **Unleash, Flipt, or UC-table-based**.

**Three-layer flag hierarchy** (resolved at startup):

1. **Environment variables** (`LIVEVALIDATOR_FLAG_NEW_ORACLE_DRIVER=true`) — baseline, zero deps.
2. **UC table `livevalidator.config.flags`** — per-workspace overrides. PS consultants flip via SQL.
3. **Optional Unleash** — for customers wanting a real dashboard. Docker image in customer VPC; SDK evaluates locally.

Schema: `(flag_name STRING, value STRING, environment STRING, updated_at TIMESTAMP)`.

Every flag site: `if flags.enabled("new_oracle_driver", default=False):`.

**Every new validator ships behind a flag for its first two minor versions. Default off. Flip on per-customer as confidence builds.**

### 7.2 Semantic Versioning

Semver 2.0 non-negotiable. **0.x has its own rules** — anything may change, but the ecosystem treats 0.x minor bumps as breaking. Path: **0.1 → 0.2 → 0.3 → … with one breaking change per minor, then 1.0 when NVIDIA runs a full quarter without incident.**

Customer pinning:

| Strategy | Example | Use case |
|---|---|---|
| Exact pin | `livevalidator==0.1.3` | PS engagements in production |
| Patch range | `~=0.1.3` | Dev/staging |
| Loose range | `>=0.1,<0.2` | Only if committed to 0.1 compat |
| Open | `>=0.1` | **Never** |

Breaking change policy for 0.x: minor bump required; deprecation precedes removal by one full minor; CHANGELOG `BREAKING CHANGE:` footer mandatory.

### 7.3 Release Automation: release-please

**release-please over semantic-release** for LiveValidator:

1. **PR-based review** — Release PR shows computed version, CHANGELOG, every commit. Dan clicks merge when ready.
2. **Conventional Commits flexibility** — graceful with imperfect commits.
3. **Multi-language** — `pyproject.toml` + `package.json` from one config.

```json
{
  "packages": {
    ".": {
      "release-type": "python",
      "package-name": "livevalidator",
      "changelog-sections": [
        {"type": "feat", "section": "Features"},
        {"type": "fix",  "section": "Bug Fixes"},
        {"type": "perf", "section": "Performance"},
        {"type": "docs", "section": "Documentation", "hidden": false}
      ],
      "bump-minor-pre-major": true
    }
  }
}
```

- `feat: add DuckDB backend` → minor bump (0.1.x → 0.2.0 pre-1.0)
- `fix: handle null in Oracle numeric` → patch bump
- `feat!: remove legacy Spark 2 support` or `BREAKING CHANGE:` footer → major bump post-1.0

### 7.4 Release Channels

| Channel | Version | Who | SLO |
|---|---|---|---|
| nightly | `0.2.0.dev20260422` | Internal only | None |
| rc | `0.2.0rc1` | Dogfood at Databricks | 48h soak |
| beta | `0.2.0b1` | Opt-in customers | Known issues documented |
| stable | `0.2.0` | Default PS | Full support |

**48-hour internal dogfood soak** on a Databricks workspace running synthetic validation before any stable tag. Single cheapest bug-catcher available.

### 7.5 Canary / Gradual Rollout

For a library/CLI, canary = **distribution decision**:

1. **Cohort rollout** — Cohort A (early adopters), B (PS willing to take v+1), C (NVIDIA). Roll A → B → C over 1–2 weeks. Publish schedule.
2. **Opt-in beta** — documented `beta-tester` tag.
3. **Feature-flag kill switches** — flip on regression, no redeploy.
4. **Dogfood first** — internal workspace runs nightly continuously.

### 7.6 Rollback

Writes to UC/Delta are durable. Preconditions:

- **Backward-compatible schema migrations** — new code reads old tables; old code reads new tables.
- **No destructive ops without `--confirm`** — `DROP`, `DELETE`, `TRUNCATE` require printed warning.
- **Idempotency** — re-running validation on same input produces same output.

```bash
livevalidator rollback --to 0.1.2
# pip install livevalidator==0.1.2 + revert UC config to prior snapshot
```

Tag prior config in UC (`livevalidator.config.snapshots`) on every release.

**State reconciliation** when v0.2.0 wrote bad rows:

- **Delta time-travel**: `RESTORE TABLE results TO VERSION AS OF N`.
- **Quarantine**: move suspect partitions to `results_quarantine`, re-run on good version.
- **Forward-fix**: ship 0.2.1 that repairs bad rows.

**Decide upfront per release:** reversible, or forward-fix only? Document in release notes.

### 7.7 Error Tracking

**Sentry self-hosted** (via Docker Compose) with **Relay** sidecar that scrubs PII before data leaves. Advanced Data Scrubbing on Business+; basic scrubbing free. Rollbar/Bugsnag smaller ecosystems; Datadog/Honeycomb APM-heavier.

**Recommendation:** self-hosted Sentry in customer VPC + opt-in for Databricks-internal aggregation.

```python
# Default: off
TELEMETRY_ENABLED = false
# If enabled, customer's internal Sentry:
TELEMETRY_DSN = "https://...@sentry.internal.nvidia.com/1"
# Optional sanitized phone-home to Databricks:
TELEMETRY_PHONE_HOME = false
```

Consent explicit, default off. Customers opt in during onboarding.

```python
import sentry_sdk

def scrub(event, hint):
    if "request" in event:
        event["request"].pop("data", None)
    for frame in event.get("exception", {}).get("values", [{}])[0].get("stacktrace", {}).get("frames", []):
        frame.pop("vars", None)  # never ship local variables
    return event

sentry_sdk.init(
    dsn=os.environ.get("LIVEVALIDATOR_SENTRY_DSN"),
    send_default_pii=False,
    attach_stacktrace=True,
    before_send=scrub,
    release=f"livevalidator@{__version__}",
    environment=os.environ.get("LIVEVALIDATOR_ENV", "production"),
    sample_rate=1.0,
    traces_sample_rate=0.1,
)
```

### 7.8 Structured Logging + OpenTelemetry

**structlog** + **OpenTelemetry** 2026 standard. For Databricks Apps, managed OTel exporter to UC (see §5.9).

```python
# Correlation ID on every log line, span, Sentry event
run_id = uuid.uuid4()
```

Phone-home policy: **explicit consent, aggregate only, documented schema.** Acceptable: version + OS + anonymized customer ID + aggregate error counts. Never: row data, table names, SQL, user IDs. Provide `--no-telemetry` and `LIVEVALIDATOR_DISABLE_TELEMETRY=1`.

### 7.9 Customer Pinning & LTS

Policy for 5–10 engagements:

- **Support latest minor + previous minor (N-1)**. `0.3.x` and `0.4.x` supported; `0.2.x` security fixes only.
- **Deprecation window: one minor.** Warn in 0.4, remove in 0.5.
- **Migration guide per breaking change** next to CHANGELOG.
- **One representative CI config per supported line.**

Deprecation warnings in CLI output on every invocation.

### 7.10 Security

- **SECURITY.md** with disclosure email, PGP key, 90-day coordinated disclosure.
- CVE process via GitHub Security Advisories.
- **SBOM** (CycloneDX default 2026) on every release. `cyclonedx-py`. Per PEP 770, Python ships SBOMs in wheels.
- **Signed releases**: cosign + Sigstore keyless via OIDC in GitHub Actions. PyPI Sigstore attestations.

```yaml
- uses: sigstore/gh-action-sigstore-python@v3
  with:
    inputs: dist/*.whl dist/*.tar.gz
- run: cyclonedx-py requirements -o sbom.cdx.json
- run: cosign sign-blob --yes sbom.cdx.json > sbom.cdx.json.sig
```

### 7.11 Communication

- **Release notes for PS consultants** — not commit log. Structure: What's new / What's fixed / What's deprecated / Action required. Two sentences max per item.
- **Breaking-change warnings in CLI** on first invocation: `DEPRECATION: --mode=legacy removed in 0.5. Switch to --mode=strict. See migration: <link>`.
- **Status page** (pinned GitHub issue or `status.livevalidator.dev`): current stable, latest beta, known issues, planned deprecations.
- **#livevalidator-announce Slack channel** in Databricks tenant, read-only for customers.

### Release Safety Checklist for v0.1.0 → v1.0

- [ ] SECURITY.md with disclosure process and contact
- [ ] CODEOWNERS configured; release branch requires 2 approvals
- [ ] CHANGELOG.md auto-generated by release-please; never hand-edited
- [ ] Conventional Commits enforced by pre-commit + CI check
- [ ] Every release tagged with signed tag (`git tag -s`)
- [ ] Every wheel signed via Sigstore; attestation uploaded to PyPI
- [ ] CycloneDX SBOM generated and attached to every GitHub release
- [ ] Feature flag scaffolding in place (env var → UC table → optional Unleash)
- [ ] Every new validator ships behind a default-off flag for ≥1 minor version
- [ ] 48-hour dogfood soak on internal Databricks workspace before stable tag
- [ ] Beta channel published on TestPyPI; ≥2 internal users on it
- [ ] One-command rollback documented and tested (`livevalidator rollback --to X.Y.Z`)
- [ ] Backward-compatible schema migrations verified against prior minor
- [ ] Sentry self-hosted deployment tested; PII scrubbing reviewed
- [ ] OpenTelemetry tracer emits spans for each validator; correlation IDs present
- [ ] `--no-telemetry` flag honored; documented in README + data-sheet
- [ ] Migration guide drafted for any breaking change, linked from CLI deprecation warning
- [ ] Release notes reviewed by one non-engineer (PS consultant) before publishing
- [ ] Status page / announce channel updated within 1 hour of release
- [ ] Post-release smoke test against NVIDIA-like reference environment before advertising availability
- [ ] Ephemeral PR app mechanism tested via a dry-run PR before first real deploy

---

## 8. Implementation Roadmap

Sequenced to front-load the highest-leverage items.

### Week 1 — Foundation

- [ ] **Day 1–2:** Add Testcontainers (MySQL + Oracle Free + Delta-Spark). One integration test per DB. Wire into GitHub Actions matrix. (§4)
- [ ] **Day 2–3:** Install fast-check (JS) and Hypothesis (Py). Write one property per validator module: `parse ∘ render = id`, `validate(x)` never throws, deterministic. Commit `.hypothesis/` + fast-check seeds. (§3)
- [ ] **Day 3–4:** Stand up golden-file SQL tests per (dialect × rule). `UPDATE_GOLDENS=1` regeneration workflow. (§3, §4)
- [ ] **Day 4–5:** Adopt release-please with Conventional Commits. Auto-CHANGELOG. Set up `pyproject.toml` + `package.json` config. (§7.3)

### Week 2 — Safety Nets

- [ ] **Day 6–7:** Three-layer feature flag system (env var → UC table → optional Unleash). Add `flags.enabled()` call sites around risky code paths. (§7.1)
- [ ] **Day 8:** Self-host Sentry (or point at customer's) with PII scrubbing + `before_send` hook. Set `environment=` tag. (§7.7)
- [ ] **Day 9–10:** Write one-command rollback (`livevalidator rollback --to X.Y.Z`). Document in runbook. Test it. (§7.6)

### Week 3 — Playwright + Databricks Apps

- [ ] **Day 11–12:** Switch Playwright to standard project structure (`setup` → `chromium` + `webkit` → `fullyParallel: true`). Pre-minted `storageState.json` for Databricks SSO. (§2, §5.4)
- [ ] **Day 13–14:** Build DABs with four targets (`dev`, `pr`, `staging`, `prod`). Deploy PR ephemeral apps on PR open, destroy on close. OIDC federation for CI. (§5.5, §5.6, §5.7)
- [ ] **Day 15:** 5–7 Playwright smoke tests running against PR apps. Health-probe warmup step. (§2, §5.9)

### Week 4 — Generative Testing

- [ ] **Day 16–17:** Differential tests across the four dialects for every rule. Parametrized pytest. (§3.7, §4)
- [ ] **Day 18–19:** Model-based testing on form state machine (`fc.commands` + `hypothesis.stateful`). (§3.2, §3.3)
- [ ] **Day 20:** Stryker incremental on PR, full nightly; mutmut on core validator packages nightly. Baseline mutation score. (§3.4)

### Week 5–6 — Polish

- [ ] Playwright MCP + Claude Code weekly exploratory sweep. Commit useful generated tests. (§2.11)
- [ ] HAR-replay corpus from real NVIDIA usage with consent + PII scrubbing. (§2.8)
- [ ] Chromatic at $149/mo with TurboSnap. (§1.5)
- [ ] Storybook 9 for validation UI primitives. Interaction tests + a11y addon. (§1.4)
- [ ] lefthook pre-commit orchestration (replace husky if present). (§6.6)
- [ ] Trunk Flaky Tests with auto-quarantine on `main` only. (§6.10)
- [ ] Renovate with grouped updates + patch automerge. (§6.13)

### Quarter Goals (Month 2–3)

- [ ] Nightly workflow: full Playwright matrix × browsers, Teradata integration, mutation testing, Atheris fuzz campaigns (4h/night with persisted corpus).
- [ ] Managed OTel telemetry in UC on staging + prod. Per-customer latency dashboards.
- [ ] Cohort rollout playbook (A → B → C over 1–2 weeks per release). Published schedule.
- [ ] 48-hour internal dogfood soak mandatory before stable tags.
- [ ] Cosign/Sigstore keyless signing + CycloneDX SBOM on every release.
- [ ] v1.0 release after a full quarter without NVIDIA incident.

### Year-Long Goals

- [ ] Mutation score >80% on core validator/SQL-gen modules.
- [ ] Flake rate <1%.
- [ ] PR signal <10 min wall-clock (unit + integration + smoke + PR app).
- [ ] Nightly under 90 min.
- [ ] Zero "silent" regressions reaching NVIDIA (all escaped bugs become regression tests in their appropriate tier).
- [ ] "Deploying LiveValidator at a new PS engagement" runbook — 45-minute onboarding.

---

## 9. Top-15 Actions Prioritized

Ranked by anxiety-reduction per hour of effort, tier-labeled (M = Must, S = Should, C = Could):

| # | Action | Tier | Effort | Section |
|---|--------|------|--------|---------|
| 1 | Ephemeral Databricks Apps per PR via DABs | M | ~1 day | §5.5, §5.7 |
| 2 | Three-layer feature flag system | M | ~4 hrs | §7.1 |
| 3 | Testcontainers MySQL + Oracle + Delta per PR | M | ~1–2 days | §4 |
| 4 | fast-check + Hypothesis properties on validators | M | ~1 day | §3.2, §3.3 |
| 5 | Differential tests across dialects | M | ~1 day | §3.7 |
| 6 | Pre-minted Playwright storageState for SSO | M | ~2 hrs | §5.4 |
| 7 | Self-hosted Sentry with PII scrubbing | M | ~3 hrs | §7.7 |
| 8 | release-please + Conventional Commits | M | ~2 hrs | §7.3 |
| 9 | 48-hour internal dogfood soak | M | ~2 hrs | §7.4 |
| 10 | Golden-file SQL tests per (dialect × rule) | S | ~1 day | §3.6, §4 |
| 11 | Playwright MCP weekly exploratory sweep | S | ~4 hrs | §2.11 |
| 12 | Managed OTel telemetry to UC | S | ~3 hrs | §5.9, §7.8 |
| 13 | Stryker + mutmut nightly | S | ~1 day | §3.4 |
| 14 | Chromatic at $149/mo with TurboSnap | S | ~4 hrs | §1.5 |
| 15 | Atheris fuzz campaigns nightly | C | ~1 day | §3.5 |

**Minimum viable v0.1.0 in 1 week** (if time-pressured): items 1, 2, 3, 6, 7, 8, 9. Covers the majority of bug-escape risk.

---

## 10. Appendix

### Tool Reference

| Category | Tool | Version | URL |
|---|---|---|---|
| Unit/component | Vitest | 4.1.4 | https://vitest.dev/ |
| Component | React Testing Library | 16.x | https://testing-library.com/ |
| Component | user-event | 14.x | https://testing-library.com/docs/user-event/intro/ |
| API mock | MSW | 2.13.4 | https://mswjs.io/ |
| Storybook | Storybook | 9.1.x | https://storybook.js.org/ |
| Visual regression | Chromatic | SaaS | https://www.chromatic.com/ |
| Visual regression (free) | Playwright `toHaveScreenshot` | 1.59.1 | https://playwright.dev/docs/test-snapshots |
| A11y | jest-axe / vitest-axe | 9.x / 0.x | https://github.com/NickColley/jest-axe |
| A11y standard | WCAG | 2.2 | https://www.w3.org/TR/WCAG22/ |
| E2E | Playwright | 1.59.1 | https://playwright.dev/ |
| Playwright MCP | microsoft/playwright-mcp | latest | https://github.com/microsoft/playwright-mcp |
| Property-based (JS) | fast-check | 4.5.3 | https://fast-check.dev/ |
| Property-based (Py) | Hypothesis | 6.152.x | https://hypothesis.readthedocs.io/ |
| Mutation (JS) | Stryker | 9.x | https://stryker-mutator.io/ |
| Mutation (Py) | mutmut | latest | https://github.com/boxed/mutmut |
| Fuzz (Py) | Atheris | latest | https://github.com/google/atheris |
| Multi-dialect SQL | sqlglot | latest | https://github.com/tobymao/sqlglot |
| Containers | testcontainers-python | 4.x | https://github.com/testcontainers/testcontainers-python |
| Oracle container | gvenzl/oracle-free | 23.7 | https://github.com/gvenzl/oci-oracle-free |
| Delta local | delta-spark | 4.2.0 | https://pypi.org/project/delta-spark/ |
| DABs | Databricks CLI | latest | https://docs.databricks.com/dev-tools/bundles/ |
| Feature flags | Unleash | latest | https://www.getunleash.io/ |
| Release automation | release-please | latest | https://github.com/googleapis/release-please |
| Error tracking | Sentry self-hosted | 25.x | https://develop.sentry.dev/self-hosted/ |
| Observability | OpenTelemetry | 2026 standard | https://opentelemetry.io/ |
| Pre-commit (JS+Py) | lefthook | latest | https://github.com/evilmartians/lefthook |
| CI | GitHub Actions | — | https://docs.github.com/en/actions |
| CI flake mgmt | Trunk Flaky Tests | latest | https://trunk.io/flaky-tests |
| Dependency bot | Renovate | latest | https://docs.renovatebot.com/ |
| Scanner | CodeQL | latest | https://codeql.github.com/ |
| Signing | Sigstore + cosign | 2.x | https://www.sigstore.dev/ |
| SBOM | CycloneDX | latest | https://cyclonedx.org/ |

### Example `package.json` Scripts

```json
{
  "scripts": {
    "test": "vitest",
    "test:ui": "vitest --ui",
    "test:coverage": "vitest run --coverage",
    "test:mutation": "stryker run",
    "test:e2e": "playwright test",
    "test:e2e:ui": "playwright test --ui",
    "test:e2e:codegen": "playwright codegen",
    "test:visual": "chromatic --exit-zero-on-changes",
    "storybook": "storybook dev -p 6006",
    "test:storybook": "test-storybook --coverage",
    "lint": "eslint .",
    "typecheck": "tsc --noEmit"
  }
}
```

### Example `pyproject.toml` Excerpt

```toml
[project]
name = "livevalidator"
dynamic = ["version"]
requires-python = ">=3.11"

[tool.uv]
dev-dependencies = [
  "pytest>=8.0",
  "pytest-asyncio",
  "pytest-regressions",
  "hypothesis>=6.152",
  "mutmut>=3.0",
  "testcontainers[oracle-free,mysql]>=4.0",
  "sqlglot>=26.0",
  "faker>=30.0",
  "atheris>=2.3",
  "opentelemetry-distro",
  "opentelemetry-exporter-otlp-proto-grpc",
  "opentelemetry-instrumentation-fastapi",
  "sentry-sdk>=2.0",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra --strict-markers --strict-config"
markers = [
  "slow: marks tests as slow (select with '-m slow')",
  "nightly: only runs in nightly pipeline",
  "teradata: requires Teradata endpoint",
]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "B", "UP", "SIM", "ANN"]
```

### References

- Databricks Apps docs: https://docs.databricks.com/dev-tools/databricks-apps/
- Databricks Apps Cookbook: https://apps-cookbook.dev/
- Databricks Asset Bundles docs: https://docs.databricks.com/dev-tools/bundles/
- Playwright docs: https://playwright.dev/docs/intro
- Testcontainers docs: https://testcontainers.com/
- sqlglot docs: https://sqlglot.com/
- fast-check docs: https://fast-check.dev/
- Hypothesis docs: https://hypothesis.readthedocs.io/
- release-please docs: https://github.com/googleapis/release-please
- Unleash docs: https://docs.getunleash.io/
- Sentry self-hosted docs: https://develop.sentry.dev/self-hosted/
- OpenTelemetry Python: https://opentelemetry.io/docs/languages/python/
- WCAG 2.2: https://www.w3.org/TR/WCAG22/
- Property-Based Testing in Practice (ICSE '24): https://dl.acm.org/doi/10.1145/3597503.3639581

---

---

## 11. Codebase Reality Check & Revised Plan

*This section was added after analyzing the actual LiveValidator repository at https://github.com/databricks-solutions/livevalidator. It reconciles the draft plan above with the as-built architecture. Where earlier sections conflict with this one, this section wins.*

### 11.1 What LiveValidator Actually Is

**Purpose:** three-tier validation between heterogeneous database systems — schema match, row-count match, row-level match (via EXCEPT ALL set comparison or primary-key-based). Results in 7-day history UI + UC Delta archive.

**Supported source systems** (per `README.md` and `jobs/data_reader.py`):

- **Databricks / Delta Lake** (native via Spark)
- **Netezza** (IBM)
- **Teradata** (via `teradatasql` + JDBC jar)
- **SQL Server** (with partition detection for parallel reads)
- **MySQL** (via JDBC)
- **PostgreSQL** (via JDBC, not to be confused with LakeBase control plane)
- **Snowflake** (via JDBC)
- **Custom JDBC** — any JDBC driver user supplies (including **Oracle** via `ojdbc11` jar; JDBC jars must be UC-allowlisted)

Actual system list is **7+ engines, not 4**. The Oracle/Delta/MySQL/Teradata framing from §4 was a simplification.

**Architecture (confirmed from repo):**

```
┌──────────────────────────────────────────────────────────────────┐
│                     Databricks Apps runtime                        │
│  ┌─────────────────┐     ┌──────────────────────────────────┐     │
│  │   React SPA     │ ←→  │        FastAPI backend           │     │
│  │  (Vite build,   │     │  (uvicorn, async, 4 workers)     │     │
│  │  Tailwind,      │     │    •  x-forwarded-email auth     │     │
│  │  Recharts)      │     │    •  asyncpg connection pool    │     │
│  └─────────────────┘     │    •  pydantic models            │     │
│                           │    •  raw SQL (no ORM)           │     │
│                           └──────────────┬───────────────────┘     │
└──────────────────────────────────────────┼─────────────────────────┘
                                           │
                    ┌──────────────────────┴─────────────────────┐
                    │                                             │
                    ▼                                             ▼
        ┌───────────────────────┐             ┌─────────────────────────┐
        │  LakeBase (Postgres)  │             │    Databricks Workflow  │
        │  control plane:       │             │                         │
        │  •  queue (SKIP       │             │  •  run_validation job  │
        │     LOCKED atomic     │◄────────────│     (serverless + clus- │
        │     claim)            │             │     ter variants)       │
        │  •  configuration     │             │  •  test-connection job │
        │  •  7-day history     │             │  •  fetch-lineage job   │
        │  •  optimistic        │             │  •  job-sentinel worker │
        │     locking (version) │─────────────►     (polls queue,       │
        └───────────────────────┘             │     SKIP LOCKED)        │
                                              └───────────┬─────────────┘
                                                          │
                                                          ▼
                                    ┌──────────────────────────────────┐
                                    │  Source systems (JDBC/native)    │
                                    │  Netezza · Teradata · SQL Server │
                                    │  MySQL · Postgres · Snowflake    │
                                    │  Custom JDBC · Databricks/Delta  │
                                    └──────────────────────────────────┘
```

**Key implementation facts:**

- Backend uses **raw SQL + `asyncpg`** — no ORM. Pydantic is API-input validation only.
- Jobs (`jobs/*.py`) are **Databricks notebooks** prefixed with `# Databricks notebook source`. Rely on `dbutils`, `spark` globals.
- Frontend `dist/` is **committed to the repo** — production build checked in.
- Version mismatch: `pyproject.toml` says `0.1.0`; `src/app/pyproject.toml` says `0.0.1`.
- No CHANGELOG.md, SECURITY.md, release automation, pre-commit hooks, or structured logging.
- **Contributing.md admits:** *"Tests are designed to run without Spark. Only pure Python logic is tested; Spark-dependent code is tested manually on Databricks."*

### 11.2 Current Test Surface

**Backend (`tests/backend/` — 19 files):**

- Service-level pytest with `MockDBSession` (custom class imitating asyncpg API via result queues and call logs).
- `MockDatabricksService` for SDK calls.
- 65% coverage threshold on `src/app/backend/services/` + `utils.py` in CI.
- `pytest-asyncio 0.23` with `asyncio_mode = "auto"`.

**Jobs (`tests/jobs/` — 4 files):**

- `sys.modules["pyspark"] = MagicMock()` to mock PySpark entirely.
- Only pure-Python paths exercised.

**Frontend: zero tests.** No `vitest.config`, no `jest.config`, no `playwright.config`, no `@testing-library/*` in `package.json`.

**No property-based tests. No mutation tests. No fuzz tests. No golden-file SQL tests.** Those are all greenfield.

**CI (`.github/workflows/ci.yml`):** single workflow with `lint` (ruff check/format on `src/app/backend`) + `test` (pytest with coverage). No frontend build, no Playwright, no integration against real DBs, no DABs deploy per PR.

### 11.3 Revised Headline Recommendations (Delta to §1 Exec Summary)

Given the actual state, the priority reshuffles:

| Rank | Action | Delta from original plan |
|:----:|--------|--------------------------|
| 1 | **Replace `MockDBSession` with `testcontainers-python[postgres]` singleton fixture** — load `src/app/backend/sql/ddl.sql` + `grants.sql` once per session; each test gets a transaction it rolls back. | **New #1.** Higher leverage than adding Oracle/MySQL containers because *every* backend service test depends on this, and the mocks don't exercise SKIP LOCKED / optimistic locking correctly. |
| 2 | **Ephemeral Databricks Apps per PR via DABs** with a new `pr` target layered onto `databricks.yml.example`. | Same as before, but builds on the existing example file rather than inventing a new one. |
| 3 | **Bootstrap frontend test stack from zero** — Vitest + RTL + MSW + Playwright. There is nothing to migrate; everything is greenfield. | Reframes §1 and §2: not "migrate Jest → Vitest", but "install the stack for the first time." |
| 4 | **Test the type-transformation `exec()` path** — AST validation at save, optional sandbox, property-based tests of user-supplied functions. | **New, not in original plan.** This is LV's largest latent security + correctness hot spot. |
| 5 | **Testcontainers for the realistic subset**: Postgres (LakeBase mirror) + MySQL + SQL Server + Delta-Spark per PR. Oracle, Netezza, Snowflake, Teradata have no free CI-friendly containers — use sqlglot parse-only + dialect-specific dialect adapters. | Expands §4 to 7+ systems and is honest about the non-containerizable ones. |
| 6 | **Three-layer feature flag system** — still right. NVIDIA constraint still applies. | Unchanged from §7.1. |
| 7 | **Differential tests across dialects** using sqlglot transpile + Testcontainers where available + sqlglot parse elsewhere. | Still high-leverage; now applied to 7 dialects via sqlglot. |
| 8 | **Pre-minted Playwright `storageState`** for Databricks SSO. | Unchanged from §5.4. |
| 9 | **Self-host Sentry + structured logging + managed OTel** — today LV uses only `print()`. | Unchanged from §7.7–§7.8. |
| 10 | **release-please + version unification** (resolve the `0.1.0` vs `0.0.1` split), add `CHANGELOG.md`, `SECURITY.md`. | New emphasis: version unification. |
| 11 | **Spark-notebook testing strategy** — extract Spark logic from notebooks into plain Python modules; run Databricks Connect in CI nightly for the residue. | **New, not in original plan.** Directly addresses contributing.md's admission. |
| 12 | **Stop committing `src/app/frontend/dist/`** — build in CI and ship as bundle artifact; adds `.gitignore` entry and a CI build step. | **New.** Removes a diff-noise source that masks genuine frontend changes. |

### 11.4 LakeBase / Postgres Control-Plane Testing (new, replaces §4.2's Postgres stub)

LakeBase is Databricks' managed Postgres. LiveValidator's *control plane* — queue, configuration, history — lives here. The tool uses raw SQL + `asyncpg`, not SQLAlchemy, so every path depends on **exact Postgres behavior**. `MockDBSession` cannot test:

- `SELECT ... FOR UPDATE SKIP LOCKED` (triggers service — atomic queue claim)
- Optimistic-locking `UPDATE ... WHERE version = $n` version conflicts
- Default privileges cascading to sequences (`ALTER DEFAULT PRIVILEGES IN SCHEMA control GRANT USAGE, SELECT ON SEQUENCES TO apprunner;` — setup step from README)
- Delete-cascading foreign-key behavior
- Unique-constraint race conditions between concurrent POSTs
- Postgres `JSONB` vs `TEXT` semantics used in `column_overrides` and `extra_replace_regex`

**Recommended pattern:** replace `MockDBSession` with a **session-scoped `testcontainers-python[postgres]` fixture** that boots once, runs `ddl.sql` + `grants.sql`, and hands every test a short-lived transaction:

```python
# tests/backend/conftest.py
import asyncpg
import pathlib
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

SQL_DIR = pathlib.Path(__file__).parents[2] / "src/app/backend/sql"

@pytest_asyncio.fixture(scope="session")
async def pg():
    with PostgresContainer("postgres:16", username="apprunner",
                           password="apprunner", dbname="livevalidator") as c:
        dsn = c.get_connection_url().replace("postgresql+psycopg2", "postgresql")
        pool = await asyncpg.create_pool(dsn)
        async with pool.acquire() as conn:
            for script in ("ddl.sql", "grants.sql"):
                await conn.execute((SQL_DIR / script).read_text())
        yield pool
        await pool.close()

@pytest_asyncio.fixture
async def db(pg):
    """Per-test transaction, auto-rolled-back."""
    async with pg.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            yield conn
        finally:
            await tx.rollback()
```

Each service test then exercises **real Postgres behavior**. The 19 service tests move from "the service issued the SQL I expected" to "the service produced the correct database state." Coverage stays the same but **semantics are verified**.

**Concurrency tests** — new, critical for the queue:

```python
import asyncio, pytest

@pytest.mark.asyncio
async def test_skip_locked_prevents_double_claim(pg):
    """Two workers must never claim the same queued trigger."""
    async with pg.acquire() as conn:
        await conn.execute("""
          INSERT INTO control.triggers (entity_id, status, priority)
          VALUES ('test-1', 'queued', 0)""")

    async def claim_next():
        async with pg.acquire() as conn:
            return await conn.fetchrow("""
              SELECT * FROM control.triggers
              WHERE status = 'queued'
              ORDER BY priority DESC, created_at ASC
              FOR UPDATE SKIP LOCKED
              LIMIT 1""")

    claim_a, claim_b = await asyncio.gather(claim_next(), claim_next())
    assert (claim_a is None) ^ (claim_b is None), \
           "exactly one worker should win the race"
```

```python
@pytest.mark.asyncio
async def test_optimistic_locking_rejects_stale_update(db):
    """Update with a stale version field must fail."""
    await db.execute("""INSERT INTO control.tables (id, name, version)
                        VALUES ('t1', 'customers', 1)""")
    r = await db.execute("""UPDATE control.tables SET name = 'renamed', version = 2
                            WHERE id = 't1' AND version = 99""")
    assert r == "UPDATE 0"                 # no rows updated — version conflict
```

**This single change** (mock → container) **is the highest-leverage improvement to the backend test suite.** It converts 19 services' worth of tests from "checks that we built the SQL string we intended" to "checks that the database produced the right result." Everything you currently trust `MockDBSession` for, you'll trust more under Postgres.

Trade-off: CI gets ~8–10 seconds slower per job for the container boot (singleton mitigates). Worth it.

### 11.5 Per-System Source Testing (revised §4.2)

Realistic options for each of the seven+ source systems:

| System | Docker container (2026) | CI tier | Fallback if no container |
|--------|-------------------------|---------|--------------------------|
| **Databricks / Delta** | `delta-spark` 4.2.0 in-process (Spark 4.x) | Per-PR | Databricks Connect to shared dev workspace (nightly) |
| **MySQL** | `mysql:8` — <10s startup | Per-PR | — (container exists) |
| **PostgreSQL** (as a *source*, separate from LakeBase) | `postgres:16` — <10s | Per-PR | — |
| **SQL Server** | `mcr.microsoft.com/mssql/server:2022-latest` — ~60s, ~2 GB RAM | Per-PR | Azure SQL Edge image for ARM |
| **Oracle** (via custom JDBC) | `gvenzl/oracle-free:23.7-slim-faststart` — ~60–90s | Per-PR optional | sqlglot Oracle dialect parse-only |
| **Netezza** | **No Docker image available.** Commercial IBM Netezza Performance Server only. Postgres is a *close* dialect cousin but not identical. | Nightly / external endpoint | sqlglot Netezza dialect parse-only; community `netezza_emulator`-style test harness via Postgres with function stubs |
| **Teradata** | No container for CI use. Vantage Express VM (15 GB, VMware-only) for dev. Vantage Cloud Lake dev endpoint for nightly. | Nightly / external | sqlglot Teradata parse-only per-PR |
| **Snowflake** | **No Docker container.** Free trial account (rate-limited) or mocked JDBC driver. | Nightly / external | sqlglot Snowflake dialect parse-only per-PR + [`snowflake-connector-python`'s fakes](https://github.com/snowflakedb/snowflake-connector-python) |

**Sqlglot is load-bearing here.** The four systems without CI-friendly containers (Netezza, Teradata, Snowflake; and Oracle if you skip the container) still participate in tests via sqlglot's dialect-aware parser: generated SQL is parsed against the target dialect's grammar, pin any observed output to a golden file, and cross-dialect differential tests assert semantic equivalence within a canonical AST.

```python
# sqlglot-based parse validation as a universal safety net
import pytest, sqlglot

@pytest.mark.parametrize("dialect", [
    "databricks", "netezza", "teradata", "tsql",
    "mysql", "postgres", "snowflake", "oracle",
])
def test_generated_sql_parses_in_target_dialect(dialect):
    sql = generate_validation_sql(rule="not_null", column="email", dialect=dialect)
    sqlglot.parse_one(sql, read=dialect)   # raises if unparseable
```

Also add **dialect-specific known-divergence tests** — document where Netezza's `'' IS NULL` semantics differ from Postgres's, Teradata's `QUALIFY` from standard SQL's windowed `HAVING`, etc.

### 11.6 Spark / Notebook Job Testing (new section)

The files in `jobs/` are Databricks notebooks disguised as Python files. They begin with `# Databricks notebook source` and use `dbutils`, `spark`, and secret-scoped connections as globals. Current tests mock the pyspark module entirely — they verify none of the transformation logic. Contributing.md admits this.

**Three-phase migration path:**

1. **Refactor:** move every pure-Python helper out of the notebooks and into plain modules under `src/app/jobs/` (a new directory), leaving the notebook as a thin driver that imports and orchestrates. Pure helpers then become testable with vanilla pytest.

   Before (in `jobs/run_validation.py`):
   ```python
   # Databricks notebook source
   # MAGIC %md ## Validation runner
   rows = spark.read.jdbc(url, table, properties=props).count()
   ```

   After (in `jobs/run_validation.py`):
   ```python
   # Databricks notebook source
   from livevalidator_jobs.readers import count_rows
   rows = count_rows(spark, url, table, props)
   ```

   And in `src/app/jobs/readers.py`:
   ```python
   def count_rows(spark, url, table, props):
       return spark.read.jdbc(url, table, properties=props).count()
   ```

   This makes ~60% of the job code regular-pytest-testable.

2. **Databricks Connect in CI (nightly):** for the Spark-dependent helpers, use Databricks Connect 15.x+ against a shared dev workspace. A nightly workflow runs the full job against a tiny synthetic dataset:

   ```yaml
   # .github/workflows/nightly-spark.yml
   - name: Databricks Connect tests
     env:
       DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
       DATABRICKS_TOKEN: ${{ secrets.SPARK_CI_TOKEN }}
       DATABRICKS_CLUSTER_ID: ${{ vars.CI_CLUSTER_ID }}
     run: uv run pytest tests/jobs_spark -v --timeout=600
   ```

3. **Ephemeral PR-app integration smoke:** when the ephemeral Databricks App deploys for a PR (see §5.5), include a smoke-test step that submits a single validation run through the real job workflow against a fixture table in the test UC schema, then asserts the result row in history. This catches integration regressions that unit-test refactoring alone can't.

**Stop mocking `sys.modules["pyspark"]`.** Replace with either: (a) the refactored plain-Python helpers that don't need Spark, or (b) Databricks Connect against a real Spark context. The magic-mock pattern hides more bugs than it catches.

### 11.7 Type Transformation `exec()` — Security + Property-Based Testing (new section)

**The feature:** users paste Python functions into the UI; those functions are stored as text in Postgres and `exec()`'d at runtime inside the Databricks job to produce SQL expressions for per-system type transformation. Example from README:

```python
def transform_columns(column_name: str, data_type: str) -> str:
    if 'CHAR' in data_type:
        return f"RTRIM({column_name})"
    if data_type.startswith('NUMERIC'):
        return column_name
    return f"CAST({column_name} AS VARCHAR(250))"
```

**The risk surface:**

- The job's execution context has full `dbutils`, `spark`, network egress, and service-principal credentials. `import os; os.system(...)` in a saved transformation runs with those privileges.
- Malformed functions crash validation jobs silently.
- Non-deterministic or slow functions can wedge the queue.
- No current tests cover what happens when a user's function misbehaves.

**Recommended three-layer defense:**

#### 11.7.1 AST validation at save time

```python
import ast

ALLOWED_IMPORTS = {"re", "typing", "decimal"}
FORBIDDEN_NAMES = {"exec", "eval", "__import__", "open", "compile",
                   "globals", "locals", "getattr", "setattr"}

def validate_user_function(source: str) -> list[str]:
    errors: list[str] = []
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return [f"Syntax error: {e}"]

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [n.name.split(".")[0] for n in node.names]
            bad = [n for n in names if n not in ALLOWED_IMPORTS]
            if bad:
                errors.append(f"Forbidden import(s): {bad}")
        if isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            errors.append(f"Forbidden identifier: {node.id}")
        if isinstance(node, ast.Attribute):
            if any(p in ast.dump(node) for p in ("os.system", "subprocess")):
                errors.append(f"Forbidden attribute reference in: {ast.unparse(node)}")

    # Must define exactly one function named transform_columns
    funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
    if len(funcs) != 1 or funcs[0].name != "transform_columns":
        errors.append("Must define exactly one function `transform_columns`")
    return errors
```

Save endpoint returns the error list as 400 before persisting.

#### 11.7.2 Optional sandbox at exec time

`RestrictedPython` (Zope's library) compiles AST into a restricted bytecode subset that blocks dangerous operations at runtime. Adds ~10–20× execution overhead, acceptable because transformations run once per column:

```python
from RestrictedPython import compile_restricted, safe_globals, limited_builtins

def exec_transformation_safe(source: str, column_name: str, data_type: str) -> str:
    byte_code = compile_restricted(source, filename="<user>", mode="exec")
    ns = {**safe_globals, "__builtins__": limited_builtins}
    exec(byte_code, ns)
    return ns["transform_columns"](column_name, data_type)
```

Stronger option: run user code in a **WASM sandbox** (Wasmtime-py) or an isolated Databricks Jobs task with no service-principal token. More plumbing; better isolation.

#### 11.7.3 Property-based testing of the exec machinery

```python
import string
import hypothesis.strategies as st
from hypothesis import given, settings

identifiers = st.from_regex(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
data_types = st.sampled_from([
    "VARCHAR(250)", "CHAR(10)", "NUMERIC(18,2)", "DECIMAL(5,0)",
    "INT", "BIGINT", "TIMESTAMP", "DATE", "BOOLEAN", "BYTES",
])

SAFE_DEFAULTS = [
    # ... the library of default transformations shipped with LV
]

@given(source=st.sampled_from(SAFE_DEFAULTS),
       col=identifiers, dt=data_types)
@settings(max_examples=500)
def test_default_transformations_never_crash(source, col, dt):
    result = exec_transformation_safe(source, col, dt)
    assert isinstance(result, str)
    assert col in result or result == col

@given(source=st.text(max_size=2000), col=identifiers, dt=data_types)
@settings(max_examples=200)
def test_user_code_is_safely_bounded(source, col, dt):
    """Any user input — even malicious — must either validate-reject or exec-safely."""
    errors = validate_user_function(source)
    if errors:
        return  # rejected at save time — safe
    try:
        result = exec_transformation_safe(source, col, dt)
        assert isinstance(result, str), f"non-string return: {type(result)}"
        assert len(result) < 100_000, "implausibly large output"
    except (SyntaxError, TypeError, NameError, AttributeError):
        pass  # acceptable — user function can raise; caller handles
```

Together, these three layers turn a "someone will eventually type `os.system`" risk into a bounded, tested, reviewed surface.

### 11.8 Frontend Bootstrap from Zero (delta to §1 and §2)

Frontend test state as of today: **nothing installed, no config, no tests.** §1 described a rich React-testing stack; the practical first step is to **install the minimum viable stack** and grow it:

```bash
cd src/app/frontend
npm install --save-dev \
  vitest@^4 \
  @vitest/coverage-v8 \
  @testing-library/react@^16 \
  @testing-library/user-event@^14 \
  @testing-library/jest-dom \
  jsdom \
  msw@^2 \
  @playwright/test@^1.59 \
  vitest-axe
```

```json
// src/app/frontend/package.json — scripts
{
  "scripts": {
    "test": "vitest",
    "test:coverage": "vitest run --coverage",
    "test:e2e": "playwright test",
    "test:e2e:codegen": "playwright codegen"
  }
}
```

```typescript
// src/app/frontend/vitest.config.ts  (new file)
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    globals: true,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html', 'lcov'],
      thresholds: { lines: 60, functions: 60, branches: 55, statements: 60 },
    },
  },
})
```

Start with coverage thresholds at 60% (not 80%) to establish a baseline that won't block PRs for six months while the team writes tests. Ratchet up 5% per quarter.

**Priority order for the first 10 frontend tests:**

1. Systems view — register a Databricks system form, validate the "Test Connection" button state.
2. Tables view — CSV bulk upload happy path + malformed CSV error.
3. Queue view — polling state transitions (queued → running → done).
4. Validation History — filter by tag (AND logic), filter by time range.
5. Setup Wizard — the multi-step onboarding that currently exists (per README).
6. Type Mappings — the Python-function editor, including the save-blocked-on-AST-violation path.
7. Schedules — cron input validation, flight-plan next-run preview.
8. Dashboards — stats rendering with zero data + large data.
9. Error banners — network 500, session timeout, optimistic-lock version conflict dialog.
10. Navigation and deep-link routing.

And **three Playwright smoke journeys** against an ephemeral PR app:

1. Authenticated user lands on dashboard (session valid).
2. User creates a system → connection test → saves.
3. User triggers a validation → sees it progress through queue → result in history.

### 11.9 CI Workflow Evolution (delta to §6)

Current workflow: `/.github/workflows/ci.yml` with two jobs (lint, test). §6 prescribed a seven-workflow layout. **Migrate incrementally**:

- **Week 1** — add a third job to `ci.yml`: `test-integration` with testcontainers-postgres singleton. Move the MockDBSession → real-postgres conversion behind this job.
- **Week 2** — add a `test-frontend` job that builds with Vite and runs `vitest run`. Fails if frontend tests drop below 60% coverage.
- **Week 3** — add a `test-e2e` job triggered by label `e2e` on PRs, deploying an ephemeral PR app via DABs and running 3–5 Playwright smokes.
- **Week 4** — split `ci.yml` into `ci-pr.yml` (fast gate) + `ci-integration.yml` (path-filtered) + `ci-e2e.yml` (label-gated). Wire up merge queue with a single `summary` required check.
- **Week 5** — add `nightly.yml` with Databricks Connect Spark tests, Teradata/Snowflake endpoint tests, mutation tests.
- **Week 6** — add `release.yml` triggered on tag, using release-please to auto-generate versions and CHANGELOG.

The existing `ci.yml` structure (lint + test) stays as the "fast PR gate" inside `ci-pr.yml` — don't rewrite; just split and extend.

### 11.10 Auth Pattern Correction (delta to §5)

§5 used `X-Forwarded-User` as the canonical identity header. LiveValidator's actual middleware (confirmed in `src/app/backend/app.py`) reads `x-forwarded-email` (lowercase). Both are injected by the Databricks Apps reverse proxy; use whichever the backend already reads so tests exercise the real path. Updated test header stub:

```python
@pytest.fixture
def authed_client():
    c = TestClient(app)
    c.headers.update({
        "x-forwarded-email":         "ci@livevalidator.test",
        "x-forwarded-user":          "u-test-123",
        "x-forwarded-access-token":  "dummy-local-token",
        "x-request-id":              "test-req-1",
    })
    return c
```

### 11.11 Committed `dist/` — Fix and Delete (new)

`src/app/frontend/dist/` is a production build committed to git. This:

- Inflates the repo and every clone.
- Masks whether a PR actually changed frontend code (the built artifact diff drowns the source diff).
- Risks deploying stale builds if the committer forgot to run `npm run build`.
- Prevents tooling (Renovate, Dependabot) from trusting the source as authoritative.

**Fix:** add `src/app/frontend/dist/` to `.gitignore`, delete the tree, and add a **frontend build step to the deploy workflow** that runs `npm ci && npm run build` before `databricks bundle deploy`. Example:

```yaml
- name: Build frontend
  working-directory: src/app/frontend
  run: |
    npm ci
    npm run build
- name: Deploy bundle
  run: databricks bundle deploy -t ${{ matrix.target }}
```

### 11.12 Observability — Current is `print()`; Close the Gap (delta to §7)

Today's observability is `print()` calls captured by Databricks notebook logs. Three concrete migrations:

1. **Replace `print()` with `structlog`** across both backend and jobs. Single diff, enormous grep-ability win.

   ```python
   # Before
   print(f"[WARN] Parallel read failed: {e}")

   # After
   log = structlog.get_logger()
   log.warning("parallel_read_failed", error=str(e), table=table_name)
   ```

2. **Add `sentry_sdk.init(...)`** with PII scrubbing as in §7.7. Start with opt-in per customer workspace.

3. **Enable Databricks Apps managed OTel** (§5.9) on staging and prod. Validation jobs already emit timing data that becomes first-class spans when wrapped:

   ```python
   from opentelemetry import trace
   tracer = trace.get_tracer("livevalidator.jobs")

   with tracer.start_as_current_span("validate_table") as span:
       span.set_attribute("source.kind", source_kind)
       span.set_attribute("rows.source", src_count)
       span.set_attribute("rows.target", tgt_count)
       # ... existing logic
   ```

### 11.13 Revised Roadmap

Replaces the §8 roadmap where it conflicts. Sequenced around real-repo entry points.

**Week 1 — Foundation for everything that follows**

- [ ] Replace `MockDBSession` with `testcontainers-python[postgres]` singleton + `ddl.sql`/`grants.sql` preload. Convert all 19 service tests. (**highest single leverage**)
- [ ] Add 3 new concurrency tests against real Postgres (SKIP LOCKED race, optimistic-lock version conflict, default-privilege sequence grant).
- [ ] Unify versions: bump `src/app/pyproject.toml` to `0.1.0` to match root. Add a pre-commit check that they stay in lockstep.

**Week 2 — Ephemeral PR apps + frontend bootstrap**

- [ ] Add `pr` target to `databricks.yml.example` (copy-adapt from §5.6). Wire GitHub Actions OIDC → Databricks.
- [ ] Deploy-on-open, destroy-on-close workflows.
- [ ] Install Vitest + RTL + MSW + Playwright in `src/app/frontend/`. First 3 component tests (SystemsView, TablesView, QueueView).

**Week 3 — Spark job refactor + type-transform hardening**

- [ ] Extract pure-Python helpers out of `jobs/*.py` notebooks into `src/app/jobs/` plain modules. Target: 60%+ of job logic becomes vanilla-pytest-testable.
- [ ] Add AST validation + RestrictedPython sandbox to type-transformation save endpoint. Reject at save.
- [ ] Hypothesis property tests on the exec path (default transformations never crash; arbitrary input either rejects or stays bounded).

**Week 4 — Multi-DB Testcontainers + sqlglot**

- [ ] Add MySQL + SQL Server + Delta-Spark + (optionally Oracle Free) Testcontainers to the integration job.
- [ ] Add sqlglot parse-only tests for Netezza, Teradata, Snowflake.
- [ ] Differential tests across the five dialects with containerized/mocked backends.

**Week 5 — Observability**

- [ ] Replace `print()` with `structlog` across backend + jobs.
- [ ] Add `sentry_sdk.init(...)` with PII scrubbing; wire `SENTRY_DSN` as an app-yaml secret.
- [ ] Enable managed OTel in UC for staging app. Build one dashboard: "validation runs by status, per-customer workspace."

**Week 6 — Release engineering**

- [ ] Delete `src/app/frontend/dist/` from git; add to `.gitignore`; build in CI.
- [ ] Install release-please with conventional-commit CI check.
- [ ] Add `CHANGELOG.md`, `SECURITY.md`, `SBOM` generation via CycloneDX.
- [ ] Sigstore keyless signing of release wheels.

**Month 2 — Feature flags + canary**

- [ ] Three-layer flag system (env var → `control.flags` Postgres table → optional Unleash).
- [ ] Cohort rollout: Cohort A (Databricks internal), B (2 friendly PS engagements), C (NVIDIA + the rest).
- [ ] 48-hour dogfood soak gating every stable tag.

**Month 3 — v1.0 readiness**

- [ ] Playwright MCP weekly exploratory sweep.
- [ ] HAR-replay corpus from real NVIDIA usage (with consent + scrub).
- [ ] Mutation testing baseline on `src/app/backend/services/` (mutmut) and any pure frontend logic (Stryker).
- [ ] Second full engagement running `0.x` with zero escaped regressions → promote to `1.0`.

### 11.14 Final Word to Dan

Three things that will disproportionately reduce the "3 AM, NVIDIA called" fear:

1. **Postgres Testcontainer replacing `MockDBSession`** — you'll catch entire classes of concurrency, locking, and constraint bugs your mocks silently pass today.
2. **Ephemeral PR apps** — every change gets demoed on a live Databricks App before merge. Reviewers click through; regressions surface pre-merge.
3. **AST-validation + RestrictedPython on the type-transformation `exec()`** — closes what is probably the repo's largest latent correctness-and-security gap.

Items 1 and 3 are ~2 engineering days each. Item 2 is ~1 day once the DABs `pr` target is wired. Inside of a week you can meaningfully shift the risk posture of the project.

The rest of the document (§1–§10) is still the right long-term target. §11 is how you get there from where the repo is today.

---

*Document ends.*
