# Dashboard browser regression tests

From `dashboard/frontend`:

```sh
npm ci
npx playwright install --with-deps chromium
npm run test:browser
```

The tests run the production frontend bundle in Chromium, with mocked API
responses and the dashboard's SPA document fallback. They do not require a
Kubernetes cluster or replace the cluster E2E suite.

Coverage includes individual Job dependency edges, shared predecessors,
edge alignment after zoom and resize, and Tailwind utility precedence over
global element defaults. Theme/interaction tests also exercise Light, Dark,
System, keyboard focus, reduced motion, pressed buttons, Job navigation, and
automatic Step log loading. Resource-page tests cover Home, About, Run and
Runtime lists/details, and the WorkflowRun list. Screenshots are saved under
the Playwright output directory for visual review, not used as pixel baselines.
Installing browser system dependencies may require
administrator privileges on Linux.
