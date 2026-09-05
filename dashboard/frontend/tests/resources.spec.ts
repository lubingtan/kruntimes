import { expect, test } from "@playwright/test";
import { readFile } from "node:fs/promises";

test("resource pages share raised surfaces in both themes", async ({
  page,
}, testInfo) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const run = {
    name: "build",
    namespace: "default",
    uid: "build-1",
    runtime: "bash",
    mode: "Task",
    phase: "Succeeded",
    creationTimestamp: "2026-09-01T00:00:00Z",
    assignedPod: "bash-1",
    spec: { runtime: "bash" },
    status: { phase: "Succeeded" },
  };
  const runtime = {
    name: "bash",
    namespace: "default",
    replicas: 1,
    readyReplicas: 1,
    capacity: { executions: "10" },
    runCount: 1,
    healthy: true,
  };
  const workflow = {
    name: "release",
    uid: "release-1",
    namespace: "default",
    phase: "Succeeded",
    jobCount: 4,
    creationTimestamp: run.creationTimestamp,
  };
  await page.route("**/*", async (route) => {
    if (route.request().resourceType() === "document") {
      return route.fulfill({
        contentType: "text/html",
        body: await readFile("../backend/assets/index.html"),
      });
    }
    const path = new URL(route.request().url()).pathname;
    if (!path.startsWith("/api/")) return route.continue();
    const data: Record<string, unknown> = {
      "/api/session": { authenticated: true },
      "/api/namespaces": { items: ["default"] },
      "/api/namespaces/default/runs": {
        items: [
          run,
          ...["Failed", "Cancelled", "Pending"].map((phase) => ({
            ...run,
            name: phase.toLowerCase(),
            uid: phase,
            phase,
          })),
        ],
      },
      "/api/namespaces/default/runs/build": run,
      "/api/namespaces/default/runtimes": { items: [runtime] },
      "/api/namespaces/default/runtimes/bash": {
        runtime,
        spec: {},
        status: {},
        pods: [
          {
            name: "bash-1",
            phase: "Running",
            ready: true,
            runtimedReady: true,
            runs: [run],
          },
        ],
      },
      "/api/namespaces/default/workflowruns": { items: [workflow] },
    };
    await route.fulfill({ json: data[path] ?? { authenticated: true } });
  });
  const paths = [
    "/",
    "/about",
    "/namespaces/default/runs",
    "/namespaces/default/runs/build",
    "/namespaces/default/runtimes",
    "/namespaces/default/runtimes/bash",
    "/namespaces/default/workflowruns",
  ];
  for (const mode of ["light", "dark"]) {
    for (const [index, path] of paths.entries()) {
      await page.goto(path);
      await page
        .getByRole("combobox", { name: "Theme", exact: true })
        .selectOption(mode);
      const panel = page.locator("main .neu-raised").first();
      await expect(panel).toBeVisible();
      await expect(panel).not.toHaveCSS("box-shadow", "none");
      await expect(panel).toHaveCSS(
        "background-color",
        mode === "light" ? "rgb(230, 235, 240)" : "rgb(39, 47, 59)",
      );
      if (path.endsWith("/runs")) {
        await expect(page.locator(".phase")).toHaveCount(4);
        await expect(page.locator("aside .neu-selected")).toContainText("Runs");
      }
      await page.screenshot({
        path: testInfo.outputPath(`resource-${index}-${mode}.png`),
        fullPage: true,
        animations: "disabled",
      });
    }
  }
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(
    page.getByRole("combobox", { name: "Namespace", exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  expect(errors).toEqual([]);
});
