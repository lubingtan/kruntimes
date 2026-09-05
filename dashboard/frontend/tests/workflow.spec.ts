import { expect, test, type Page } from "@playwright/test";
import { readFile } from "node:fs/promises";

async function openWorkflow(page: Page, sharedDependencies = false) {
  // The Go server serves index.html for frontend routes; Vite preview only
  // supports the configured /assets/ base. Reproduce that SPA fallback here.
  await page.route(
    "**/namespaces/default/workflowruns/review**",
    async (route) => {
      if (route.request().resourceType() !== "document")
        return route.fallback();
      await route.fulfill({
        contentType: "text/html",
        body: await readFile("../backend/assets/index.html"),
      });
    },
  );
  await page.route("**/api/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const json = path.endsWith("/session")
      ? { authenticated: true }
      : path === "/api/namespaces"
        ? { items: ["default"] }
        : {
            name: "review",
            namespace: "default",
            phase: "Succeeded",
            creationTimestamp: "2026-09-01T00:00:00Z",
            spec: {
              jobs: {
                A: {},
                B: {},
                C: { needs: sharedDependencies ? ["A", "B"] : ["A"] },
                D: { needs: sharedDependencies ? ["A", "B"] : ["B"] },
              },
            },
            status: {
              jobs: {
                A: {
                  phase: "Succeeded",
                  steps: [
                    { name: "Build", phase: "Succeeded", runName: "build-run" },
                  ],
                },
                B: { phase: "Succeeded" },
                C: { phase: "Succeeded" },
                D: { phase: "Succeeded" },
              },
            },
          };
    await route.fulfill({ json });
  });
  await page.goto("/namespaces/default/workflowruns/review");
}

// Compare the rendered SVG endpoints with the visible Job rows, in screen
// coordinates. This catches both lost dependencies and double-applied zoom.
async function renderedDependencies(page: Page) {
  return page.locator(".dag").evaluate((canvas) => {
    const nodes = [...canvas.querySelectorAll<HTMLAnchorElement>(".dag-node")];
    return [...canvas.querySelectorAll<SVGPathElement>(".dag-edges path")]
      .map((edge) => {
        const matrix = edge.getScreenCTM()!;
        const start = edge.getPointAtLength(0).matrixTransform(matrix);
        const end = edge
          .getPointAtLength(edge.getTotalLength())
          .matrixTransform(matrix);
        const find = (point: DOMPoint, side: "left" | "right") => {
          const node = nodes.find((item) => {
            const rect = item.getBoundingClientRect();
            return (
              Math.abs(rect[side] - point.x) < 1 &&
              Math.abs(rect.top + rect.height / 2 - point.y) < 1
            );
          });
          return node?.querySelector("strong")?.textContent ?? "disconnected";
        };
        return find(start, "right") + "->" + find(end, "left");
      })
      .sort();
  });
}

test("parallel jobs retain distinct dependency relationships", async ({
  page,
}) => {
  await openWorkflow(page);
  await expect.poll(() => renderedDependencies(page)).toEqual(["A->C", "B->D"]);
});

test("shared predecessors retain all four dependency edges", async ({
  page,
}) => {
  await openWorkflow(page, true);
  await expect
    .poll(() => renderedDependencies(page))
    .toEqual(["A->C", "A->D", "B->C", "B->D"]);
});

test("edges stay attached after zoom and resize", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await openWorkflow(page);
  for (const zoom of ["Zoom out", "Zoom in"]) {
    await page.setViewportSize({ width: 1600, height: 1000 });
    await page
      .locator(".dag-node strong")
      .first()
      .evaluate((node) => {
        (node as HTMLElement).style.paddingBottom = "0px";
      });
    await page.getByRole("button", { name: zoom, exact: true }).click();
    await expect
      .poll(() => renderedDependencies(page))
      .toEqual(["A->C", "B->D"]);
    await page.setViewportSize({ width: 1100, height: 800 });
    // Force a node resize as well as a viewport resize at non-default zoom.
    await page
      .locator(".dag-node strong")
      .first()
      .evaluate((node) => {
        (node as HTMLElement).style.paddingBottom = "24px";
      });
    await expect
      .poll(() => renderedDependencies(page))
      .toEqual(["A->C", "B->D"]);
    await page.getByRole("button", { name: "Fit", exact: true }).click();
  }
  expect(errors).toEqual([]);
});

test("Tailwind utilities override global element defaults", async ({
  page,
}) => {
  await openWorkflow(page);
  await expect(
    page.getByRole("button", { name: "Zoom in", exact: true }),
  ).toHaveCSS("width", "40px");
  await expect(page.getByLabel("Search jobs").locator("..")).toHaveCSS(
    "display",
    "flex",
  );
  await expect(page.locator("dl")).toHaveCSS("display", "flex");
  await expect(page.locator("main")).toHaveCSS("padding", "0px");
  await page.setViewportSize({ width: 700, height: 800 });
  await expect(page.locator("main")).toHaveCSS("padding", "0px");
});

test("neumorphic themes, focus and pressed controls", async ({
  page,
}, testInfo) => {
  await openWorkflow(page);
  const theme = page.getByRole("combobox", { name: "Theme", exact: true });
  const zoom = page.getByRole("button", { name: "Zoom in", exact: true });
  for (const mode of ["light", "dark"]) {
    await theme.selectOption(mode);
    await expect(page.locator("html")).toHaveAttribute("data-theme", mode);
    await expect(zoom).toHaveCSS(
      "color",
      mode === "light" ? "rgb(36, 52, 72)" : "rgb(237, 242, 247)",
    );
    await expect(page.locator(".dag-stage").first()).not.toHaveCSS(
      "box-shadow",
      "none",
    );
    await expect(page.getByLabel("Search jobs").locator("..")).toHaveCSS(
      "box-shadow",
      /inset/,
    );
    await expect(
      page.getByRole("button", { name: "Pipeline", exact: true }),
    ).toHaveCSS("box-shadow", /inset/);
    await page.getByRole("button", { name: "Zoom out", exact: true }).focus();
    await page.keyboard.press("Tab");
    await expect(zoom).toBeFocused();
    await expect(zoom).toHaveCSS("outline-style", "solid");
    await zoom.hover();
    await page.mouse.down();
    await expect(zoom).toHaveCSS("box-shadow", /inset/);
    await page.mouse.up();
    await page.getByRole("button", { name: "Fit", exact: true }).click();
    await page.screenshot({
      path: testInfo.outputPath(`workflow-${mode}.png`),
      fullPage: true,
      animations: "disabled",
    });
  }
  await theme.selectOption("system");
  for (const mode of ["light", "dark"] as const) {
    await page.emulateMedia({ colorScheme: mode });
    await expect(page.locator("body")).toHaveCSS(
      "background-color",
      mode === "light" ? "rgb(230, 235, 240)" : "rgb(39, 47, 59)",
    );
  }
  await page.emulateMedia({ reducedMotion: "reduce" });
  await expect(zoom).toHaveCSS("transition-duration", "0s");
});

test("Job navigation and expanding a recessed Step loads logs", async ({
  page,
}, testInfo) => {
  let logRequests = 0;
  await openWorkflow(page);
  await page.route("**/runs/build-run/logs?*", async (route) => {
    logRequests++;
    await route.fulfill({
      json: { items: [{ stream: "stdout", message: "Build complete" }] },
    });
  });
  await page
    .locator(".dag-node")
    .filter({ has: page.locator("strong", { hasText: /^A$/ }) })
    .click();
  await expect(page).toHaveURL(/\/jobs\/A$/);
  await expect(page.locator(".step")).toBeVisible();
  expect(logRequests).toBe(0);
  await page.locator(".step summary").click();
  await expect(page.locator(".log-viewer")).toContainText("Build complete");
  await expect(page.locator(".step")).toHaveCSS("box-shadow", /inset/);
  expect(logRequests).toBe(1);
  for (const mode of ["light", "dark"]) {
    await page
      .getByRole("combobox", { name: "Theme", exact: true })
      .selectOption(mode);
    await page.screenshot({
      path: testInfo.outputPath(`job-${mode}.png`),
      fullPage: true,
      animations: "disabled",
    });
  }
  await page.locator(".step summary").click();
  await page.locator(".step summary").click();
  expect(logRequests).toBe(1);
});
