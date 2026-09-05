import type {
  LogEntry,
  RunDetail,
  RunSummary,
  RuntimeDetail,
  RuntimeSummary,
  WorkflowRunDetail,
  WorkflowRunSummary,
} from "./types";

export class DashboardAPI {
  private async request(
    path: string,
    init: RequestInit = {},
  ): Promise<Response> {
    const response = await fetch(path, {
      ...init,
      credentials: "same-origin",
      headers: { ...init.headers },
    });
    if (!response.ok) {
      let message = `Request failed (${response.status})`;
      try {
        message =
          ((await response.json()) as { error?: string }).error || message;
      } catch {
        // Keep the safe status-derived message for non-JSON error responses.
      }
      throw new Error(message);
    }
    return response;
  }

  async connect(token: string): Promise<void> {
    await this.request("/api/session", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token }),
    });
  }
  async session(): Promise<boolean> {
    return (
      (await (await this.request("/api/session")).json()) as {
        authenticated: boolean;
      }
    ).authenticated;
  }
  async disconnect(): Promise<void> {
    await this.request("/api/session", { method: "DELETE" });
  }

  async namespaces(): Promise<string[]> {
    return (
      (await (await this.request("/api/namespaces")).json()) as {
        items: string[];
      }
    ).items;
  }

  async runs(namespace: string): Promise<RunSummary[]> {
    return (
      (await (
        await this.request(
          `/api/namespaces/${encodeURIComponent(namespace)}/runs?limit=200`,
        )
      ).json()) as { items: RunSummary[] }
    ).items;
  }

  async run(namespace: string, name: string): Promise<RunDetail> {
    return (await (
      await this.request(
        `/api/namespaces/${encodeURIComponent(namespace)}/runs/${encodeURIComponent(name)}`,
      )
    ).json()) as RunDetail;
  }

  async logs(namespace: string, name: string): Promise<LogEntry[]> {
    return (
      (await (
        await this.request(
          `/api/namespaces/${encodeURIComponent(namespace)}/runs/${encodeURIComponent(name)}/logs?tail=100`,
        )
      ).json()) as { items: LogEntry[] }
    ).items;
  }
  async runtimes(namespace: string): Promise<RuntimeSummary[]> {
    return (
      (await (
        await this.request(
          `/api/namespaces/${encodeURIComponent(namespace)}/runtimes`,
        )
      ).json()) as { items: RuntimeSummary[] }
    ).items;
  }
  async runtime(namespace: string, name: string): Promise<RuntimeDetail> {
    return (await (
      await this.request(
        `/api/namespaces/${encodeURIComponent(namespace)}/runtimes/${encodeURIComponent(name)}`,
      )
    ).json()) as RuntimeDetail;
  }
  async workflowRuns(namespace: string): Promise<WorkflowRunSummary[]> {
    return (
      (await (
        await this.request(
          `/api/namespaces/${encodeURIComponent(namespace)}/workflowruns`,
        )
      ).json()) as { items: WorkflowRunSummary[] }
    ).items;
  }
  async workflowRun(
    namespace: string,
    name: string,
  ): Promise<WorkflowRunDetail> {
    return (await (
      await this.request(
        `/api/namespaces/${encodeURIComponent(namespace)}/workflowruns/${encodeURIComponent(name)}`,
      )
    ).json()) as WorkflowRunDetail;
  }
}
