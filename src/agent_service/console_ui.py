from __future__ import annotations


def build_console_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Agent Console MVP</title>
  <style>
    :root {
      --bg-a: #f3efe7;
      --bg-b: #d8e4ec;
      --card: rgba(255, 255, 255, 0.82);
      --ink: #17232b;
      --muted: #43535b;
      --accent: #0f766e;
      --line: rgba(23, 35, 43, 0.16);
      --shadow: 0 14px 45px rgba(16, 32, 46, 0.12);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "Segoe UI", "Helvetica Neue", sans-serif;
      background:
        radial-gradient(circle at 8% 7%, rgba(15, 118, 110, 0.18), transparent 32%),
        radial-gradient(circle at 93% 84%, rgba(43, 108, 176, 0.2), transparent 36%),
        linear-gradient(130deg, var(--bg-a), var(--bg-b));
      min-height: 100vh;
      padding: 26px;
    }

    .shell {
      width: min(1180px, 100%);
      margin: 0 auto;
      display: grid;
      grid-template-columns: 330px 1fr;
      gap: 16px;
    }

    .panel {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(8px);
    }

    .controls {
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .title {
      font-size: 20px;
      font-weight: 700;
      margin: 0;
      letter-spacing: 0.2px;
    }

    .subtitle {
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 13px;
    }

    label {
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 5px;
      font-weight: 600;
    }

    select,
    input,
    textarea,
    button {
      width: 100%;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: #ffffff;
      color: var(--ink);
      font: inherit;
    }

    select,
    input,
    textarea {
      padding: 10px 12px;
    }

    textarea {
      min-height: 96px;
      resize: vertical;
    }

    button {
      cursor: pointer;
      padding: 10px 12px;
      border: 0;
      background: linear-gradient(90deg, #0f766e, #0d9488);
      color: #ffffff;
      font-weight: 700;
      transition: transform 120ms ease, filter 120ms ease;
    }

    button:hover {
      transform: translateY(-1px);
      filter: brightness(1.03);
    }

    .content {
      padding: 16px;
      display: grid;
      gap: 12px;
      align-content: start;
    }

    .card {
      background: rgba(255, 255, 255, 0.88);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      animation: reveal 260ms ease;
    }

    @keyframes reveal {
      from {
        opacity: 0;
        transform: translateY(6px);
      }
      to {
        opacity: 1;
        transform: translateY(0);
      }
    }

    .card h2 {
      margin: 0 0 8px;
      font-size: 14px;
      letter-spacing: 0.3px;
      text-transform: uppercase;
      color: #1f3a42;
    }

    .mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 13px;
    }

    ul {
      margin: 0;
      padding-left: 18px;
    }

    li {
      margin-bottom: 4px;
    }

    .meta {
      font-size: 12px;
      color: var(--muted);
      margin-top: 6px;
    }

    .error {
      color: #9f1239;
      font-size: 13px;
      min-height: 18px;
    }

    @media (max-width: 900px) {
      body {
        padding: 12px;
      }

      .shell {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside class="panel controls">
      <div>
        <h1 class="title">Agent Console</h1>
        <p class="subtitle">Phase 2A MVP for read-only Ops and DQ diagnostics</p>
      </div>

      <div>
        <label for="mode">Mode</label>
        <select id="mode">
          <option value="ops">Ops</option>
          <option value="dq">DQ</option>
        </select>
      </div>

      <div>
        <label for="env">Environment</label>
        <select id="env">
          <option value="dev">dev</option>
          <option value="prod">prod</option>
        </select>
      </div>

      <div>
        <label for="run_id">Run Id</label>
        <input id="run_id" placeholder="latest_failed_run or explicit run_id" />
      </div>

      <div>
        <label for="question">Question</label>
        <textarea id="question" placeholder="Ask about pipeline failures or DQ violations..."></textarea>
      </div>

      <button id="send_btn" type="button">Send</button>
      <div id="error" class="error"></div>
      <div class="meta mono" id="session_meta">No session started</div>
    </aside>

    <main class="panel content">
      <section class="card">
        <h2>Answer</h2>
        <div id="answer">No response yet.</div>
      </section>

      <section class="card">
        <h2>Evidence Used</h2>
        <ul id="evidence_list"></ul>
      </section>

      <section class="card">
        <h2>Tool Calls</h2>
        <ul id="tool_list"></ul>
      </section>
    </main>
  </div>

  <script>
    const modeEl = document.getElementById("mode");
    const envEl = document.getElementById("env");
    const runIdEl = document.getElementById("run_id");
    const questionEl = document.getElementById("question");
    const sendBtn = document.getElementById("send_btn");
    const answerEl = document.getElementById("answer");
    const evidenceListEl = document.getElementById("evidence_list");
    const toolListEl = document.getElementById("tool_list");
    const errorEl = document.getElementById("error");
    const sessionMetaEl = document.getElementById("session_meta");

    let sessionId = null;

    function clearLists() {
      evidenceListEl.innerHTML = "";
      toolListEl.innerHTML = "";
    }

    function addListItem(listEl, text) {
      const li = document.createElement("li");
      li.className = "mono";
      li.textContent = text;
      listEl.appendChild(li);
    }

    async function ensureSession(mode) {
      if (sessionId) {
        return sessionId;
      }
      const response = await fetch("/sessions", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({user_id: "agent_console_mvp", mode})
      });
      if (!response.ok) {
        throw new Error("failed_to_create_session");
      }
      const body = await response.json();
      sessionId = body.session_id;
      sessionMetaEl.textContent = `session_id=${sessionId}`;
      return sessionId;
    }

    async function sendQuery() {
      errorEl.textContent = "";
      const mode = modeEl.value;
      const env = envEl.value;
      const runId = (runIdEl.value || "").trim();
      const question = (questionEl.value || "").trim();

      if (!question) {
        errorEl.textContent = "Question is required.";
        return;
      }

      sendBtn.disabled = true;
      answerEl.textContent = "Running read-only diagnostics...";
      clearLists();

      try {
        const sid = await ensureSession(mode);
        const contextSuffix = runId ? ` [env=${env} run_id=${runId}]` : ` [env=${env}]`;
        const response = await fetch("/router", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            session_id: sid,
            query: question + contextSuffix,
            intent_hint: mode
          })
        });

        if (!response.ok) {
          throw new Error("router_request_failed");
        }

        const body = await response.json();
        answerEl.textContent = body.response_text;

        (body.evidence_refs || []).forEach((evidenceRef) => {
          addListItem(evidenceListEl, evidenceRef);
        });
        if ((body.evidence_refs || []).length === 0) {
          addListItem(evidenceListEl, "No evidence returned");
        }

        (body.tool_calls || []).forEach((call) => {
          const args = JSON.stringify(call.arguments || {});
          addListItem(toolListEl, `${call.tool_name} ${args}`);
        });
        if ((body.tool_calls || []).length === 0) {
          addListItem(toolListEl, "No tool calls returned");
        }
      } catch (err) {
        answerEl.textContent = "No response yet.";
        errorEl.textContent = "Request failed. Check service logs and try again.";
      } finally {
        sendBtn.disabled = false;
      }
    }

    sendBtn.addEventListener("click", sendQuery);
  </script>
</body>
</html>
"""
