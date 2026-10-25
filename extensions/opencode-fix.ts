import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { spawn } from "node:child_process";

const OPENCODE_VERSION = "2.1.4";
const USER_AGENT = `opencode/latest/${OPENCODE_VERSION}/cli`;

const ID_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
let counter = 0;
let lastTimestamp = 0;

function generateId(prefix: "ses" | "msg", descending: boolean, timestamp = Date.now()): string {
  if (timestamp !== lastTimestamp) {
    lastTimestamp = timestamp;
    counter = 0;
  }
  counter++;

  const current = BigInt(timestamp) * 0x1000n + BigInt(counter);
  const value = descending ? ~current : current;
  const timeHex = Array.from({ length: 6 }, (_, index) =>
    Number((value >> BigInt(40 - 8 * index)) & 0xffn)
      .toString(16)
      .padStart(2, "0")
  ).join("");

  const bytes = crypto.getRandomValues(new Uint8Array(14));
  const rand = Array.from(bytes, (b) => ID_CHARS[b % 62]).join("");
  return `${prefix}_${timeHex}${rand}`;
}

type MessageItem = {
  role?: string;
  content?: string | Array<{ text?: string }>;
};

function extractPromptAndModel(body: unknown): { model: string; prompt: string } {
  let model = "beam-free";
  let prompt = "";

  if (typeof body === "string") {
    try {
      body = JSON.parse(body);
    } catch {
      return { model, prompt: body };
    }
  }

  if (body && typeof body === "object") {
    const b = body as Record<string, unknown>;
    if (typeof b.model === "string") {
      model = b.model.replace(/^opencode-zen\//, "").replace(/^opencode\//, "");
    }
    const messageList: MessageItem[] = Array.isArray(b.messages)
      ? (b.messages as MessageItem[])
      : Array.isArray(b.input)
        ? (b.input as MessageItem[])
        : [];

    const userMsgs = messageList.filter((m) => m && m.role === "user");
    const lastUser = userMsgs[userMsgs.length - 1];
    if (typeof lastUser?.content === "string") {
      prompt = lastUser.content;
    } else if (Array.isArray(lastUser?.content)) {
      prompt = lastUser.content.map((p) => p?.text || "").join("\n");
    }
  }

  return { model: model || "beam-free", prompt: prompt || "hello" };
}

function bridgeOpenCodeStandalone(
  model: string,
  prompt: string,
  isResponsesApi: boolean,
  streaming: boolean
): Response {
  if (!streaming) {
    const child = spawn("opencode", [
      "run",
      "--standalone",
      "--format", "json",
      "--model", `opencode/${model}`,
      prompt
    ], {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env }
    });

    let fullText = "";
    child.stdout.on("data", (chunk: Buffer) => {
      const lines = chunk.toString().split("\n");
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const ev = JSON.parse(line) as { type?: string; part?: { text?: string } };
          if (ev.type === "text" && ev.part?.text) {
            fullText += ev.part.text;
          }
        } catch {}
      }
    });

    return new Response(
      JSON.stringify(
        isResponsesApi
          ? {
              id: `resp_${Date.now()}`,
              status: "completed",
              output: [
                {
                  id: `msg_${Date.now()}`,
                  type: "message",
                  role: "assistant",
                  content: [{ type: "output_text", text: fullText }]
                }
              ]
            }
          : {
              id: `chatcmpl-${Date.now()}`,
              object: "chat.completion",
              created: Math.floor(Date.now() / 1000),
              model,
              choices: [
                {
                  index: 0,
                  message: { role: "assistant", content: fullText },
                  finish_reason: "stop"
                }
              ]
            }
      ),
      {
        status: 200,
        headers: { "content-type": "application/json" }
      }
    );
  }

  const child = spawn("opencode", [
    "run",
    "--standalone",
    "--format", "json",
    "--model", `opencode/${model}`,
    prompt
  ], {
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env }
  });

  const stream = new ReadableStream({
    start(controller) {
      let buffer = "";
      const encoder = new TextEncoder();

      child.stdout.on("data", (chunk: Buffer) => {
        buffer += chunk.toString();
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const ev = JSON.parse(line) as { type?: string; part?: { text?: string } };
            if (ev.type === "text" && ev.part?.text) {
              const text = ev.part.text;
              if (isResponsesApi) {
                const sse = `event: response.output_text.delta\ndata: ${JSON.stringify({
                  type: "response.output_text.delta",
                  delta: text
                })}\n\n`;
                controller.enqueue(encoder.encode(sse));
              } else {
                const sse = `data: ${JSON.stringify({
                  id: `chatcmpl-${Date.now()}`,
                  object: "chat.completion.chunk",
                  created: Math.floor(Date.now() / 1000),
                  model,
                  choices: [{ index: 0, delta: { content: text }, finish_reason: null }]
                })}\n\n`;
                controller.enqueue(encoder.encode(sse));
              }
            }
          } catch {}
        }
      });

      child.on("close", () => {
        if (isResponsesApi) {
          const sse = `event: response.completed\ndata: ${JSON.stringify({
            type: "response.completed",
            response: { id: `resp_${Date.now()}`, status: "completed" }
          })}\n\n`;
          controller.enqueue(encoder.encode(sse));
        } else {
          const sse = `data: ${JSON.stringify({
            id: `chatcmpl-${Date.now()}`,
            object: "chat.completion.chunk",
            created: Math.floor(Date.now() / 1000),
            model,
            choices: [{ index: 0, delta: {}, finish_reason: "stop" }]
          })}\n\ndata: [DONE]\n\n`;
          controller.enqueue(encoder.encode(sse));
        }
        controller.close();
      });

      child.on("error", (err) => {
        controller.error(err);
      });
    }
  });

  return new Response(stream, {
    status: 200,
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache",
      "connection": "keep-alive"
    }
  });
}

// Monkey-patch globalThis.fetch to inject OpenCode v2.1.4 headers & provide fallback bridge
const originalFetch = globalThis.fetch;
globalThis.fetch = async function (input: RequestInfo | URL, init?: RequestInit) {
  const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;

  if (typeof url === "string" && url.includes("opencode.ai")) {
    init = init || {};
    const now = Date.now();
    const sessionId = generateId("ses", true, now - 3000);
    const requestId = generateId("msg", false, now);

    const headersToInject: Record<string, string> = {
      "User-Agent": USER_AGENT,
      "user-agent": USER_AGENT,
      "x-opencode-client": "cli",
      "x-opencode-project": "global",
      "x-opencode-session": sessionId,
      "x-opencode-session-id": sessionId,
      "x-opencode-request": requestId,
      "x-session-affinity": sessionId,
      "X-Session-Id": sessionId,
    };

    if (!init.headers) {
      init.headers = headersToInject;
    } else if (init.headers instanceof Headers) {
      for (const [k, v] of Object.entries(headersToInject)) {
        if (!init.headers.has(k) || k.toLowerCase() === "user-agent") {
          init.headers.set(k, v);
        }
      }
    } else if (Array.isArray(init.headers)) {
      for (const [k, v] of Object.entries(headersToInject)) {
        init.headers.push([k, v]);
      }
    } else {
      Object.assign(init.headers, headersToInject);
    }

    try {
      const response = await originalFetch(input, init);

      // If gateway returns 429 FreeUsageLimitError or 403 FreeTierError, rescue with standalone OpenCode CLI if available
      if (response.status === 429 || response.status === 403) {
        const { model, prompt } = extractPromptAndModel(init.body);
        const isResponses = url.includes("/responses");
        const acceptHeader =
          init.headers instanceof Headers
            ? init.headers.get("accept")
            : !Array.isArray(init.headers) && typeof init.headers === "object"
              ? (init.headers as Record<string, string>)["accept"]
              : undefined;
        const streaming = acceptHeader?.includes("text/event-stream") ?? true;

        try {
          return bridgeOpenCodeStandalone(model, prompt, isResponses, streaming);
        } catch {
          return response;
        }
      }

      return response;
    } catch (err) {
      // Network or gateway error fallback
      const { model, prompt } = extractPromptAndModel(init.body);
      const isResponses = url.includes("/responses");
      try {
        return bridgeOpenCodeStandalone(model, prompt, isResponses, true);
      } catch {
        throw err;
      }
    }
  }

  return originalFetch(input, init);
};

export default function (pi: ExtensionAPI) {
  const providers = ["oc", "opencode", "opencode-go", "opencode-zen"];
  for (const p of providers) {
    pi.registerProvider?.(p, {
      headers: {
        "User-Agent": USER_AGENT,
        "x-opencode-client": "cli",
        "x-opencode-project": "global",
      },
    });
  }
}
