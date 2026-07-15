/**
 * AgentRouter Billing SSE Filter Plugin
 *
 * 背景：AgentRouter 在 SSE 流式响应正文发送完之后，会额外多发一帧
 * 非标准事件：data: {"billing":{...},"object":"billing.summary"}
 * opencode 内部的 @ai-sdk/openai-compatible 用 Zod 校验这一帧时，
 * 因为它不符合 completion chunk 的 schema，抛出 invalid_union /
 * "Type validation failed" 错误——但这时候真正的回答内容已经流完了，
 * 所以现象是"对话正常，结尾报错"。
 *
 * 本插件在 fetch 层拦截发往 agentrouter.org 的响应，从 SSE 流里
 * 精确过滤掉这一帧 billing.summary 事件，其余内容原样透传。
 *
 * 用法：
 * 1. 放到 .opencode/plugins/agentrouter-billing-fix.js
 * 2. 全局配置 ~/.config/opencode/opencode.jsonc（或项目级 opencode.json）里加：
 *      "plugin": ["./.opencode/plugins/agentrouter-billing-fix.js"]
 *    注意：这个版本的 opencode 配置字段是单数 "plugin"，不是 "plugins"。
 * 3. 重启 opencode。
 *
 * 注意事项：
 * - 这是接管 globalThis.fetch 的非官方做法，依赖 AgentRouter 当前的
 *   SSE 分帧行为，opencode 或 AgentRouter 任一方变更实现都可能让它失效。
 * - 只处理 text/event-stream 流式响应；非流式 JSON 响应不做处理。
 */

const AGENTROUTER_HOST = "agentrouter.org";

function isAgentRouterUrl(url) {
  try {
    const parsed =
      typeof url === "string" ? new URL(url) : new URL(url.url ?? url.href ?? String(url));
    return (
      parsed.hostname === AGENTROUTER_HOST ||
      parsed.hostname.endsWith("." + AGENTROUTER_HOST)
    );
  } catch {
    return false;
  }
}

/**
 * 判断一个 SSE data 负载是不是那个非标准的 billing.summary 尾帧。
 * 用 JSON.parse + 精确字段比对，不用字符串子串匹配——
 * 避免模型正常回答里恰好提到 "billing" 这个词时被误杀。
 */
function isBillingSummaryPayload(payload) {
  if (payload === "[DONE]") return false;
  try {
    const json = JSON.parse(payload);
    return !!json && json.object === "billing.summary";
  } catch {
    return false; // 解析不了就当正常帧放行，不误杀
  }
}

/**
 * 过滤 SSE 流：按标准的空行（\n\n）分事件块，逐块检查其中的
 * data: 行，命中 billing.summary 就整块丢弃，否则原样透传。
 */
function filterBillingSummaryStream(body) {
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let buffer = "";

  return new ReadableStream({
    async start(controller) {
      const reader = body.getReader();
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            if (buffer.trim()) controller.enqueue(encoder.encode(buffer));
            controller.close();
            return;
          }

          buffer += decoder.decode(value, { stream: true });
          const parts = buffer.split("\n\n");
          buffer = parts.pop() ?? ""; // 最后一段可能不完整，留到下一轮

          for (const part of parts) {
            let drop = false;
            for (const line of part.split("\n")) {
              const trimmed = line.trim();
              if (!trimmed.startsWith("data:")) continue;
              const payload = trimmed.slice(5).trim();
              if (isBillingSummaryPayload(payload)) {
                drop = true;
                break;
              }
            }
            if (!drop) controller.enqueue(encoder.encode(part + "\n\n"));
          }
        }
      } catch (err) {
        controller.error(err);
      }
    },
  });
}

function patchFetchOnce(logFn) {
  // 防止插件热重载时反复包裹 fetch，多层套娃
  if (globalThis.__agentrouter_billing_filter_patched__) return;
  globalThis.__agentrouter_billing_filter_patched__ = true;

  const originalFetch = globalThis.fetch.bind(globalThis);

  globalThis.fetch = async function (input, init) {
    const response = await originalFetch(input, init);

    const url = typeof input === "string" || input instanceof URL ? input : input?.url;
    if (!url || !isAgentRouterUrl(url) || !response.body) return response;

    const contentType = response.headers.get("content-type") ?? "";
    if (!contentType.includes("text/event-stream")) return response;

    logFn?.("AgentRouter SSE 响应已接入 billing.summary 过滤");

    // 保留原始 status / headers，只替换 body，避免下游因为
    // 缺失 content-type 等头信息而误判响应类型
    return new Response(filterBillingSummaryStream(response.body), {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  };
}

export const AgentRouterBillingFilterPlugin = async ({ client }) => {
  const logFn = (message) => {
    client?.app
      ?.log?.({
        body: { service: "agentrouter-billing-filter", level: "info", message },
      })
      .catch(() => {});
  };

  patchFetchOnce(logFn);

  try {
    await client.app.log({
      body: {
        service: "agentrouter-billing-filter",
        level: "info",
        message: "AgentRouter billing SSE filter installed",
      },
    });
  } catch {
    // 日志接口不可用就算了，不影响过滤逻辑本身
  }

  return {};
};

export default AgentRouterBillingFilterPlugin;