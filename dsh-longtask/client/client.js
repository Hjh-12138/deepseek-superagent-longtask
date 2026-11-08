/* DSH client-module entry. The host supplies React and the declared client services. */
window.__ModuleLoader__.load({
  id: "dsh-plugin-longtask",
  factory: require => {
    const {createElement: h} = require("react");
    const kind = "longtask-status";
    const definition = {
      kind: "longtask-status-notice", target: "chat",
      match(event) {
        return event.type === "user/message" && event.surfaceOp === "append"
          && event.data?.source?.kind === kind && typeof event.data.id === "string"
          ? {id: event.data.id, role: "start", exclusive: true} : null;
      },
      start(_context, match) {
        const {event} = match;
        return {seq: event.seq, time: event.time, source: event.data.source,
          text: (event.data.content ?? []).filter(b => b.type === "text" && typeof b.text === "string").map(b => b.text).join("\n")};
      },
      update(context) {return context.state;},
      buildViewNode(context) {
        if (!context.state) return null;
        return {key: context.key, id: context.id, target: "chat", kind, anchorSeq: context.state.seq,
          location: context.start?.location ?? context.matches[0]?.location ?? {kind: "unresolved"},
          visibility: "visible", groupTree: "input", data: context.state};
      }
    };
    function apply(ctx) {
      ctx.uiConversation.events.register(definition);
      function Notice({node}) {
        const {text, source} = node.data;
        const heading = text.split("\n")[0] || source.summary || "长任务通知";
        // rc.5-rc.9 notices already persisted the Worker identity in text. Keep those navigable too.
        const workers = Array.isArray(source.workers) ? source.workers.filter(w => typeof w?.sessionId === "string")
          : Array.from(text.matchAll(/工具 ([^；\n]+)；Worker会话 ([0-9a-f-]{36})；专家 ([^；\n]+)；审批 ([0-9a-f-]{36})/gi),
            match => ({toolName: match[1], sessionId: match[2], relPath: match[3], requestId: match[4]}));
        return h("section", {"data-longtask-notice": source.eventId, "aria-label": heading,
          style: {border: "1px solid var(--dsw-alias-border-l2, #d5d9df)", borderRadius: 10, padding: 14, margin: "8px 0", background: "var(--dsw-alias-markdown-code-block, #f7f8fa)"}},
          h("strong", null, heading),
          h("div", {style: {whiteSpace: "pre-wrap", overflowWrap: "anywhere", marginTop: 8, lineHeight: 1.6}}, text.slice(text.indexOf("\n") + 1)),
          ...workers.map(w => h("button", {key: `${w.sessionId}:${w.requestId ?? ""}`, type: "button",
            onClick: () => ctx.uiWorkspace.openSession(w.sessionId),
            style: {marginTop: 10, marginRight: 8, padding: "6px 10px", cursor: "pointer"}},
          `打开 Worker：${w.relPath ?? w.sessionId} · ${w.toolName ?? "审批"}`)));
      }
      ctx.slots.inject("conversation.chat.node", () => ctx.slots.register({name: "conversation.chat.node", key: kind}, Notice));
    }
    return {inject: ["uiConversation", "slots", "uiWorkspace"], apply};
  }
});
