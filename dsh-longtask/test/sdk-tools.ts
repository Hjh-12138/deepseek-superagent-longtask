/** Test composition adapter: 0.2.2 separates the native executor and takes whole batches. */
export async function loadSdk(name: string): Promise<any> {
  return import("@deepseek-ai/" + name);
}

export async function prepareSdkTools(ctx: any, tools: any): Promise<any> {
  if (typeof tools.setExecutor !== "function") return tools;
  const executorPackage = "@deepseek-ai/dsh-tool-executor-native";
  const {NativeToolExecutor} = await import(executorPackage);
  tools.setExecutor(new NativeToolExecutor(ctx));
  return new Proxy(tools, {get(target, property) {
    if (property === "execute") return async (request: any) => {
      if (request.calls) return target.execute(request);
      const batch = await target.execute({calls: [{type: "tool-call", id: request.callId, name: request.name, arguments: JSON.stringify(request.arguments ?? {})}],
        agent: request.agent, signal: request.signal});
      if (batch.results.length !== 1) throw Error("Expected exactly one tool result");
      return batch.results[0].result;
    };
    const value = Reflect.get(target, property, target);
    return typeof value === "function" ? value.bind(target) : value;
  }});
}
