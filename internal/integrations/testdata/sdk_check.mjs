// Opt-in installed-SDK integration: real extension/session lifecycles, deterministic model stream.
import assert from 'node:assert/strict';
import net from 'node:net';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';

const [adapter, host, sdkRoot] = process.argv.slice(2);
const networkAttempts = [];
globalThis.fetch = async (url) => {
  networkAttempts.push(String(url));
  throw new Error('Network disabled in installed-SDK fixture');
};
const sdk = await import(pathToFileURL(join(sdkRoot, host === 'pi' ? 'dist/index.js' : 'src/index.ts')).href);
const { default: mesh } = await import(pathToFileURL(adapter).href);
const cwd = process.cwd(), agentDir = join(cwd, 'agent');
await mkdir(agentDir, { recursive: true });
const errors = [];
let extensionContext;
const witness = (pi) => {
  pi.on('session_start', (_event, ctx) => { extensionContext = ctx; });
};
const modelSpec = {
  id: 'fixture', name: 'Fixture', reasoning: false, input: ['text'],
  contextWindow: 64000, maxTokens: 1024,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
};
const provider = { api: 'openai-completions', baseUrl: 'http://127.0.0.1:9', apiKey: 'fixture', models: [modelSpec] };
let session, auth;
if (host === 'pi') {
  const modelRuntime = await sdk.ModelRuntime.create({
    authPath: join(agentDir, 'auth.json'), modelsPath: null,
    modelsStorePath: join(agentDir, 'models-cache.json'), allowModelNetwork: false, refreshOnCreate: false,
  });
  modelRuntime.registerProvider('agm-fixture', provider);
  await modelRuntime.setRuntimeApiKey('agm-fixture', 'fixture');
  const settingsManager = sdk.SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } });
  const resourceLoader = new sdk.DefaultResourceLoader({
    cwd, agentDir, settingsManager, noExtensions: true, noSkills: true,
    noPromptTemplates: true, noThemes: true, noContextFiles: true,
    systemPrompt: 'Offline adapter integration fixture.', extensionFactories: [mesh, witness],
  });
  await resourceLoader.reload();
  assert.deepEqual(resourceLoader.getExtensions().errors, []);
  ({ session } = await sdk.createAgentSession({
    cwd, agentDir, modelRuntime, model: modelRuntime.getModel('agm-fixture', 'fixture'),
    settingsManager, resourceLoader, sessionManager: sdk.SessionManager.inMemory(cwd),
    tools: ['agent_mesh'], thinkingLevel: 'off',
  }));
  await session.bindExtensions({ mode: 'print', onError: (e) => errors.push(e) });
} else {
  auth = await sdk.AuthStorage.create(join(agentDir, 'auth.db'));
  auth.setRuntimeApiKey('agm-fixture', 'fixture');
  const modelRegistry = new sdk.ModelRegistry(auth, join(agentDir, 'models.json'));
  await modelRegistry.refresh('offline');
  modelRegistry.registerProvider('agm-fixture', provider);
  ({ session } = await sdk.createAgentSession({
    cwd, agentDir, authStorage: auth, modelRegistry, model: modelRegistry.find('agm-fixture', 'fixture'),
    settings: sdk.Settings.isolated({ 'compaction.enabled': false, 'retry.enabled': false }),
    sessionManager: sdk.SessionManager.inMemory(cwd), thinkingLevel: 'off',
    disableExtensionDiscovery: true, extensions: [mesh, witness], preloadedCustomToolPaths: [],
    enableMCP: false, enableLsp: false, enableIrc: false, skipPythonPreflight: true,
    skills: [], rules: [], contextFiles: [], promptTemplates: [], slashCommands: [],
    systemPrompt: 'Offline adapter integration fixture.', toolNames: ['agent_mesh'],
  }));
  const { initializeExtensions } = await import(pathToFileURL(join(sdkRoot, 'src/modes/runtime-init.ts')).href);
  await initializeExtensions(session, {
    reportRuntimeError: (e) => errors.push(e),
    reportSendError: (action, error) => errors.push({ action, message: error.message }),
  });
}
assert.ok(session.model && extensionContext, 'SDK did not bind model/extension context');
const sid = session.sessionManager.getSessionId();

const socket = net.createConnection(process.env.AGM_SOCKET);
socket.setEncoding('utf8');
let buffer = '', sequence = 0;
const pending = new Map();
socket.on('data', (chunk) => {
  buffer += chunk;
  for (;;) {
    const end = buffer.indexOf('\n');
    if (end < 0) break;
    const frame = JSON.parse(buffer.slice(0, end)); buffer = buffer.slice(end + 1);
    const waiter = pending.get(frame.id);
    if (!waiter) continue;
    pending.delete(frame.id);
    if (frame.error) waiter.reject(new Error(JSON.stringify(frame.error)));
    else waiter.resolve(frame.result);
  }
});
await new Promise((resolve, reject) => { socket.once('connect', resolve); socket.once('error', reject); });
function rpc(request) {
  return new Promise((resolve, reject) => {
    const id = ++sequence;
    pending.set(id, { resolve, reject });
    socket.write(JSON.stringify({ id, ...request }) + '\n');
  });
}
async function eventually(label, check, ms = 5000) {
  for (const end = Date.now() + ms; Date.now() < end; await sleep(20)) if (await check()) return;
  throw new Error(`Timed out after ${ms}ms: ${label}`);
}
await rpc({ op: 'hello', session: { id: 'sdk-peer', name: 'sdk-peer' } });
const info = () => rpc({ op: 'resolve', to: sid });
await eventually('native initial idle', async () => (await info()).activity === 'idle');
await session.setSessionName('sdk-renamed', 'user');
await eventually('idle rename within 1200ms', async () => (await info()).name === 'sdk-renamed', 1200);
console.log(`${host}: native idle and idle rename passed`);

let mode = 'steer', calls = 0, toolsIssued = false, release, started;
const firstStarted = new Promise((resolve) => { started = resolve; });
const contexts = [];
function stream(model, context, options) {
  const n = ++calls;
  contexts.push(context);
  let final;
  return {
    async *[Symbol.asyncIterator]() {
      const message = {
        role: 'assistant', provider: model.provider, model: model.id, api: model.api,
        content: [], timestamp: Date.now(), stopReason: 'stop',
        usage: { input: 10, output: 5, cacheRead: 0, cacheWrite: 0, totalTokens: 15,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      };
      yield { type: 'start', partial: message };
      if (n === 1 || mode === 'abort') {
        await new Promise((resolve) => {
          release = resolve;
          options.signal?.addEventListener('abort', resolve, { once: true });
          if (options.signal?.aborted) resolve();
          started();
        });
      }
      if (mode === 'abort' || mode === 'error') {
        message.stopReason = mode === 'abort' ? 'aborted' : 'error';
        message.errorMessage = `fixture ${mode}`;
        final = message;
        yield { type: 'error', reason: message.stopReason, error: message };
        return;
      }
      if (mode === 'tools' && !toolsIssued) {
        toolsIssued = true;
        const args = { action: 'send', to: 'sdk-peer', summary: 'Native SDK send', request_key: 'sdk-identical-retry' };
        message.content = [
          { type: 'toolCall', id: 'sdk-call-1', name: 'agent_mesh', arguments: args },
          { type: 'toolCall', id: 'sdk-call-2', name: 'agent_mesh', arguments: args },
          // OMP repairs extra properties before execute; a missing required summary must still fail.
          { type: 'toolCall', id: 'sdk-invalid', name: 'agent_mesh', arguments: { action: 'send', to: 'sdk-peer' } },
        ];
        message.stopReason = 'toolUse';
      } else message.content = [{ type: 'text', text: 'Fixture done.' }];
      final = message;
      yield { type: 'done', reason: message.stopReason, message };
    },
    result: async () => final,
  };
}
if (host === 'pi') session.agent.streamFunction = stream;
else session.agent.streamFn = stream;

try {
  const running = session.prompt('Exercise native mesh delivery.');
  await firstStarted;
  await eventually('native active run', async () => (await info()).activity === 'busy');
  const incoming = await rpc({ op: 'send', to: sid, text: 'SDK busy delivery 😀', expects_reply: false });
  await sleep(50); // allow the real broker event to reach the host's steering queue
  release();
  await running;
  await session.waitForIdle();
  await eventually('native settled idle', async () => (await info()).activity === 'idle', 11000);
  // OMP legitimately skips tools when steering interrupts a step; test tools in a separate run.
  mode = 'tools';
  await session.prompt('Exercise native tool validation and keyed retries.');
  await session.waitForIdle();
  const results = session.messages.filter((m) => m.role === 'toolResult' && m.toolName === 'agent_mesh');
  assert.equal(results.length, 3, JSON.stringify(results));
  const successful = results.filter((m) => !m.isError).map((m) => JSON.parse(m.content.find((c) => c.type === 'text').text));
  assert.equal(successful.length, 2, JSON.stringify(results));
  assert.equal(successful[0].id, successful[1].id, 'same-key native retry created two messages');
  assert.equal(successful[0].status, 'broker_accepted');
  assert.equal(results.filter((m) => m.isError).length, 1, 'native validation accepted a missing summary');
  assert.equal((await rpc({ op: 'inbox' })).length, 1, 'native keyed retry delivered duplicate mail');
  await eventually('persisted incoming mail ACKed', async () => (await info()).queued === 0);
  const entries = session.sessionManager.getBranch();
  assert.ok(entries.some((entry) => entry.type === 'custom_message' && entry.details?.id === incoming.id), 'ACK preceded session persistence');
  assert.ok(JSON.stringify(contexts).includes('SDK busy delivery 😀'), 'steer never reached the model');
  console.log(`${host}: real native tools, parameter validation, keyed retry, busy steer, persistence and ACK passed`);

  mode = 'error';
  await session.prompt('Exercise terminal error.');
  await session.waitForIdle();
  await eventually('idle after terminal error', async () => (await info()).activity === 'idle', 11000);
  mode = 'abort';
  const abortStarted = new Promise((resolve) => { started = resolve; });
  const aborting = session.prompt('Exercise abort.');
  await abortStarted;
  await eventually('busy before abort', async () => (await info()).activity === 'busy');
  await session.abort();
  await aborting;
  await eventually('idle after abort', async () => (await info()).activity === 'idle', 11000);
  mode = 'idle';
  const beforeIdleDelivery = calls;
  const idleMail = await rpc({ op: 'send', to: sid, text: 'SDK idle delivery 😀' });
  await eventually('idle mail started a real host turn', () => calls > beforeIdleDelivery);
  await session.waitForIdle();
  await eventually('idle mail persisted and ACKed', async () => (await info()).queued === 0);
  assert.ok(session.sessionManager.getBranch().some((entry) => entry.type === 'custom_message' && entry.details?.id === idleMail.id));
  assert.deepEqual(errors, [], 'extension runtime reported errors');
  assert.deepEqual(networkAttempts, [], 'SDK attempted network access despite isolated configuration');
  console.log(`${host}: native terminal-error, abort and idle-triggered delivery passed`);
} finally {
  await session.extensionRunner.emit({ type: 'session_shutdown' });
  await session.dispose();
  socket.end();
  auth?.close();
}
