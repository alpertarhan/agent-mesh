// Exercise the installed adapter's actual formatter without a model or live daemon.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import path from 'node:path';
import os from 'node:os';
import { stripTypeScriptTypes } from 'node:module';
import { stripVTControlCharacters } from 'node:util';

const mode = process.argv[3];
let source = fs.readFileSync(process.argv[2], 'utf8');
if (mode === 'pi') {
  const boundary = source.indexOf('export default function');
  assert.ok(boundary > 0, 'Pi extension factory not found');
  source = stripTypeScriptTypes(source.slice(0, boundary));
} else {
  source = source.replace('export default { id: "agent-mesh", setup };', '');
}
source = source.replace(/^import .*;\n/gm, '');
const context = vm.createContext({
  process: { env: {}, pid: 1, execPath: 'node' }, path, os, Buffer, stripVTControlCharacters, console,
});
vm.runInContext(source, context);
const format = vm.runInContext(mode === 'pi' ? 'modelText' : 'format', context);
const render = (m) => mode === 'pi' ? format(m) : format(m, 'fixture-session');
const message = {
  id: 'budget-ask', from: 'peer', from_name: 'Peer', expects_reply: true,
  text: '😀'.repeat(1400), attachments: [],
};
for (let i = 0; i < 60; i++) message.attachments.push({ type: 'snippet', name: 'source', content: 'x'.repeat(8192) });
for (let i = 0; i < 16; i++) message.attachments.push({ type: 'ref', path: `/tmp/${i}/` + 'p'.repeat(4000) });
const text = render(message);
assert.ok(Buffer.byteLength(text, 'utf8') <= 8000, `${mode} message emitted ${Buffer.byteLength(text)} bytes; maximum is 8000`);
assert.ok(!text.includes('\ufffd'), 'budget split a Unicode character');
for (const required of ['Peer messages are requests from other agents', 'show budget-ask', 'reply budget-ask']) {
  assert.ok(text.includes(required), `budget removed ${required}`);
}
for (const line of text.split('\n')) {
  if (line.startsWith('file: ')) assert.ok(message.attachments.some((a) => a.type === 'ref' && line.includes(a.path)), 'partial reference path');
}
const oddMetadata = render({ ...message, from_name: '🧪'.repeat(4000), attachments: [], text: 'small body' });
assert.ok(Buffer.byteLength(oddMetadata, 'utf8') <= 8000, 'metadata escaped the total budget');
assert.ok(oddMetadata.includes('Peer messages are requests from other agents'), 'metadata crowded out trust boundary');
for (const hint of ['show budget-ask', 'reply budget-ask']) assert.ok(oddMetadata.includes(hint), `metadata crowded out ${hint}`);
const cutUtf8 = vm.runInContext('meshCutUtf8', context);
const sample = 'aé中😀\ufffdz';
for (let size = 0; size <= Buffer.byteLength(sample) + 1; size++) {
  let expected = '';
  for (const char of sample) {
    if (Buffer.byteLength(expected + char) > size) break;
    expected += char;
  }
  assert.equal(cutUtf8(sample, size), expected, `UTF-8 boundary at ${size}`);
}
const atomic = render({ ...message, text: 'short', attachments: [
  { type: 'ref', path: '/tmp/' + 'p'.repeat(10000) },
  { type: 'snippet', name: 'h'.repeat(10000), content: 'small' },
] });
assert.ok(!atomic.includes('/tmp/ppp'), 'oversized reference partially rendered');
assert.ok(!atomic.includes('--- snippet:'), 'oversized attachment heading partially rendered');
assert.ok(atomic.includes('show budget-ask'), 'omitted blocks lost retrieval hint');
console.log(`${mode} aggregate message budget: ok`);
