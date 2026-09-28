// Checks the opencode adapter's delivery/retry logic with a fake socket, fake timers
// and a fake `opencode api`. Run by TestAdapterScripts: node opencode_check.mjs <tui.js>

import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { EventEmitter } from 'node:events';
import { StringDecoder } from 'node:string_decoder';
import path from 'node:path';
import os from 'node:os';

const filename = process.argv[2];
const source = fs.readFileSync(filename, 'utf8')
  .replace(/^import .*;\n/gm, '')
  .replace('export default { id: "agent-mesh", setup };', '');
const tick = () => new Promise(resolve => setImmediate(resolve));

function harness() {
  const sockets = [], timers = new Set(), calls = [], notices = [];
  const context = vm.createContext({
    process: { env: {}, execPath: 'node', pid: 1 }, path, os,
    net: { createConnection: () => {
      const socket = new EventEmitter();
      socket.frames = [];
      socket.write = data => {
        try { socket.frames.push(JSON.parse(data)); }
        catch (err) { assert.fail(`adapter wrote invalid JSON: ${data} (${err.message})`); }
      };
      socket.end = () => { socket.ended = true; };
      // Like net.Socket: with an encoding, 'data' gets strings decoded across chunks.
      socket.setEncoding = enc => { socket.decoder = new StringDecoder(enc); };
      socket.bytes = buf => socket.emit('data', socket.decoder ? socket.decoder.write(buf) : buf);
      sockets.push(socket);
      return socket;
    } },
    execFile: (_bin, args, _opts, callback) => {
      if (args[1] !== 'session.synthetic') callback(null);
      else calls.push({ args, callback });
    },
    spawn: () => { throw new Error('unexpected daemon start'); },
    setTimeout: (fn, ms) => {
      const timer = { fn, ms, unref() { return this; } };
      timers.add(timer);
      return timer;
    },
    clearTimeout: timer => timers.delete(timer),
    console,
  });
  vm.runInContext(source, context);
  const start = vm.runInContext('connectSession', context);
  const close = start('session-one', '/tmp', message => notices.push(message));
  sockets[0].emit('connect');
  return {
    sockets, timers, calls, notices, close, context,
    fire(ms) {
      const timer = [...timers].find(t => t.ms === ms);
      assert(timer, `missing ${ms}ms timer`);
      timers.delete(timer); timer.fn();
    },
    send(socket, id = 'm1') {
      socket.emit('data', JSON.stringify({ event: 'message', message: { id, from: 'peer', text: 'hello' } }) + '\n');
    },
  };
}

{
  const h = harness();
  h.send(h.sockets[0]); await tick();
  assert.equal(h.calls.length, 1);
  h.calls[0].callback(new Error('temporary')); await tick();
  h.send(h.sockets[0]); await tick();
  assert.equal(h.calls.length, 1, 'replay starts duplicate API work');
  h.fire(1000); await tick();
  assert.equal(h.calls.length, 2);
  h.calls[1].callback(null); await tick();
  assert.equal(h.sockets[0].frames.filter(f => f.op === 'ack').length, 1);
  assert.equal(h.notices.length, 1);
  h.close();
  console.log('PASS: retry without reconnect; pending replay dedup; ACK only after success');
}
{
  const h = harness();
  h.send(h.sockets[0]); await tick();
  h.sockets[0].emit('close');
  h.fire(250);
  h.sockets[1].emit('connect');
  h.send(h.sockets[1]); await tick();
  assert.equal(h.calls.length, 1);
  h.calls[0].callback(new Error('temporary')); await tick();
  h.fire(1000); await tick();
  h.calls[1].callback(null); await tick();
  assert.equal(h.sockets[1].frames.filter(f => f.op === 'ack').length, 1);
  h.close();
  console.log('PASS: reconnect replay during an in-flight failure still retries on new connection');
}
{
  const h = harness();
  h.send(h.sockets[0]); await tick();
  h.close();
  h.calls[0].callback(null); await tick();
  assert.equal(h.sockets[0].frames.filter(f => f.op === 'ack').length, 0);
  assert.equal(h.calls.length, 1);
  console.log('PASS: late result after switch cannot ACK old mail or start additional work');
}
{
  const h = harness();
  h.send(h.sockets[0]); await tick();
  h.calls[0].callback(new Error('temporary')); await tick();
  h.close();
  assert.equal(h.timers.size, 0, 'session close leaves a retry sleep timer alive');
  console.log('PASS: close clears retry timers');
}
{
  const h = harness();
  const text = 'çok 😀 ğ';
  const line = Buffer.from(JSON.stringify({ event: 'message', message: { id: 'u1', from: 'peer', text } }) + '\n');
  const cut = line.indexOf(Buffer.from('😀')) + 2; // inside the 4-byte emoji
  h.sockets[0].bytes(line.subarray(0, cut));
  h.sockets[0].bytes(line.subarray(cut));
  await tick();
  assert.equal(h.calls.length, 1, 'frame split inside a rune was dropped');
  const raw = h.calls[0].args[h.calls[0].args.indexOf('-d') + 1];
  let body;
  try { body = JSON.parse(raw).text; }
  catch (err) { assert.fail(`adapter passed invalid JSON to opencode api: ${raw} (${err.message})`); }
  assert.ok(body.includes(text), `rune corrupted: ${body}`);
  h.calls[0].callback(null); await tick();
  h.close();
  console.log('PASS: a frame split inside a UTF-8 rune is decoded intact');
}
