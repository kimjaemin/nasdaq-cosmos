// NASDAQ Cosmos relay
// One Durable Object keeps a single connection to Finnhub and fans every
// message out to all connected viewers (phones, browsers, TouchDesigner).
// The Finnhub key lives only here, as the FINNHUB_KEY secret.

const SYMBOLS = ['AAPL', 'NVDA', 'TSLA', 'MSFT', 'AMZN'];
const IDLE_CLOSE_MS = 30000;   // drop the upstream 30 s after the last viewer leaves

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected a WebSocket upgrade', { status: 426 });
      }
      const stub = env.RELAY.get(env.RELAY.idFromName('nasdaq'));
      return stub.fetch(request);
    }
    if (url.pathname === '/status') {
      const stub = env.RELAY.get(env.RELAY.idFromName('nasdaq'));
      return stub.fetch(new Request(new URL('/status', request.url)));
    }
    return new Response('NASDAQ Cosmos relay is running. Connect to /ws', {
      headers: { 'content-type': 'text/plain; charset=utf-8', 'access-control-allow-origin': '*' },
    });
  },
};

export class Relay {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.clients = new Set();
    this.upstream = null;
    this.connecting = false;
    this.idleTimer = null;
    this.last = {};          // last trade price per symbol, sent to new viewers
    this.trades = 0;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/status') {
      return Response.json({
        viewers: this.clients.size,
        upstream: !!this.upstream,
        trades: this.trades,
        last: this.last,
      }, { headers: { 'access-control-allow-origin': '*' } });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    this.clients.add(server);
    clearTimeout(this.idleTimer);

    server.send(JSON.stringify({ type: 'hello', last: this.last, upstream: !!this.upstream }));
    const drop = () => this.dropClient(server);
    server.addEventListener('close', drop);
    server.addEventListener('error', drop);
    server.addEventListener('message', (ev) => {
      if (ev.data === 'ping') { try { server.send('pong'); } catch (e) { drop(); } }
    });

    this.ensureUpstream();
    return new Response(null, { status: 101, webSocket: client });
  }

  dropClient(ws) {
    this.clients.delete(ws);
    if (this.clients.size === 0) {
      clearTimeout(this.idleTimer);
      this.idleTimer = setTimeout(() => {
        if (this.clients.size === 0 && this.upstream) {
          try { this.upstream.close(1000, 'no viewers'); } catch (e) {}
          this.upstream = null;
        }
      }, IDLE_CLOSE_MS);
    }
  }

  broadcast(text) {
    for (const ws of this.clients) {
      try { ws.send(text); } catch (e) { this.clients.delete(ws); }
    }
  }

  async ensureUpstream() {
    if (this.upstream || this.connecting) return;
    this.connecting = true;
    try {
      const resp = await fetch('https://ws.finnhub.io/?token=' + encodeURIComponent(this.env.FINNHUB_KEY), {
        headers: { Upgrade: 'websocket' },
      });
      const ws = resp.webSocket;
      if (!ws) throw new Error('Finnhub refused the connection (HTTP ' + resp.status + ')');
      ws.accept();
      this.upstream = ws;
      for (const s of SYMBOLS) ws.send(JSON.stringify({ type: 'subscribe', symbol: s }));
      this.broadcast(JSON.stringify({ type: 'relay', upstream: true }));

      ws.addEventListener('message', (ev) => {
        if (typeof ev.data !== 'string') return;
        if (ev.data.indexOf('"trade"') !== -1) {
          try {
            const m = JSON.parse(ev.data);
            for (const t of m.data || []) { this.last[t.s] = t.p; this.trades++; }
          } catch (e) {}
          this.broadcast(ev.data);
        } else if (ev.data.indexOf('"error"') !== -1) {
          this.broadcast(ev.data);
        }
      });
      const lost = () => {
        if (this.upstream !== ws) return;
        this.upstream = null;
        this.broadcast(JSON.stringify({ type: 'relay', upstream: false }));
        if (this.clients.size) setTimeout(() => this.ensureUpstream(), 3000);
      };
      ws.addEventListener('close', lost);
      ws.addEventListener('error', lost);
    } catch (e) {
      this.upstream = null;
      this.broadcast(JSON.stringify({ type: 'relay_error', msg: String(e && e.message || e) }));
      if (this.clients.size) setTimeout(() => this.ensureUpstream(), 10000);
    } finally {
      this.connecting = false;
    }
  }
}
