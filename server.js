const express = require('express');
const { default: makeWASocket, DisconnectReason, Browsers, BufferJSON, initAuthCreds, proto } = require('@whiskeysockets/baileys');
const pino = require('pino');
const path = require('path');
const crypto = require('crypto');

process.on('unhandledRejection', (e) => console.log('unhandled:', e && e.message));
process.on('uncaughtException', (e) => console.log('uncaught:', e && e.message));

const TURSO_URL = (process.env.TURSO_URL || '').replace(/^libsql:\/\//, 'https://');
const TURSO_TOKEN = process.env.TURSO_TOKEN || '';
if (!TURSO_URL || !TURSO_TOKEN) { console.error('Missing TURSO_URL / TURSO_TOKEN env vars'); process.exit(1); }

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ---------- Turso over HTTP: limited concurrency + batching ----------
let active = 0;
const waiters = [];
async function limited(fn) {
    while (active >= 4) await new Promise(r => waiters.push(r));
    active++;
    try { return await fn(); }
    finally { active--; const w = waiters.shift(); if (w) w(); }
}

async function pipeline(items, timeoutMs) {
    const r = await fetch(TURSO_URL + '/v2/pipeline', {
        method: 'POST',
        signal: AbortSignal.timeout(timeoutMs),
        headers: { Authorization: 'Bearer ' + TURSO_TOKEN, 'Content-Type': 'application/json' },
        body: JSON.stringify({ requests: [
            ...items.map(it => ({ type: 'execute', stmt: { sql: it.sql, args: (it.args || []).map(a => ({ type: 'text', value: String(a) })) } })),
            { type: 'close' }
        ] })
    });
    const j = await r.json();
    if (!r.ok || !j.results) throw new Error('DB error: ' + JSON.stringify(j).slice(0, 200));
    for (const x of j.results) {
        if (x.type === 'error') throw new Error('DB error: ' + JSON.stringify(x).slice(0, 200));
    }
    return j.results;
}

async function run(items, timeoutMs = 20000) {
    let lastErr;
    for (let i = 0; i < 3; i++) {
        try { return await limited(() => pipeline(items, timeoutMs)); }
        catch (e) { lastErr = e; console.log('DB retry', i + 1, e.message); await sleep(500 * (i + 1)); }
    }
    throw lastErr;
}

async function sql(stmt, args = []) {
    const results = await run([{ sql: stmt, args }]);
    return results[0].response.result.rows.map(row => row.map(c => c.value));
}

// write queue: many small writes become a few big requests
const writeQ = [];
let flushing = false;
function enqueue(item) {
    return new Promise((resolve, reject) => {
        writeQ.push({ item, resolve, reject });
        if (!flushing) { flushing = true; setTimeout(flush, 150); }
    });
}
async function flush() {
    while (writeQ.length) {
        const chunk = writeQ.splice(0, 100);
        try { await run(chunk.map(c => c.item)); chunk.forEach(c => c.resolve()); }
        catch (e) { chunk.forEach(c => c.reject(e)); }
    }
    flushing = false;
}

const cache = new Map();
const ck = (d, k) => d + '|' + k;

async function kvGet(d, k) {
    const key = ck(d, k);
    if (cache.has(key)) return cache.get(key);
    const rows = await sql('SELECT v FROM kv WHERE device=? AND k=?', [d, k]);
    const v = rows.length ? rows[0][0] : null;
    cache.set(key, v);
    return v;
}
async function kvGetMany(d, ks) {
    const miss = ks.filter(k => !cache.has(ck(d, k)));
    for (let i = 0; i < miss.length; i += 50) {
        const part = miss.slice(i, i + 50);
        const rows = await sql(`SELECT k, v FROM kv WHERE device=? AND k IN (${part.map(() => '?').join(',')})`, [d, ...part]);
        const found = new Map(rows.map(r => [r[0], r[1]]));
        for (const k of part) cache.set(ck(d, k), found.has(k) ? found.get(k) : null);
    }
    return ks.map(k => cache.get(ck(d, k)));
}
async function kvSet(d, k, v) {
    cache.set(ck(d, k), v);
    await enqueue({ sql: 'INSERT INTO kv(device,k,v) VALUES(?,?,?) ON CONFLICT(device,k) DO UPDATE SET v=excluded.v', args: [d, k, v] });
}
async function kvDel(d, k) {
    cache.set(ck(d, k), null);
    await enqueue({ sql: 'DELETE FROM kv WHERE device=? AND k=?', args: [d, k] });
}
async function kvWipeAuth(d) {
    for (const key of [...cache.keys()]) if (key.startsWith(d + '|a:')) cache.delete(key);
    await enqueue({ sql: "DELETE FROM kv WHERE device=? AND k LIKE 'a:%'", args: [d] });
}

// WhatsApp auth state stored in the database instead of files
async function useDBAuthState(id) {
    const parse = (v) => (v ? JSON.parse(v, BufferJSON.reviver) : null);
    const wr = (k, data) => kvSet(id, 'a:' + k, JSON.stringify(data, BufferJSON.replacer));
    const rm = (k) => kvDel(id, 'a:' + k);
    const creds = parse(await kvGet(id, 'a:creds')) || initAuthCreds();
    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const vals = await kvGetMany(id, ids.map(i => 'a:' + type + '-' + i));
                    const data = {};
                    ids.forEach((i, n) => {
                        let v = parse(vals[n]);
                        if (type === 'app-state-sync-key' && v) v = proto.Message.AppStateSyncKeyData.fromObject(v);
                        data[i] = v;
                    });
                    return data;
                },
                set: async (data) => {
                    const tasks = [];
                    for (const type in data) for (const i in data[type]) {
                        const v = data[type][i];
                        tasks.push(v ? wr(type + '-' + i, v) : rm(type + '-' + i));
                    }
                    await Promise.all(tasks);
                }
            }
        },
        saveCreds: () => wr('creds', creds)
    };
}

// ---------- App ----------
const app = express();
app.use(express.json({ limit: '100kb' }));

const ID_RE = /^[a-f0-9-]{36}$/;
const DEFAULTS = { workers: [], numbers: [], teamName: 'MY TEAM', currentShift: '' };

async function loadConfig(id) {
    const v = await kvGet(id, 'config');
    return { ...DEFAULTS, ...(v ? JSON.parse(v) : {}) };
}

function cleanNumber(raw) {
    let n = String(raw || '').replace(/[^0-9]/g, '');
    if (n.startsWith('2340')) n = '234' + n.slice(4);
    else if (n.length === 11 && n.startsWith('0')) n = '234' + n.slice(1);
    return n;
}

// phone number OR group id (123456789-123@g.us)
function cleanRecipient(raw) {
    const t = String(raw || '').trim();
    if (/^[0-9-]+@g\.us$/.test(t)) return t;
    return cleanNumber(t);
}

function getCookie(req, name) {
    const hit = (req.headers.cookie || '').split(';').map(s => s.trim()).find(s => s.startsWith(name + '='));
    return hit ? decodeURIComponent(hit.slice(name.length + 1)) : null;
}

app.use((req, res, next) => {
    let id = req.query.device;
    if (!(typeof id === 'string' && ID_RE.test(id))) id = getCookie(req, 'device');
    if (!id || !ID_RE.test(id)) id = crypto.randomUUID();
    res.cookie('device', id, { maxAge: 31536000000, sameSite: 'lax', httpOnly: true });
    req.deviceId = id;
    next();
});
app.use(express.static(path.join(__dirname, 'public'), { etag: false, lastModified: false, setHeaders: (res) => res.setHeader('Cache-Control', 'no-store') }));

// ---------- WhatsApp sessions (one per device) ----------
const sessions = new Map();

async function startWA(id) {
    let s = sessions.get(id);
    if (!s) { s = { sock: null, connected: false, starting: false }; sessions.set(id, s); }
    if (s.starting || s.connected) return s;
    s.starting = true;
    try {
        const { state, saveCreds } = await useDBAuthState(id);
        const sock = makeWASocket({
            auth: state,
            printQRInTerminal: false,
            logger: pino({ level: 'silent' }),
            browser: Browsers.ubuntu('Chrome')
        });
        s.sock = sock;
        sock.ev.on('creds.update', () => saveCreds().catch(e => console.log('saveCreds failed:', e.message)));
        sock.ev.on('connection.update', (u) => {
            if (s.sock !== sock) return;
            if (u.connection === 'open') { s.connected = true; console.log('WA connected', id.slice(0, 8)); }
            if (u.connection === 'close') {
                s.connected = false; s.starting = false;
                const code = u.lastDisconnect?.error?.output?.statusCode;
                console.log('WA closed', id.slice(0, 8), code);
                if (code === DisconnectReason.loggedOut) {
                    s.sock = null;
                    kvWipeAuth(id).catch(() => {});
                } else if (state.creds.me || state.creds.registered) {
                    setTimeout(() => startWA(id).catch(() => {}), 5000);
                } else {
                    s.sock = null;
                }
            }
        });
    } catch (e) { s.starting = false; throw e; }
    return s;
}

// ---------- API ----------
app.get('/api/config', async (req, res) => {
    try { res.json(await loadConfig(req.deviceId)); }
    catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/config', async (req, res) => {
    try {
        const b = req.body || {};
        const cfg = await loadConfig(req.deviceId);
        if (Array.isArray(b.workers)) cfg.workers = b.workers.map(w => String(w).trim().slice(0, 60)).filter(Boolean).slice(0, 200);
        if (Array.isArray(b.numbers)) cfg.numbers = [...new Set(b.numbers.map(cleanRecipient).filter(n => n.length >= 10))].slice(0, 50);
        if (typeof b.teamName === 'string') cfg.teamName = b.teamName.trim().slice(0, 40);
        if (typeof b.currentShift === 'string') cfg.currentShift = b.currentShift.slice(0, 20);
        await kvSet(req.deviceId, 'config', JSON.stringify(cfg));
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/status', (req, res) => {
    const s = sessions.get(req.deviceId);
    res.json({ connected: !!(s && s.connected) });
});

app.get('/api/health', async (req, res) => {
    try {
        const rows = await sql('SELECT COUNT(DISTINCT device) FROM kv');
        res.json({
            ok: true, db: 'turso', devices: Number(rows[0][0]),
            linked: [...sessions.values()].filter(x => x.connected).length,
            pendingWrites: writeQ.length, uptimeSec: Math.round(process.uptime())
        });
    } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/api/pair', async (req, res) => {
    try {
        const id = req.deviceId;
        const phone = cleanNumber(req.body && req.body.phone);
        if (phone.length < 10) return res.status(400).json({ error: 'Enter your number with country code, no + and no 0 after it' });
        let s = sessions.get(id);
        if (s && s.connected) return res.status(400).json({ error: 'Already connected' });
        if (s && s.sock) { const old = s.sock; s.sock = null; s.starting = false; try { old.end(undefined); } catch {} }
        await kvWipeAuth(id);
        s = await startWA(id);
        await sleep(3000);
        const code = await s.sock.requestPairingCode(phone);
        res.json({ code });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/unlink', async (req, res) => {
    const s = sessions.get(req.deviceId);
    try { if (s && s.sock) await s.sock.logout(); } catch {}
    try { await kvWipeAuth(req.deviceId); } catch {}
    sessions.delete(req.deviceId);
    res.json({ success: true });
});

app.get('/api/groups', async (req, res) => {
    const st = sessions.get(req.deviceId);
    if (!st || !st.connected) return res.status(500).json({ error: 'WhatsApp not connected' });
    try {
        const g = await st.sock.groupFetchAllParticipating();
        const list = Object.values(g).map(x => ({ id: x.id, name: x.subject || x.id }));
        list.sort((a, b) => a.name.localeCompare(b.name));
        res.json(list);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/send', async (req, res) => {
    const id = req.deviceId;
    const s = sessions.get(id);
    if (!s || !s.connected) return res.status(500).json({ error: 'WhatsApp not connected' });
    try {
        const cfg = await loadConfig(id);
        if (!cfg.numbers.length) return res.status(400).json({ error: 'No receiver numbers added' });
        const text = String((req.body && req.body.text) || '').slice(0, 4000);
        if (!text) return res.status(400).json({ error: 'Empty message' });
        for (const num of cfg.numbers) await s.sock.sendMessage(num.endsWith('@g.us') ? num : num + '@s.whatsapp.net', { text });
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/test-alarm', (req, res) => res.json({ triggered: false }));
app.post('/api/stop-alarm', (req, res) => res.json({ stopped: true }));

// ---------- Boot ----------
(async () => {
    await sql('CREATE TABLE IF NOT EXISTS kv (device TEXT NOT NULL, k TEXT NOT NULL, v TEXT, PRIMARY KEY (device, k))');
    app.listen(process.env.PORT || 4545, () => console.log('Backend running'));
    const rows = await sql("SELECT device FROM kv WHERE k='a:creds'");
    for (const [id] of rows) {
        if (!ID_RE.test(id)) continue;
        try { await startWA(id); } catch (e) { console.log('resume failed', id.slice(0, 8), e.message); }
        await sleep(1000);
    }
})().catch(e => { console.error('Boot failed', e); process.exit(1); });
