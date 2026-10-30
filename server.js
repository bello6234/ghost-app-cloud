const express = require('express');
const { default: makeWASocket, DisconnectReason, Browsers, BufferJSON, initAuthCreds, proto } = require('@whiskeysockets/baileys');
const pino = require('pino');
const path = require('path');
const crypto = require('crypto');

const TURSO_URL = (process.env.TURSO_URL || '').replace(/^libsql:\/\//, 'https://');
const TURSO_TOKEN = process.env.TURSO_TOKEN || '';
if (!TURSO_URL || !TURSO_TOKEN) { console.error('Missing TURSO_URL / TURSO_TOKEN env vars'); process.exit(1); }

// ---------- Turso over HTTP (no dependencies) ----------
async function sql(stmt, args = []) {
    const r = await fetch(TURSO_URL + '/v2/pipeline', {
        method: 'POST',
        headers: { Authorization: 'Bearer ' + TURSO_TOKEN, 'Content-Type': 'application/json' },
        body: JSON.stringify({ requests: [
            { type: 'execute', stmt: { sql: stmt, args: args.map(a => ({ type: 'text', value: String(a) })) } },
            { type: 'close' }
        ] })
    });
    const j = await r.json();
    const first = j.results && j.results[0];
    if (!r.ok || !first || first.type === 'error') throw new Error('DB error: ' + JSON.stringify(first || j).slice(0, 200));
    return first.response.result.rows.map(row => row.map(c => c.value));
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
async function kvSet(d, k, v) {
    cache.set(ck(d, k), v);
    await sql('INSERT INTO kv(device,k,v) VALUES(?,?,?) ON CONFLICT(device,k) DO UPDATE SET v=excluded.v', [d, k, v]);
}
async function kvDel(d, k) {
    cache.set(ck(d, k), null);
    await sql('DELETE FROM kv WHERE device=? AND k=?', [d, k]);
}
async function kvWipeAuth(d) {
    for (const key of [...cache.keys()]) if (key.startsWith(d + '|a:')) cache.delete(key);
    await sql("DELETE FROM kv WHERE device=? AND k LIKE 'a:%'", [d]);
}

// WhatsApp auth state stored in the database instead of files
async function useDBAuthState(id) {
    const rd = async (k) => { const v = await kvGet(id, 'a:' + k); return v ? JSON.parse(v, BufferJSON.reviver) : null; };
    const wr = (k, data) => kvSet(id, 'a:' + k, JSON.stringify(data, BufferJSON.replacer));
    const rm = (k) => kvDel(id, 'a:' + k);
    const creds = (await rd('creds')) || initAuthCreds();
    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    await Promise.all(ids.map(async (i) => {
                        let v = await rd(`${type}-${i}`);
                        if (type === 'app-state-sync-key' && v) v = proto.Message.AppStateSyncKeyData.fromObject(v);
                        data[i] = v;
                    }));
                    return data;
                },
                set: async (data) => {
                    const tasks = [];
                    for (const type in data) for (const i in data[type]) {
                        const v = data[type][i];
                        tasks.push(v ? wr(`${type}-${i}`, v) : rm(`${type}-${i}`));
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
app.use(express.static(path.join(__dirname, 'public')));

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
        sock.ev.on('creds.update', () => saveCreds().catch(e => console.log('saveCreds', e.message)));
        sock.ev.on('connection.update', (u) => {
            if (s.sock !== sock) return;
            if (u.connection === 'open') { s.connected = true; console.log('WA connected', id); }
            if (u.connection === 'close') {
                s.connected = false; s.starting = false;
                const code = u.lastDisconnect?.error?.output?.statusCode;
                if (code === DisconnectReason.loggedOut) {
                    s.sock = null;
                    kvWipeAuth(id).catch(() => {});
                } else if (state.creds.registered) {
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
        if (Array.isArray(b.numbers)) cfg.numbers = [...new Set(b.numbers.map(cleanNumber).filter(n => n.length >= 10))].slice(0, 50);
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
        const rows = await sql("SELECT COUNT(DISTINCT device) FROM kv");
        res.json({ ok: true, db: 'turso', devices: Number(rows[0][0]), uptimeSec: Math.round(process.uptime()) });
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
        await new Promise(r => setTimeout(r, 3000));
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

app.post('/api/send', async (req, res) => {
    const id = req.deviceId;
    const s = sessions.get(id);
    if (!s || !s.connected) return res.status(500).json({ error: 'WhatsApp not connected' });
    try {
        const cfg = await loadConfig(id);
        if (!cfg.numbers.length) return res.status(400).json({ error: 'No receiver numbers added' });
        const text = String((req.body && req.body.text) || '').slice(0, 4000);
        if (!text) return res.status(400).json({ error: 'Empty message' });
        for (const num of cfg.numbers) await s.sock.sendMessage(num + '@s.whatsapp.net', { text });
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
        try { await startWA(id); } catch (e) { console.log('resume failed', id, e.message); }
        await new Promise(r => setTimeout(r, 1000));
    }
})().catch(e => { console.error('Boot failed', e); process.exit(1); });
