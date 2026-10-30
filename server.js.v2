const express = require('express');
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, Browsers } = require('@whiskeysockets/baileys');
const pino = require('pino');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });

const app = express();
app.use(express.json({ limit: '100kb' }));

const ID_RE = /^[a-f0-9-]{36}$/;
const DEFAULTS = { workers: [], numbers: [], teamName: 'MY TEAM', currentShift: '' };

const devDir = (id) => path.join(DATA_DIR, id);
const authDir = (id) => path.join(devDir(id), 'auth');
const cfgPath = (id) => path.join(devDir(id), 'config.json');

function loadConfig(id) {
    try { return { ...DEFAULTS, ...JSON.parse(fs.readFileSync(cfgPath(id), 'utf8')) }; }
    catch { return { ...DEFAULTS }; }
}
function saveConfig(id, cfg) {
    fs.mkdirSync(devDir(id), { recursive: true });
    const tmp = cfgPath(id) + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(cfg, null, 2));
    fs.renameSync(tmp, cfgPath(id));
}

// 2340909... -> 234909... ; 0909... -> 234909...
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

// Identify the device: ?device=ID from the app, else cookie, else new ID
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
        const { state, saveCreds } = await useMultiFileAuthState(authDir(id));
        const sock = makeWASocket({
            auth: state,
            printQRInTerminal: false,
            logger: pino({ level: 'silent' }),
            browser: Browsers.ubuntu('Chrome')
        });
        s.sock = sock;
        sock.ev.on('creds.update', saveCreds);
        sock.ev.on('connection.update', (u) => {
            if (s.sock !== sock) return;
            if (u.connection === 'open') { s.connected = true; console.log('WA connected', id); }
            if (u.connection === 'close') {
                s.connected = false; s.starting = false;
                const code = u.lastDisconnect?.error?.output?.statusCode;
                if (code === DisconnectReason.loggedOut) {
                    fs.rmSync(authDir(id), { recursive: true, force: true });
                    s.sock = null;
                } else if (state.creds.registered) {
                    setTimeout(() => startWA(id).catch(() => {}), 5000);
                } else {
                    s.sock = null; // unpaired: wait for user to request a code
                }
            }
        });
    } catch (e) { s.starting = false; throw e; }
    return s;
}

// ---------- API ----------
app.get('/api/config', (req, res) => res.json(loadConfig(req.deviceId)));

app.post('/api/config', (req, res) => {
    const b = req.body || {};
    const cfg = loadConfig(req.deviceId);
    if (Array.isArray(b.workers)) {
        cfg.workers = b.workers.map(w => String(w).trim().slice(0, 60)).filter(Boolean).slice(0, 200);
    }
    if (Array.isArray(b.numbers)) {
        cfg.numbers = [...new Set(b.numbers.map(cleanNumber).filter(n => n.length >= 10))].slice(0, 50);
    }
    if (typeof b.teamName === 'string') cfg.teamName = b.teamName.trim().slice(0, 40);
    if (typeof b.currentShift === 'string') cfg.currentShift = b.currentShift.slice(0, 20);
    saveConfig(req.deviceId, cfg);
    res.json({ success: true });
});

app.get('/api/status', (req, res) => {
    const s = sessions.get(req.deviceId);
    res.json({ connected: !!(s && s.connected) });
});

app.post('/api/pair', async (req, res) => {
    try {
        const id = req.deviceId;
        const phone = cleanNumber(req.body && req.body.phone);
        if (phone.length < 10) return res.status(400).json({ error: 'Enter your number with country code, no + and no 0 after it' });
        let s = sessions.get(id);
        if (s && s.connected) return res.status(400).json({ error: 'Already connected' });
        if (s && s.sock) { const old = s.sock; s.sock = null; s.starting = false; try { old.end(undefined); } catch {} }
        fs.rmSync(authDir(id), { recursive: true, force: true });
        s = await startWA(id);
        await new Promise(r => setTimeout(r, 3000));
        const code = await s.sock.requestPairingCode(phone);
        res.json({ code });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/unlink', async (req, res) => {
    const s = sessions.get(req.deviceId);
    try { if (s && s.sock) await s.sock.logout(); } catch {}
    fs.rmSync(authDir(req.deviceId), { recursive: true, force: true });
    sessions.delete(req.deviceId);
    res.json({ success: true });
});

app.post('/api/send', async (req, res) => {
    const id = req.deviceId;
    const s = sessions.get(id);
    if (!s || !s.connected) return res.status(500).json({ error: 'WhatsApp not connected' });
    const cfg = loadConfig(id);
    if (!cfg.numbers.length) return res.status(400).json({ error: 'No receiver numbers added' });
    const text = String((req.body && req.body.text) || '').slice(0, 4000);
    if (!text) return res.status(400).json({ error: 'Empty message' });
    try {
        for (const num of cfg.numbers) await s.sock.sendMessage(num + '@s.whatsapp.net', { text });
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// old local-alarm routes: no-ops on the cloud server
app.post('/api/test-alarm', (req, res) => res.json({ triggered: false }));
app.post('/api/stop-alarm', (req, res) => res.json({ stopped: true }));

// resume every paired device after a restart
(async () => {
    for (const id of fs.readdirSync(DATA_DIR)) {
        if (ID_RE.test(id) && fs.existsSync(path.join(authDir(id), 'creds.json'))) {
            try { await startWA(id); } catch (e) { console.log('resume failed', id, e.message); }
            await new Promise(r => setTimeout(r, 1000));
        }
    }
})();

app.listen(process.env.PORT || 4545, () => console.log('Backend running'));
