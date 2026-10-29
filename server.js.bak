const express = require('express');
const { default: makeWASocket, useMultiFileAuthState } = require('@whiskeysockets/baileys');
const pino = require('pino');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');

const app = express();
app.use(express.json());
app.use(express.static('public'));

const configPath = path.join(__dirname, 'config.json');
let config = { workers: [], numbers: [], teamName: "STRIPPERS", currentShift: "" };

if (fs.existsSync(configPath)) {
    const saved = JSON.parse(fs.readFileSync(configPath));
    config = { ...config, ...saved };
} else {
    config.workers = [
        "Abdulrusheed Toheeb", "Bardi Emmanuel", "Yusuf Suleiman", 
        "fawaz ghost 👻 F X", "Olalekan Okesanjo", "Damilola Bello", 
        "David Favour", "Ajiwoju Michael", "Eniola Shadare"
    ];
    config.numbers = ["2348119656999", "2348037938860"];
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
}

let sock;
let isConnected = false;

async function connectWA() {
    const { state, saveCreds } = await useMultiFileAuthState('auth_info');
    sock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        logger: pino({ level: 'silent' }),
        browser: ["Mac OS", "Safari", "10.15.7"]
    });

    sock.ev.on('creds.update', saveCreds);
    sock.ev.on('connection.update', (update) => {
        const { connection } = update;
        if (connection === 'open') { isConnected = true; console.log("✅ WhatsApp Connected!"); }
        if (connection === 'close') { isConnected = false; setTimeout(connectWA, 5000); }
    });
}
connectWA();

app.get('/api/config', (req, res) => res.json(config));
app.post('/api/config', (req, res) => {
    config = { ...config, ...req.body };
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
    res.json({ success: true });
});

app.post('/api/pair', async (req, res) => {
    try {
        let phone = req.body.phone.replace(/[^0-9]/g, '');
        if (!sock) return res.status(500).json({ error: "Socket not ready" });
        let code = await sock.requestPairingCode(phone);
        res.json({ code: code });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/send', async (req, res) => {
    if (!isConnected) return res.status(500).json({ error: "WhatsApp not connected" });
    const { text } = req.body;
    try {
        for (let num of config.numbers) {
            await sock.sendMessage(num + "@s.whatsapp.net", { text });
        }
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/test-alarm', (req, res) => { exec('~/attendance-bot/alarm_worker.sh &'); res.json({ triggered: true }); });
app.post('/api/stop-alarm', (req, res) => { exec('curl -s -X POST http://127.0.0.1:4545/api/stop-alarm; termux-notification-remove 999'); res.json({ stopped: true }); });

app.listen(4545, () => console.log('🔒 Backend Running on port 4545'));
