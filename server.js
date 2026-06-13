const express = require('express');
const cors = require('cors');
const pako = require('pako');
const WebSocket = require('ws');
const { createProxyMiddleware } = require('http-proxy-middleware');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());

// Global State
let liveState = {};
let brokerRunning = false;

// WebSocket Server for Clients (Angular App)
const wss = new WebSocket.Server({ noServer: true });

// Helper: Deep Merge
function deepObjectMerge(original = {}, modifier) {
    if (!modifier) return original;
    const copy = { ...original };
    for (const [key, value] of Object.entries(modifier)) {
        const valueIsObject = typeof value === 'object' && !Array.isArray(value) && value !== null;
        if (valueIsObject && Object.keys(value).length) {
            copy[key] = deepObjectMerge(copy[key], value);
        } else {
            copy[key] = value;
        }
    }
    return copy;
}

const SIGNALR_CORE_URL = 'https://livetiming.formula1.com/signalrcore';
const F1_ORIGIN = 'https://www.formula1.com';
const RECORD_SEPARATOR = '\x1e';

const SUBSCRIBE_CHANNELS = [
    'Heartbeat', 'CarData.z', 'Position.z', 'ExtrapolatedClock', 'TimingStats',
    'TimingAppData', 'WeatherData', 'TrackStatus', 'DriverList',
    'RaceControlMessages', 'SessionInfo', 'SessionData', 'LapCount', 'TimingData',
    'TeamRadio', 'TyreStintSeries', 'TyreStintSeries.z'
];

const F1_HEADERS = {
    'User-Agent': 'BestHTTP',
    'Origin': F1_ORIGIN,
    'Referer': `${F1_ORIGIN}/`,
};

function getResponseCookies(response) {
    if (typeof response.headers.getSetCookie === 'function') {
        return response.headers.getSetCookie().map((cookie) => cookie.split(';')[0]);
    }
    const raw = response.headers.get('set-cookie');
    return raw ? [raw.split(';')[0]] : [];
}

function buildFieldUpdate(field, value) {
    if (field.endsWith('.z') && typeof value === 'string') {
        return { [field.split('.')[0]]: decompress(value) };
    }
    return { [field]: value };
}

// Helper: Decompress (Protegido contra JSONs corruptos)
function decompress(data) {
    try {
        if (!data) return {};
        const buffer = Buffer.from(data, 'base64');
        const inflated = pako.inflateRaw(buffer, { to: 'string' });
        if (!inflated || !inflated.trim()) return {};
        return JSON.parse(inflated);
    } catch (e) {
        console.error('[Decompress Error] Error de descompresion:', e.message);
        return {};
    }
}

function parseSignalRFrames(rawData) {
    return rawData.split(RECORD_SEPARATOR).filter((frame) => frame.trim().length > 1);
}

function broadcastUpdates(updates) {
    if (updates.length === 0) return;
    const payload = JSON.stringify(updates.length === 1 ? updates[0] : updates);
    wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    });
}

function processSignalRFrame(frame, ws, state) {
    let parsed;
    try {
        parsed = JSON.parse(frame);
    } catch {
        return;
    }

    if (!state.handshakeComplete && Object.keys(parsed).length === 0) {
        state.handshakeComplete = true;
        brokerRunning = true;
        ws.send(JSON.stringify({
            type: 1,
            target: 'Subscribe',
            arguments: [SUBSCRIBE_CHANNELS],
            invocationId: '1',
        }) + RECORD_SEPARATOR);
        return;
    }

    const updates = [];

    if (parsed.type === 3 && parsed.result && typeof parsed.result === 'object') {
        const bulkUpdate = {};
        for (const [field, value] of Object.entries(parsed.result)) {
            Object.assign(bulkUpdate, buildFieldUpdate(field, value));
        }
        liveState = deepObjectMerge(liveState, bulkUpdate);
        updates.push(bulkUpdate);
    } else if (parsed.type === 1 && parsed.target === 'feed' && parsed.arguments?.length >= 2) {
        const [field, value] = parsed.arguments;
        const update = buildFieldUpdate(field, value);
        liveState = deepObjectMerge(liveState, update);
        updates.push(update);
    }

    broadcastUpdates(updates);
}

async function negotiateF1Connection() {
    const cookieParts = [];

    const optionsResponse = await fetch(`${SIGNALR_CORE_URL}/negotiate?negotiateVersion=1`, {
        method: 'OPTIONS',
        headers: F1_HEADERS,
    });
    cookieParts.push(...getResponseCookies(optionsResponse));

    const negotiateResponse = await fetch(`${SIGNALR_CORE_URL}/negotiate?negotiateVersion=1`, {
        method: 'POST',
        headers: {
            ...F1_HEADERS,
            'Content-Length': '0',
            ...(cookieParts.length ? { Cookie: cookieParts.join('; ') } : {}),
        },
    });

    const textData = await negotiateResponse.text();
    if (!negotiateResponse.ok) {
        throw new Error(`Negotiate HTTP ${negotiateResponse.status}`);
    }
    if (!textData || !textData.trim()) {
        throw new Error('Negotiate returned empty body');
    }

    const data = JSON.parse(textData);
    if (!data.connectionToken) {
        throw new Error('connectionToken missing in negotiate response');
    }

    cookieParts.push(...getResponseCookies(negotiateResponse));
    return {
        connectionToken: data.connectionToken,
        cookie: [...new Set(cookieParts)].join('; '),
    };
}

// Broker Core: Negociación y conexión con F1 (SignalR Core)
async function startF1Broker() {
    try {
        console.log('[Broker] Negotiating with F1 SignalR Core...');
        const { connectionToken, cookie } = await negotiateF1Connection();

        const wsUrl = `wss://livetiming.formula1.com/signalrcore?id=${encodeURIComponent(connectionToken)}`;
        const ws = new WebSocket(wsUrl, {
            headers: {
                ...F1_HEADERS,
                ...(cookie ? { Cookie: cookie } : {}),
            },
        });

        const connectionState = { handshakeComplete: false };

        ws.on('open', () => {
            console.log('[Broker] F1 WebSocket connected, sending handshake...');
            ws.send(JSON.stringify({ protocol: 'json', version: 1 }) + RECORD_SEPARATOR);
        });

        ws.on('message', (data) => {
            for (const frame of parseSignalRFrames(data.toString())) {
                processSignalRFrame(frame, ws, connectionState);
            }
        });

        ws.on('close', () => {
            console.log('[Broker] F1 Connection closed. Reconnecting...');
            brokerRunning = false;
            setTimeout(startF1Broker, 10000);
        });

        ws.on('error', (err) => console.error('[Broker] F1 WS Error:', err.message));
    } catch (err) {
        console.error('[Broker] Fatal Error:', err.message);
        brokerRunning = false;
        setTimeout(startF1Broker, 10000);
    }
}

// Health Check API
app.get('/', (req, res) => {
    res.json({ status: 'up', message: 'F1 Broker is alive' });
});

// HTTP/WS proxy for negotiate and static endpoints
const f1Proxy = createProxyMiddleware({
    target: 'https://livetiming.formula1.com',
    changeOrigin: true,
    ws: true,
    pathRewrite: { '^/f1-api': '' },
    headers: { 'User-Agent': 'BestHTTP' },
    onProxyReq: (proxyReq) => {
        proxyReq.setHeader('User-Agent', 'BestHTTP');
    },
    onProxyRes: (proxyRes) => {
        proxyRes.headers['Access-Control-Allow-Origin'] = '*';
        proxyRes.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS';
        proxyRes.headers['Access-Control-Allow-Headers'] = '*';
    }
});

app.use('/f1-api', f1Proxy);

const motogpProxy = createProxyMiddleware({
    target: 'https://api.motogp.pulselive.com',
    changeOrigin: true,
    pathRewrite: { '^/motogp-api': '' },
    headers: {
        'Origin': 'https://www.motogp.com',
        'Referer': 'https://www.motogp.com/'
    }
});

app.use('/motogp-api', motogpProxy);

app.get('/health', (req, res) => {
    res.json({
        status: brokerRunning ? 'running' : 'starting',
        clients: wss.clients.size,
        timestamp: new Date().toISOString()
    });
});

const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`[Broker] Server actively listening on 0.0.0.0:${PORT}`);
    console.log(`[Broker] Detected environment PORT: ${process.env.PORT || 'not set (using 3001)'}`);
    startF1Broker();
});

// Handle WebSocket upgrades for clients
server.on('upgrade', (request, socket, head) => {
    if (request.url.startsWith('/f1-api')) {
        f1Proxy.upgrade(request, socket, head);
        return;
    }
    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
    });
});

wss.on('connection', (ws) => {
    console.log('[WSS] Client connected');
    if (Object.keys(liveState).length > 0) {
        ws.send(JSON.stringify(liveState));
    }
    ws.on('close', () => console.log('[WSS] Client disconnected'));
});