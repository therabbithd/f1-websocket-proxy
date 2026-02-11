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

// Helper: Decompress
function decompress(data) {
    try {
        const buffer = Buffer.from(data, 'base64');
        const inflated = pako.inflateRaw(buffer, { to: 'string' });
        return JSON.parse(inflated);
    } catch (e) {
        return {};
    }
}

async function startF1Broker() {
    const SIGNALR_HUB = 'Streaming';
    const hub = encodeURIComponent(JSON.stringify([{ name: SIGNALR_HUB }]));

    try {
        console.log('[Broker] Negotiating with F1...');
        const response = await fetch(`https://livetiming.formula1.com/signalr/negotiate?connectionData=${hub}&clientProtocol=1.5`, {
            headers: { 'User-Agent': 'BestHTTP' }
        });

        const data = await response.json();
        const connectionToken = data.ConnectionToken;

        if (!connectionToken) {
            console.log('[Broker] No session active. Retrying in 60s...');
            setTimeout(startF1Broker, 60000);
            return;
        }

        const wsUrl = `wss://livetiming.formula1.com/signalr/connect?clientProtocol=1.5&transport=webSockets&connectionToken=${encodeURIComponent(connectionToken)}&connectionData=${hub}`;
        const ws = new WebSocket(wsUrl, {
            headers: { 'User-Agent': 'BestHTTP' }
        });

        ws.on('open', () => {
            console.log('[Broker] F1 Connection established');
            brokerRunning = true;
            ws.send(JSON.stringify({
                H: SIGNALR_HUB,
                M: 'Subscribe',
                A: [[
                    'Heartbeat', 'CarData.z', 'Position.z', 'ExtrapolatedClock', 'TimingStats',
                    'TimingAppData', 'WeatherData', 'TrackStatus', 'DriverList',
                    'RaceControlMessages', 'SessionInfo', 'SessionData', 'LapCount', 'TimingData', 'TeamRadio',
                    'TyreStintSeries', 'TyreStintSeries.z'
                ]],
                I: 1
            }));
        });

        ws.on('message', (data) => {
            updateBrokerState(data.toString());
        });

        ws.on('close', () => {
            console.log('[Broker] F1 Connection closed. Reconnecting...');
            brokerRunning = false;
            setTimeout(startF1Broker, 10000);
        });

        ws.on('error', (err) => {
            console.error('[Broker] F1 WS Error:', err.message);
        });

    } catch (err) {
        console.error('[Broker] Fatal Error:', err.message);
        setTimeout(startF1Broker, 10000);
    }
}

function updateBrokerState(rawData) {
    try {
        const parsed = JSON.parse(rawData);
        let updates = [];

        if (Array.isArray(parsed.M)) {
            for (const message of parsed.M) {
                if (message.M === 'feed') {
                    let [field, value] = message.A;
                    let update = {};

                    if (field.endsWith('.z')) {
                        const baseField = field.split('.')[0];
                        value = decompress(value);
                        update = { [baseField]: value };
                    } else {
                        update = { [field]: value };
                    }

                    liveState = deepObjectMerge(liveState, update);
                    updates.push(update);
                }
            }
        } else if (parsed.R && parsed.I === '1') {
            const bulkUpdate = {};
            for (let [field, value] of Object.entries(parsed.R)) {
                if (field.endsWith('.z')) {
                    const baseField = field.split('.')[0];
                    bulkUpdate[baseField] = decompress(value);
                } else {
                    bulkUpdate[field] = value;
                }
            }
            liveState = deepObjectMerge(liveState, bulkUpdate);
            updates.push(bulkUpdate);
        }

        // Broadcast updates to all connected clients
        if (updates.length > 0) {
            const payload = JSON.stringify(updates.length === 1 ? updates[0] : updates);
            wss.clients.forEach(client => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(payload);
                }
            });
        }
    } catch (e) {
        // Ignore keep-alive errors
    }
}

// Health Check API
app.get('/', (req, res) => {
    res.json({ status: 'up', message: 'F1 Broker is alive' });
});

// HTTP/WS proxy for negotiate and static endpoints (fallback for clients hitting /f1-api/*)
const f1Proxy = createProxyMiddleware({
    target: 'https://livetiming.formula1.com',
    changeOrigin: true,
    ws: true,
    pathRewrite: { '^/f1-api': '' },
    headers: {
        'User-Agent': 'BestHTTP'
    },
    onProxyReq: (proxyReq) => {
        // Ensure the UA is set on upgrade-less requests too
        proxyReq.setHeader('User-Agent', 'BestHTTP');
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
    console.log(`[Broker] Detected environment PORT: ${process.env.PORT || 'not set (using 3000)'}`);
    startF1Broker();
});

// Handle WebSocket upgrades for clients
server.on('upgrade', (request, socket, head) => {
    // Proxy WS upgrades hitting /f1-api to the F1 origin
    if (request.url.startsWith('/f1-api')) {
        f1Proxy.upgrade(request, socket, head);
        return;
    }
    // If the client asks for /f1-api/... upgrade, proxy it to F1 directly
    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
    });
});

wss.on('connection', (ws) => {
    console.log('[WSS] Client connected');
    // Send current state on connection
    if (Object.keys(liveState).length > 0) {
        ws.send(JSON.stringify(liveState));
    }

    ws.on('close', () => console.log('[WSS] Client disconnected'));
});
