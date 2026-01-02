# F1 WebSocket Proxy

Este servidor proxy permite que la aplicación Angular se conecte al WebSocket de F1 Live Timing desde navegadores, añadiendo los headers necesarios que los navegadores no pueden establecer directamente.

## ¿Por qué es necesario?

- **Vercel no soporta WebSockets**: Las funciones serverless de Vercel no pueden mantener conexiones WebSocket persistentes
- **CORS y Headers**: F1 requiere headers específicos (`User-Agent: BestHTTP`) que los navegadores no pueden enviar en conexiones WebSocket directas
- **Solución**: Este proxy se despliega en un servicio que soporta WebSockets (Railway, Render, etc.)

## Despliegue en Railway (Gratis)

### 1. Crear cuenta en Railway
Visita [railway.app](https://railway.app) y crea una cuenta gratuita.

### 2. Desplegar desde GitHub

```bash
# Desde la raíz del proyecto
cd websocket-proxy

# Inicializar git si no está inicializado
git init
git add .
git commit -m "Add WebSocket proxy"

# Subir a GitHub (crea un repositorio nuevo o usa el existente)
git remote add origin <tu-repo-url>
git push -u origin main
```

### 3. En Railway:
1. Click en "New Project"
2. Selecciona "Deploy from GitHub repo"
3. Elige tu repositorio
4. Railway detectará automáticamente el `package.json` y desplegará
5. Una vez desplegado, copia la URL (ej: `https://tu-app.railway.app`)

### 4. Configurar la aplicación Angular

En tu archivo de entorno de Angular, añade la URL del proxy:

```typescript
// src/environments/environment.prod.ts
export const environment = {
  production: true,
  proxyUrl: 'https://tu-app.railway.app' // URL de Railway
};
```

## Despliegue Local (Desarrollo)

```bash
cd websocket-proxy
npm install
npm start
```

El servidor correrá en `http://localhost:3000`

## Endpoints

- `GET /health` - Health check
- `/f1-api/*` - Proxy a `https://livetiming.formula1.com/*` (HTTP y WebSocket)

## Alternativas a Railway

- **Render**: Similar a Railway, también gratis
- **Fly.io**: Más complejo pero muy potente
- **Heroku**: De pago pero muy estable
