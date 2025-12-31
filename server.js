import express from "express";
import http from "http";
import { WebSocketServer } from "ws";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;

function makeId() {
  return Math.random().toString(16).slice(2) + Date.now().toString(16);
}

function safeSend(ws, obj) {
  if (!ws || ws.readyState !== ws.OPEN) return;
  ws.send(JSON.stringify(obj));
}

const state = {
  hostWs: null,   // projector display
  adminWs: null,  // controller
  clients: new Map(), // clientId -> { ws, name }

  round: null, // { roundId, prompt, actual:{lat,lng}|null, revealed:boolean, guesses: Map(clientId -> {lat,lng,ts}) }
};

function publicRound() {
  if (!state.round) return null;
  return {
    roundId: state.round.roundId,
    prompt: state.round.prompt,
    revealed: state.round.revealed,
    guessCount: state.round.guesses.size,
    hasActual: !!state.round.actual
  };
}

function fullRound() {
  if (!state.round) return null;
  return {
    roundId: state.round.roundId,
    prompt: state.round.prompt,
    revealed: state.round.revealed,
    actual: state.round.actual,
    guesses: Array.from(state.round.guesses.entries()).map(([clientId, g]) => ({
      clientId,
      name: state.clients.get(clientId)?.name ?? "Unknown",
      lat: g.lat,
      lng: g.lng,
      ts: g.ts
    }))
  };
}

function broadcastClients(obj) {
  const msg = JSON.stringify(obj);
  for (const { ws } of state.clients.values()) {
    if (ws.readyState === ws.OPEN) ws.send(msg);
  }
}

function emitAllState() {
  // Host display sees only public round state (no pins) until reveal message arrives.
  // If the round has already been revealed, send the full round to host so it
  // can render the actual + pins (this also covers hosts that connect after a reveal).
  safeSend(state.hostWs, { type: "round_state", round: state.round && state.round.revealed ? fullRound() : publicRound() });

  // Clients see public round state; reveal delivered separately.
  broadcastClients({ type: "round_state", round: publicRound() });

  // Admin sees full round plus client list.
  safeSend(state.adminWs, {
    type: "admin_state",
    clients: Array.from(state.clients.entries()).map(([id, c]) => ({ clientId: id, name: c.name })),
    round: fullRound()
  });
}

function emitAdminRoundOnly() {
  safeSend(state.adminWs, { type: "round_state_admin", round: fullRound() });
}

function emitAdminClientList() {
  safeSend(state.adminWs, {
    type: "clients_list",
    clients: Array.from(state.clients.entries()).map(([id, c]) => ({ clientId: id, name: c.name }))
  });
}

wss.on("connection", (ws) => {
  ws._role = "unknown";
  ws._clientId = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    // ---- Register ----
    if (msg.type === "register") {
      const role = msg.role;

      if (role === "host") {
        state.hostWs = ws;
        ws._role = "host";
        // If the round is already revealed, give the host the full round
        // (actual + guesses) so it can render the reveal on connect.
        safeSend(ws, { type: "round_state", round: state.round && state.round.revealed ? fullRound() : publicRound() });
        return;
      }

      if (role === "admin") {
        state.adminWs = ws;
        ws._role = "admin";
        emitAllState();
        return;
      }

      if (role === "client") {
        const name = String(msg.name ?? "").trim().slice(0, 24) || "Player";
        const clientId = makeId();

        ws._role = "client";
        ws._clientId = clientId;

        state.clients.set(clientId, { ws, name });

        safeSend(ws, { type: "registered", clientId, name, round: publicRound() });

        emitAdminClientList();
        emitAllState();
        return;
      }

      return;
    }

    // ---- Admin controls ----
    if (ws._role === "admin") {
      if (msg.type === "start_round") {
        const prompt = String(msg.prompt ?? "").trim().slice(0, 160) || "Where is it?";
        state.round = {
          roundId: makeId(),
          prompt,
          actual: null,
          revealed: false,
          guesses: new Map()
        };
        emitAllState();
        return;
      }

      if (msg.type === "set_actual") {
        if (!state.round) return;
        const lat = Number(msg.lat);
        const lng = Number(msg.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;

        state.round.actual = { lat, lng };
        emitAllState();
        return;
      }

      if (msg.type === "reveal") {
        if (!state.round || !state.round.actual) return;
        state.round.revealed = true;

        const payload = fullRound();

        // host + clients get the reveal payload (pins + actual + names)
        safeSend(state.hostWs, { type: "reveal", round: payload });
        broadcastClients({ type: "reveal", round: payload });

        // everyone also gets state update (so counters, etc. stay in sync)
        emitAllState();
        return;
      }

      if (msg.type === "clear_round") {
        state.round = null;
        emitAllState();
        return;
      }

      if (msg.type === "kick_all") {
        // optional: disconnect all clients
        for (const { ws: cws } of state.clients.values()) {
          try { cws.close(); } catch {}
        }
        state.clients.clear();
        if (state.round) state.round.guesses.clear();
        emitAllState();
        return;
      }
    }

    // ---- Client guesses ----
    if (ws._role === "client") {
      if (msg.type === "guess") {
        if (!state.round || state.round.revealed) return;

        const lat = Number(msg.lat);
        const lng = Number(msg.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;

        const clientId = ws._clientId;
        state.round.guesses.set(clientId, { lat, lng, ts: Date.now() });

        safeSend(ws, { type: "guess_saved", lat, lng });

        // update counts everywhere; admin gets full detail
        emitAllState();
        emitAdminRoundOnly();
        return;
      }
    }
  });

  ws.on("close", () => {
    if (ws._role === "host" && state.hostWs === ws) state.hostWs = null;
    if (ws._role === "admin" && state.adminWs === ws) state.adminWs = null;

    if (ws._role === "client") {
      const clientId = ws._clientId;
      state.clients.delete(clientId);
      if (state.round) state.round.guesses.delete(clientId);

      emitAdminClientList();
      emitAllState();
    }
  });
});

server.listen(PORT, () => {
  console.log(`Server: http://localhost:${PORT}`);
  console.log(`Host:   http://localhost:${PORT}/host.html`);
  console.log(`Admin:  http://localhost:${PORT}/admin.html`);
  console.log(`Client: http://localhost:${PORT}/client.html`);
});