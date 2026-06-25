const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const SET_SCORE = 10;

const PERMUTATIONS = [[1,2,3],[1,3,2],[2,1,3],[2,3,1],[3,1,2],[3,2,1]];
function generateValues() {
  const arr = [];
  for (let i = 0; i < 30; i++) arr.push(PERMUTATIONS[Math.floor(Math.random() * 6)]);
  return arr;
}

function judge(p1, p2) {
  if (p1 === p2) return 0;
  if ((p1 === 0 && p2 === 1) || (p1 === 1 && p2 === 2) || (p1 === 2 && p2 === 0)) return 1;
  return -1;
}

// フロントエンド(index.html)の配信
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url.startsWith('/?')) {
    fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) { res.writeHead(500); return res.end('Error loading index.html'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
  } else {
    res.writeHead(404); res.end('Not found');
  }
});

// 【修正後】サーバーを直接紐付けず、まずは独立して作成する
const wss = new WebSocket.Server({ noServer: true });

// HTTPサーバーが「WebSocketへのアップグレード要求」を受け取った時の処理
server.on('upgrade', (request, socket, head) => {
  const pathname = new URL(request.url, `http://${request.headers.host}`).pathname;

  // パスが /ws の場合のみ、WebSocketサーバーに処理を引き渡す
  if (pathname === '/ws') {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  } else {
    // パスが違う場合は接続を破棄する
    socket.destroy();
  }
});

const rooms = {};
let randomQueueBo1 = [];
let randomQueueBo3 = [];

function generateRoomId() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let id = '';
  for (let i = 0; i < 6; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return id;
}

function broadcastState(room) {
  const sendToPlayer = (pKey, isP1) => {
    const p = room.players[pKey];
    if (p && p.ws.readyState === WebSocket.OPEN) {
      p.ws.send(JSON.stringify({
        type: 'state_sync',
        state: {
          turn: room.turn, currentSet: room.currentSet, bo: room.bo, suddenDeath: room.suddenDeath,
          values: room.values,
          scores: { me: isP1 ? room.scores.p1 : room.scores.p2, opp: isP1 ? room.scores.p2 : room.scores.p1 },
          sets: { me: isP1 ? room.sets.p1 : room.sets.p2, opp: isP1 ? room.sets.p2 : room.sets.p1 }
        }
      }));
    }
  };
  sendToPlayer('p1', true);
  if (room.players.p2) sendToPlayer('p2', false);
}

wss.on('connection', (ws) => {
  ws.on('message', (message) => {
    let data;
    try { data = JSON.parse(message); } catch (e) { return; }

    if (data.type === 'join_random') {
      const queue = data.bo === 1 ? randomQueueBo1 : randomQueueBo3;
      ws.playerName = data.name || 'Player';
      if (queue.length > 0) {
        let oppWs = queue.shift();
        while (oppWs.readyState !== WebSocket.OPEN && queue.length > 0) oppWs = queue.shift();
        if (oppWs.readyState === WebSocket.OPEN) {
          createAndStartRoom(oppWs, ws, data.bo);
          return;
        }
      }
      queue.push(ws);
      ws.send(JSON.stringify({ type: 'waiting_random' }));
    }

    if (data.type === 'create_room') {
      ws.playerName = data.name || 'Player';
      const roomId = generateRoomId();
      const room = {
        roomId, bo: data.bo, turn: 1, currentSet: 1,
        scores: { p1: 0, p2: 0 }, sets: { p1: 0, p2: 0 },
        values: generateValues(), suddenDeath: false,
        hands: { p1: null, p2: null }, nextReady: { p1: false, p2: false },
        players: { p1: { ws, name: ws.playerName }, p2: null }
      };
      rooms[roomId] = room;
      ws.roomId = roomId;
      ws.send(JSON.stringify({ type: 'room_created', roomId }));
    }

    if (data.type === 'join_room') {
      const room = rooms[data.roomId];
      if (!room) return ws.send(JSON.stringify({ type: 'error', message: 'ルームが見つかりません' }));
      if (room.players.p2) return ws.send(JSON.stringify({ type: 'error', message: 'ルームは満員です' }));
      
      ws.playerName = data.name || 'Player';
      room.players.p2 = { ws, name: ws.playerName };
      ws.roomId = data.roomId;

      const p1Ws = room.players.p1.ws;
      p1Ws.send(JSON.stringify({ type: 'game_start', roomId: room.roomId, oppName: ws.playerName, bo: room.bo }));
      ws.send(JSON.stringify({ type: 'game_start', roomId: room.roomId, oppName: p1Ws.playerName, bo: room.bo }));
      setTimeout(() => broadcastState(room), 500);
    }

    if (data.type === 'submit_hand') {
      const room = rooms[ws.roomId];
      if (!room) return;
      const isP1 = room.players.p1.ws === ws;
      if (isP1) room.hands.p1 = data.hand;
      else room.hands.p2 = data.hand;

      if (room.hands.p1 !== null && room.hands.p2 !== null) resolveTurn(room);
    }

    if (data.type === 'next_turn' || data.type === 'next_set' || data.type === 'rematch') {
      const room = rooms[ws.roomId];
      if (!room) return;
      const isP1 = room.players.p1.ws === ws;
      if (isP1) room.nextReady.p1 = true; else room.nextReady.p2 = true;

      if (room.nextReady.p1 && room.nextReady.p2) {
        room.nextReady.p1 = false; room.nextReady.p2 = false;
        if (data.type === 'next_turn') {
          room.turn++;
        } else if (data.type === 'next_set') {
          room.currentSet++; room.turn = 1; room.scores = { p1: 0, p2: 0 }; room.suddenDeath = false;
        } else if (data.type === 'rematch') {
          room.currentSet = 1; room.turn = 1; room.scores = { p1: 0, p2: 0 }; room.sets = { p1: 0, p2: 0 };
          room.suddenDeath = false; room.values = generateValues();
        }
        room.hands.p1 = null; room.hands.p2 = null;
        broadcastState(room);
      } else {
        ws.send(JSON.stringify({ type: 'wait_opponent_next' }));
      }
    }
  });

  ws.on('close', () => {
    randomQueueBo1 = randomQueueBo1.filter(w => w !== ws);
    randomQueueBo3 = randomQueueBo3.filter(w => w !== ws);
    const room = rooms[ws.roomId];
    if (room) {
      const opp = room.players.p1.ws === ws ? room.players.p2 : room.players.p1;
      if (opp && opp.ws && opp.ws.readyState === WebSocket.OPEN) {
        opp.ws.send(JSON.stringify({ type: 'opponent_disconnected' }));
      }
      delete rooms[ws.roomId];
    }
  });
});

function createAndStartRoom(p1Ws, p2Ws, bo) {
  const roomId = generateRoomId();
  const room = {
    roomId, bo, turn: 1, currentSet: 1,
    scores: { p1: 0, p2: 0 }, sets: { p1: 0, p2: 0 },
    values: generateValues(), suddenDeath: false,
    hands: { p1: null, p2: null }, nextReady: { p1: false, p2: false },
    players: {
      p1: { ws: p1Ws, name: p1Ws.playerName },
      p2: { ws: p2Ws, name: p2Ws.playerName }
    }
  };
  rooms[roomId] = room;
  p1Ws.roomId = roomId; p2Ws.roomId = roomId;

  p1Ws.send(JSON.stringify({ type: 'game_start', roomId, oppName: p2Ws.playerName, bo }));
  p2Ws.send(JSON.stringify({ type: 'game_start', roomId, oppName: p1Ws.playerName, bo }));
  setTimeout(() => broadcastState(room), 500);
}

function resolveTurn(room) {
  const p1H = room.hands.p1;
  const p2H = room.hands.p2;
  const res = judge(p1H, p2H);
  const vals = room.values[room.turn - 1];

  let p1P = 0, p2P = 0;
  if (!room.suddenDeath) {
    if (res === 1) p1P = vals[p1H];
    else if (res === -1) p2P = vals[p2H];
    else { p1P = vals[p1H]; p2P = vals[p2H]; }
  }

  room.scores.p1 += p1P; room.scores.p2 += p2P;

  let isSetOver = false, isMatchOver = false;
  let p1SetWin = false, p2SetWin = false;

  if (room.suddenDeath) {
    if (res !== 0) {
      isSetOver = true;
      if (res === 1) { p1SetWin = true; room.sets.p1++; } else { p2SetWin = true; room.sets.p2++; }
    }
  } else {
    if (room.scores.p1 >= SET_SCORE || room.scores.p2 >= SET_SCORE) {
      if (room.scores.p1 !== room.scores.p2) {
        isSetOver = true;
        if (room.scores.p1 > room.scores.p2) { p1SetWin = true; room.sets.p1++; } else { p2SetWin = true; room.sets.p2++; }
      } else {
        room.suddenDeath = true;
      }
    }
  }

  const need = Math.ceil(room.bo / 2);
  if (room.sets.p1 >= need || room.sets.p2 >= need) isMatchOver = true;

  const sendResult = (isP1, hMe, hOp, pMe, pOp, rMe, wonSet, wonMatch) => {
    const p = isP1 ? room.players.p1 : room.players.p2;
    if (p && p.ws.readyState === WebSocket.OPEN) {
      p.ws.send(JSON.stringify({
        type: 'turn_result',
        myHand: hMe, oppHand: hOp, myPts: pMe, oppPts: pOp, result: rMe,
        isSetOver, isMatchOver, iWonSet: wonSet, iWonMatch: wonMatch,
        newScores: { me: isP1 ? room.scores.p1 : room.scores.p2, opp: isP1 ? room.scores.p2 : room.scores.p1 },
        newSets: { me: isP1 ? room.sets.p1 : room.sets.p2, opp: isP1 ? room.sets.p2 : room.sets.p1 },
        suddenDeath: room.suddenDeath
      }));
    }
  };

  sendResult(true, p1H, p2H, p1P, p2P, res, p1SetWin, room.sets.p1 >= need);
  sendResult(false, p2H, p1H, p2P, p1P, -res, p2SetWin, room.sets.p2 >= need);
}

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});
