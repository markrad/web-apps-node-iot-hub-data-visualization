const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const EventHubReader = require('./scripts/event-hub-reader.js');
const { WebSocketServer } = require('ws');

const iotHubConnectionString = process.env.IotHubConnectionString;
if (!iotHubConnectionString) {
  console.error(`Environment variable IotHubConnectionString must be specified.`);
  return;
}
console.log(`Using IoT Hub connection string [${iotHubConnectionString}]`);

const eventHubConsumerGroup = process.env.EventHubConsumerGroup;
console.log(eventHubConsumerGroup);
if (!eventHubConsumerGroup) {
  console.error(`Environment variable EventHubConsumerGroup must be specified.`);
  return;
}
console.log(`Using event hub consumer group [${eventHubConsumerGroup}]`);

// Redirect requests to the public subdirectory to the root
const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.use((req, res /* , next */) => {
  res.redirect('/');
});

const server = http.createServer(app);
// const wss = new WebSocket.Server({ server });
const wss = new WebSocketServer({ noServer: true });

function broadcast(data, clients) {
  let needCleanup = false;
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      try {
        // console.log(`Broadcasting data ${data}`);
        client.send(data);
      } catch (e) {
        console.error(e);
      }
    }
    else {
      needCleanup = true;
    }

    if (needCleanup) {
      clients = clients.filter((client) => client.readyState == WebSocket.OPEN || client.readyState == WebSocket.CONNECTING);
    }
  });
}

var wss1 = [];      // IoT hub data
var wss2 = [];      // Video stream
var vidin = null;   // Video in

wss.on('connection', (ws) => {
  wss1.push(ws);
  console.log('Data out connected');
});

wss.on('videoout', (ws) => {
  wss2.push(ws);
  console.log('Video out connected')
});

function videoSend(data) {
  broadcast(data, wss2);
}

wss.on('videoin', (ws) => {
  if (vidin) {
    vidin.off('message', videoSend);
  }
  vidin = ws;
  vidin.on('message', videoSend);
  console.log('Video in connected');
});

server.on('upgrade', (request, socket, head) => {
  if (request.url == '/') {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  }
  else if (request.url == '/videoout') {
    wss.handleUpgrade(request, socket, head, (ws) => {
      console.log('video reader connected');
      wss.emit('videoout', ws, request);
    });
  }
  else if (request.url == '/videoin') {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('videoin', ws, request);
    });
  }
})

server.listen(process.env.PORT || '443', () => {
  console.log('Listening on %d.', server.address().port);
});

const eventHubReader = new EventHubReader(iotHubConnectionString, eventHubConsumerGroup);

(async () => {
  await eventHubReader.startReadMessage((message, date, deviceId) => {
    if (wss1) {
      try {
        const payload = {
          IotData: message,
          MessageDate: date || Date.now().toISOString(),
          DeviceId: deviceId,
        };

        broadcast(JSON.stringify(payload), wss1);
      } catch (err) {
        console.error('Error broadcasting: [%s] from [%s].', err, message);
      }
    }
  });
})().catch();