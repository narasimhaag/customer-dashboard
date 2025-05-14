import express from 'express';
import { StoreConsumer } from '@acme/kafka';
import { StoreEvent, HourlyData } from '@acme/types';
import { WebSocketServer } from 'ws';
import cors from 'cors';

class DataService {
  private liveData: Map<number, StoreEvent[]> = new Map();
  private hourlyData: Map<string, HourlyData> = new Map();

  processEvent(event: StoreEvent) {
    // Update live data
    if (!this.liveData.has(event.store_id)) {
      this.liveData.set(event.store_id, []);
    }
    this.liveData.get(event.store_id)?.push(event);

    // Update hourly aggregates
    const hour = this.getHourFromTimestamp(event.time_stamp);
    const hourlyKey = `${event.store_id}-${hour}`;
    
    if (!this.hourlyData.has(hourlyKey)) {
      this.hourlyData.set(hourlyKey, {
        hour,
        customers_in: 0,
        customers_out: 0
      });
    }

    const current = this.hourlyData.get(hourlyKey)!;
    current.customers_in += event.customers_in;
    current.customers_out += event.customers_out;
  }

  getLiveData(storeId: number): StoreEvent[] {
    return this.liveData.get(storeId)?.slice(-50) || [];
  }

  getHourlyData(storeId: number, hours = 24): HourlyData[] {
    const now = new Date();
    const results: HourlyData[] = [];
    
    for (let i = 0; i < hours; i++) {
      const targetDate = new Date(now.getTime() - i * 60 * 60 * 1000);
      const hour = targetDate.toISOString().slice(0, 13) + ':00';
      const key = `${storeId}-${hour}`;
      
      if (this.hourlyData.has(key)) {
        results.unshift(this.hourlyData.get(key)!);
      } else {
        results.unshift({
          hour,
          customers_in: 0,
          customers_out: 0
        });
      }
    }
    
    return results;
  }

  private getHourFromTimestamp(timestamp: string): string {
    const date = new Date(timestamp);
    return date.toISOString().slice(0, 13) + ':00';
  }
}

const app = express();
const port = 3001;
const dataService = new DataService();

app.use(cors());
app.use(express.json());

// Initialize WebSocket server
const wss = new WebSocketServer({ port: 8080 });

wss.on('connection', (ws) => {
  console.log('New WebSocket connection');
  ws.on('close', () => console.log('Client disconnected'));
});

// Kafka consumer
const consumer = new StoreConsumer('store-traffic', (event) => {
  dataService.processEvent(event);
  
  // Broadcast to all WebSocket clients
  wss.clients.forEach((client) => {
    if (client.readyState === client.OPEN) {
      client.send(JSON.stringify({
        type: 'live_event',
        data: event
      }));
    }
  });
});

// API endpoints
app.get('/api/live/:storeId', (req, res) => {
  const data = dataService.getLiveData(parseInt(req.params.storeId));
  res.json(data);
});

app.get('/api/history/:storeId', (req, res) => {
  const data = dataService.getHourlyData(parseInt(req.params.storeId));
  res.json(data);
});

app.listen(port, async () => {
  console.log(`Server running on http://localhost:${port}`);
  try {
    await consumer.connect();
  } catch (error) {
    console.error('Failed to connect to Kafka:', error);
  }
});