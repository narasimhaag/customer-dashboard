import { Kafka, Consumer } from 'kafkajs';
import { StoreEvent } from '@acme/types';

export class StoreConsumer {
  private consumer: Consumer;

  constructor(
    private topic: string, 
    private onMessage: (event: StoreEvent) => void,
    private brokers: string[] = ['localhost:9092']
  ) {
    const kafka = new Kafka({ brokers: this.brokers });
    this.consumer = kafka.consumer({ groupId: 'store-dashboard' });
  }

  async connect() {
    try {
      await this.consumer.connect();
      await this.consumer.subscribe({ topic: this.topic, fromBeginning: false });
      await this.consumer.run({
        eachMessage: async ({ message }) => {
          try {
            const event = JSON.parse(message.value?.toString() || '{}') as StoreEvent;
            if (event.store_id && event.time_stamp) {
              this.onMessage(event);
            }
          } catch (error) {
            console.error('Error processing message:', error);
          }
        },
      });
      console.log(`Kafka consumer connected to topic ${this.topic}`);
    } catch (error) {
      console.error('Error connecting to Kafka:', error);
      throw error;
    }
  }

  async disconnect() {
    await this.consumer.disconnect();
  }
}