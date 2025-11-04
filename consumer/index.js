import { Kafka } from 'kafkajs';
import eventType from '../eventType.js';

// KafkaJS-based consumer replacing node-rdkafka
const kafka = new Kafka({
  clientId: 'node-kafka-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'kafka' });

async function start() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'test', fromBeginning: true });
    console.log('consumer ready..');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const decoded = eventType.fromBuffer(message.value);
          console.log(`received message: ${decoded}`);
        } catch (err) {
          console.error('Failed to decode message', err);
        }
      },
    });
  } catch (err) {
    console.error('Consumer startup failed', err);
    process.exit(1);
  }
}

start();

// Graceful shutdown (optional but helpful during dev)
process.on('SIGINT', async () => {
  try {
    await consumer.disconnect();
  } finally {
    process.exit(0);
  }
});
