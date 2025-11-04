import { Kafka } from 'kafkajs';
import eventType from '../eventType.js';

// KafkaJS-based producer replacing node-rdkafka
const kafka = new Kafka({
  clientId: 'node-kafka-producer',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function sendRandomMessage() {
  const category = getRandomAnimal();
  const noise = getRandomNoise(category);
  const event = { category, noise };

  try {
    await producer.send({
      topic: 'test',
      messages: [
        { value: eventType.toBuffer(event) }
      ],
    });
    console.log(`message sent (${JSON.stringify(event)})`);
  } catch (error) {
    console.error('Failed to send message', error);
  }
}

function getRandomAnimal() {
  const categories = ['CAT', 'DOG'];
  return categories[Math.floor(Math.random() * categories.length)];
}

function getRandomNoise(animal) {
  if (animal === 'CAT') {
    const noises = ['meow', 'purr'];
    return noises[Math.floor(Math.random() * noises.length)];
  } else if (animal === 'DOG') {
    const noises = ['bark', 'woof'];
    return noises[Math.floor(Math.random() * noises.length)];
  } else {
    return 'silence..';
  }
}

async function start() {
  await producer.connect();
  console.log('producer ready..');
  setInterval(sendRandomMessage, 3000);
}

start();

// Graceful shutdown
process.on('SIGINT', async () => {
  try {
    await producer.disconnect();
  } finally {
    process.exit(0);
  }
});
