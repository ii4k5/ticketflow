const express = require('express');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const bcrypt = require('bcryptjs');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(cors());

// In-memory store (replace with DB in production)
const users = new Map();

// Kafka setup
const kafka = new Kafka({
  clientId: 'user-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
  retry: { initialRetryTime: 3000, retries: 10 }
});

const producer = kafka.producer();

async function connectKafka() {
  let retries = 10;
  while (retries > 0) {
    try {
      await producer.connect();
      console.log('✅ User Service connected to Kafka');
      return;
    } catch (err) {
      console.log(`Kafka not ready, retrying... (${retries} left)`);
      retries--;
      await new Promise(r => setTimeout(r, 5000));
    }
  }
  throw new Error('Failed to connect to Kafka after retries');
}

// Routes
app.get('/health', (req, res) => res.json({ status: 'ok', service: 'user-service' }));

app.post('/api/users/register', async (req, res) => {
  try {
    const { name, email, password } = req.body;
    if (!name || !email || !password)
      return res.status(400).json({ error: 'name, email and password required' });

    if ([...users.values()].find(u => u.email === email))
      return res.status(409).json({ error: 'Email already registered' });

    const hashedPassword = await bcrypt.hash(password, 10);
    const user = { id: uuidv4(), name, email, password: hashedPassword, createdAt: new Date().toISOString() };
    users.set(user.id, user);

    // Publish UserRegistered event
    await producer.send({
      topic: 'user-events',
      messages: [{
        key: user.id,
        value: JSON.stringify({
          eventType: 'UserRegistered',
          userId: user.id,
          name: user.name,
          email: user.email,
          timestamp: new Date().toISOString()
        })
      }]
    });

    console.log(`📤 Published UserRegistered for ${user.email}`);
    res.status(201).json({ id: user.id, name: user.name, email: user.email, createdAt: user.createdAt });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/users/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = [...users.values()].find(u => u.email === email);
    if (!user) return res.status(401).json({ error: 'Invalid credentials' });

    const valid = await bcrypt.compare(password, user.password);
    if (!valid) return res.status(401).json({ error: 'Invalid credentials' });

    res.json({ id: user.id, name: user.name, email: user.email });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/users/:id', (req, res) => {
  const user = users.get(req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });
  const { password, ...safeUser } = user;
  res.json(safeUser);
});

app.get('/api/users', (req, res) => {
  const all = [...users.values()].map(({ password, ...u }) => u);
  res.json(all);
});

const PORT = process.env.PORT || 3001;

connectKafka().then(() => {
  app.listen(PORT, () => console.log(`🚀 User Service running on port ${PORT}`));
}).catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
