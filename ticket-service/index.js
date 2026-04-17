const express = require('express');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(cors());

// In-memory stores
const events = new Map();
const tickets = new Map();

// Seed some events
const seedEvents = [
  { id: uuidv4(), name: 'Coldplay World Tour 2025', venue: 'Cairo International Stadium', date: '2025-09-15', price: 150, totalSeats: 500, availableSeats: 500, category: 'Concert' },
  { id: uuidv4(), name: 'Egypt Tech Summit', venue: 'Cairo Opera House', date: '2025-08-20', price: 75, totalSeats: 300, availableSeats: 300, category: 'Conference' },
  { id: uuidv4(), name: 'Al Ahly vs Zamalek Derby', venue: 'Cairo International Stadium', date: '2025-07-30', price: 50, totalSeats: 1000, availableSeats: 1000, category: 'Sports' },
  { id: uuidv4(), name: 'Cairo Jazz Festival', venue: 'El Sawy Culturewheel', date: '2025-10-05', price: 100, totalSeats: 200, availableSeats: 200, category: 'Music' },
];
seedEvents.forEach(e => events.set(e.id, e));

// Kafka
const kafka = new Kafka({
  clientId: 'ticket-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
  retry: { initialRetryTime: 3000, retries: 10 }
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'ticket-service-group' });

async function connectKafka() {
  let retries = 10;
  while (retries > 0) {
    try {
      await producer.connect();
      await consumer.connect();
      await consumer.subscribe({ topics: ['user-events'], fromBeginning: false });

      await consumer.run({
        eachMessage: async ({ topic, message }) => {
          const event = JSON.parse(message.value.toString());
          console.log(`📥 Received [${topic}] event: ${event.eventType}`);
          if (event.eventType === 'UserRegistered') {
            console.log(`🎉 New user registered: ${event.name} (${event.email}) — welcome tickets can be offered!`);
          }
        }
      });

      console.log('✅ Ticket Service connected to Kafka');
      return;
    } catch (err) {
      console.log(`Kafka not ready, retrying... (${retries} left)`);
      retries--;
      await new Promise(r => setTimeout(r, 5000));
    }
  }
  throw new Error('Failed to connect to Kafka');
}

// Routes
app.get('/health', (req, res) => res.json({ status: 'ok', service: 'ticket-service' }));

app.get('/api/events', (req, res) => {
  res.json([...events.values()]);
});

app.get('/api/events/:id', (req, res) => {
  const event = events.get(req.params.id);
  if (!event) return res.status(404).json({ error: 'Event not found' });
  res.json(event);
});

app.post('/api/events', (req, res) => {
  const { name, venue, date, price, totalSeats, category } = req.body;
  if (!name || !venue || !date || !price || !totalSeats)
    return res.status(400).json({ error: 'Missing required fields' });

  const event = { id: uuidv4(), name, venue, date, price: Number(price), totalSeats: Number(totalSeats), availableSeats: Number(totalSeats), category: category || 'General' };
  events.set(event.id, event);
  res.status(201).json(event);
});

app.post('/api/tickets/book', async (req, res) => {
  try {
    const { userId, eventId, userName, userEmail, quantity = 1 } = req.body;
    if (!userId || !eventId) return res.status(400).json({ error: 'userId and eventId required' });

    const event = events.get(eventId);
    if (!event) return res.status(404).json({ error: 'Event not found' });
    if (event.availableSeats < quantity)
      return res.status(409).json({ error: `Only ${event.availableSeats} seats available` });

    // Reserve seats
    event.availableSeats -= quantity;
    events.set(eventId, event);

    const ticket = {
      id: uuidv4(),
      userId,
      eventId,
      eventName: event.name,
      venue: event.venue,
      date: event.date,
      quantity,
      totalPrice: event.price * quantity,
      status: 'confirmed',
      bookedAt: new Date().toISOString()
    };
    tickets.set(ticket.id, ticket);

    // Publish TicketBooked event
    await producer.send({
      topic: 'ticket-events',
      messages: [{
        key: ticket.id,
        value: JSON.stringify({
          eventType: 'TicketBooked',
          ticketId: ticket.id,
          userId,
          userName: userName || 'Guest',
          userEmail: userEmail || '',
          eventId,
          eventName: event.name,
          venue: event.venue,
          eventDate: event.date,
          quantity,
          totalPrice: ticket.totalPrice,
          timestamp: new Date().toISOString()
        })
      }]
    });

    // Check if seats are running low
    if (event.availableSeats < event.totalSeats * 0.1) {
      await producer.send({
        topic: 'ticket-events',
        messages: [{
          key: eventId,
          value: JSON.stringify({
            eventType: 'LowSeatsAlert',
            eventId,
            eventName: event.name,
            availableSeats: event.availableSeats,
            timestamp: new Date().toISOString()
          })
        }]
      });
      console.log(`⚠️ Published LowSeatsAlert for event: ${event.name}`);
    }

    console.log(`📤 Published TicketBooked: ${ticket.id}`);
    res.status(201).json(ticket);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Booking failed' });
  }
});

app.get('/api/tickets/user/:userId', (req, res) => {
  const userTickets = [...tickets.values()].filter(t => t.userId === req.params.userId);
  res.json(userTickets);
});

app.get('/api/tickets/:id', (req, res) => {
  const ticket = tickets.get(req.params.id);
  if (!ticket) return res.status(404).json({ error: 'Ticket not found' });
  res.json(ticket);
});

app.delete('/api/tickets/:id/cancel', async (req, res) => {
  try {
    const ticket = tickets.get(req.params.id);
    if (!ticket) return res.status(404).json({ error: 'Ticket not found' });
    if (ticket.status === 'cancelled') return res.status(400).json({ error: 'Already cancelled' });

    ticket.status = 'cancelled';
    tickets.set(ticket.id, ticket);

    // Restore seats
    const event = events.get(ticket.eventId);
    if (event) {
      event.availableSeats += ticket.quantity;
      events.set(event.id, event);
    }

    await producer.send({
      topic: 'ticket-events',
      messages: [{
        key: ticket.id,
        value: JSON.stringify({
          eventType: 'TicketCancelled',
          ticketId: ticket.id,
          userId: ticket.userId,
          eventName: ticket.eventName,
          timestamp: new Date().toISOString()
        })
      }]
    });

    res.json({ message: 'Ticket cancelled', ticket });
  } catch (err) {
    res.status(500).json({ error: 'Cancellation failed' });
  }
});

const PORT = process.env.PORT || 3002;

connectKafka().then(() => {
  app.listen(PORT, () => console.log(`🚀 Ticket Service running on port ${PORT}`));
}).catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
