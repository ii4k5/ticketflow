const express   = require('express');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const cors      = require('cors');

const app = express();
app.use(express.json());
app.use(cors());

const notifications = [];

const kafka   = new Kafka({ clientId: 'notification-service', brokers: [process.env.KAFKA_BROKER || 'kafka:9092'], retry: { initialRetryTime: 3000, retries: 10 } });
const consumer = kafka.consumer({ groupId: 'notification-service-group' });

function addNotif(type, title, message, userId = null, meta = {}) {
  const n = { id: uuidv4(), type, title, message, userId, meta, readAt: null, createdAt: new Date().toISOString() };
  notifications.unshift(n);
  console.log(`🔔 [${type}] ${title}`);
  return n;
}

async function connectKafka() {
  for (let i = 0; i < 15; i++) {
    try {
      await consumer.connect();
      await consumer.subscribe({ topics: ['user-events', 'ticket-events'], fromBeginning: false });
      await consumer.run({ eachMessage: async ({ message }) => {
        const ev = JSON.parse(message.value.toString());
        switch (ev.eventType) {
          case 'UserRegistered':
            addNotif('welcome', '🎉 Welcome to TicketFlow!', `Hi ${ev.name}! Your account is ready. Start exploring events!`, ev.userId);
            console.log(`📧 Welcome email → ${ev.email}`);
            break;
          case 'TicketBooked':
            addNotif('booking', '✅ Booking Confirmed!', `${ev.quantity} ticket(s) for "${ev.eventName}" at ${ev.venue} on ${ev.eventDate}. Total: $${ev.totalPrice}.`, ev.userId, { ticketId: ev.ticketId });
            console.log(`📧 Booking confirmation → ${ev.userEmail}`);
            break;
          case 'TicketCancelled':
            addNotif('cancellation', '❌ Ticket Cancelled', `Your ticket for "${ev.eventName}" was cancelled. Refund in 3-5 days.`, ev.userId, { ticketId: ev.ticketId });
            break;
          case 'LowSeatsAlert':
            addNotif('alert', '⚠️ Low Availability', `Only ${ev.availableSeats} seats left for "${ev.eventName}"!`, null, { eventId: ev.eventId });
            break;
        }
      }});
      console.log('✅ Notification Service connected to Kafka');
      return;
    } catch { console.log(`Kafka retry ${i+1}/15...`); await new Promise(r => setTimeout(r, 5000)); }
  }
}

app.get('/health', (req, res) => res.json({ status: 'ok', service: 'notification-service', total: notifications.length }));
app.get('/api/notifications', (req, res) => {
  const { userId, limit = 50 } = req.query;
  let list = [...notifications];
  if (userId) list = list.filter(n => n.userId === userId || n.userId === null);
  res.json(list.slice(0, +limit));
});
app.get('/api/notifications/stats', (req, res) => {
  res.json({ total: notifications.length, unread: notifications.filter(n => !n.readAt).length,
    byType: { welcome: notifications.filter(n => n.type==='welcome').length, booking: notifications.filter(n => n.type==='booking').length, cancellation: notifications.filter(n => n.type==='cancellation').length, alert: notifications.filter(n => n.type==='alert').length } });
});
app.patch('/api/notifications/:id/read', (req, res) => {
  const n = notifications.find(n => n.id === req.params.id);
  if (!n) return res.status(404).json({ error: 'Not found' });
  n.readAt = new Date().toISOString();
  res.json(n);
});

const PORT = process.env.PORT || 3003;
connectKafka().then(() => app.listen(PORT, () => console.log(`🚀 Notification Service :${PORT}`)));
