const express = require('express');
const { Kafka } = require('kafkajs');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());
app.use(cors());

// In-memory notification log
const notifications = [];

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
  retry: { initialRetryTime: 3000, retries: 10 }
});

const consumer = kafka.consumer({ groupId: 'notification-service-group' });

function createNotification(type, title, message, userId = null, metadata = {}) {
  const notification = {
    id: uuidv4(),
    type,         // 'welcome' | 'booking' | 'cancellation' | 'alert'
    title,
    message,
    userId,
    metadata,
    readAt: null,
    createdAt: new Date().toISOString()
  };
  notifications.unshift(notification); // newest first
  console.log(`🔔 Notification [${type}]: ${title}`);
  return notification;
}

async function connectKafka() {
  let retries = 10;
  while (retries > 0) {
    try {
      await consumer.connect();
      await consumer.subscribe({ topics: ['user-events', 'ticket-events'], fromBeginning: false });

      await consumer.run({
        eachMessage: async ({ topic, message }) => {
          const event = JSON.parse(message.value.toString());
          console.log(`📥 [${topic}] → ${event.eventType}`);

          switch (event.eventType) {
            case 'UserRegistered':
              createNotification(
                'welcome',
                '🎉 Welcome to TicketFlow!',
                `Hi ${event.name}! Your account has been created. Start exploring events and book your first ticket!`,
                event.userId,
                { email: event.email }
              );
              // Simulate email
              console.log(`📧 Email sent to ${event.email}: Welcome to TicketFlow, ${event.name}!`);
              break;

            case 'TicketBooked':
              createNotification(
                'booking',
                '✅ Booking Confirmed!',
                `Your ${event.quantity} ticket(s) for "${event.eventName}" at ${event.venue} on ${event.eventDate} are confirmed. Total: $${event.totalPrice}.`,
                event.userId,
                {
                  ticketId: event.ticketId,
                  eventName: event.eventName,
                  venue: event.venue,
                  eventDate: event.eventDate,
                  quantity: event.quantity,
                  totalPrice: event.totalPrice
                }
              );
              console.log(`📧 Email sent to ${event.userEmail}: Booking confirmed for ${event.eventName}`);
              break;

            case 'TicketCancelled':
              createNotification(
                'cancellation',
                '❌ Ticket Cancelled',
                `Your ticket for "${event.eventName}" has been cancelled. Refund will be processed within 3-5 business days.`,
                event.userId,
                { ticketId: event.ticketId, eventName: event.eventName }
              );
              break;

            case 'LowSeatsAlert':
              createNotification(
                'alert',
                '⚠️ Low Availability Alert',
                `Only ${event.availableSeats} seats remaining for "${event.eventName}"! Book now before they sell out.`,
                null,
                { eventId: event.eventId, eventName: event.eventName, availableSeats: event.availableSeats }
              );
              break;

            default:
              console.log(`Unknown event type: ${event.eventType}`);
          }
        }
      });

      console.log('✅ Notification Service connected to Kafka');
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
app.get('/health', (req, res) => res.json({ status: 'ok', service: 'notification-service', total: notifications.length }));

app.get('/api/notifications', (req, res) => {
  const { userId, type, limit = 50 } = req.query;
  let result = [...notifications];
  if (userId) result = result.filter(n => n.userId === userId || n.userId === null);
  if (type) result = result.filter(n => n.type === type);
  res.json(result.slice(0, parseInt(limit)));
});

app.get('/api/notifications/user/:userId', (req, res) => {
  const userNotifs = notifications.filter(n => n.userId === req.params.userId || n.userId === null);
  res.json(userNotifs);
});

app.patch('/api/notifications/:id/read', (req, res) => {
  const notif = notifications.find(n => n.id === req.params.id);
  if (!notif) return res.status(404).json({ error: 'Notification not found' });
  notif.readAt = new Date().toISOString();
  res.json(notif);
});

app.get('/api/notifications/stats', (req, res) => {
  const stats = {
    total: notifications.length,
    byType: {
      welcome: notifications.filter(n => n.type === 'welcome').length,
      booking: notifications.filter(n => n.type === 'booking').length,
      cancellation: notifications.filter(n => n.type === 'cancellation').length,
      alert: notifications.filter(n => n.type === 'alert').length,
    },
    unread: notifications.filter(n => !n.readAt).length
  };
  res.json(stats);
});

const PORT = process.env.PORT || 3003;

connectKafka().then(() => {
  app.listen(PORT, () => console.log(`🚀 Notification Service running on port ${PORT}`));
}).catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
