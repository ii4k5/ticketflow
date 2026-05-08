const express   = require('express');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const jwt       = require('jsonwebtoken');
const { pool, waitForDb } = require('./db');
// ── App Setup ─────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());
// ── Auth Setup ─────────────────────────────────────────────────────────────
const JWT_SECRET = process.env.JWT_SECRET || 'ticketflow-secret-2026';

// ── Auth helpers ─────────────────────────────────────────────────────────────
const ROLE_PERMISSIONS = {
  admin:   ['events:read','events:create','events:update','events:delete','tickets:read','tickets:create','tickets:cancel'],
  manager: ['events:read','events:create','events:update','tickets:read','tickets:create','tickets:cancel'],
  user:    ['events:read','tickets:read','tickets:create','tickets:cancel'],
};

async function getEffectivePermissions(userId, role) {
  const base = ROLE_PERMISSIONS[role] || [];
  try {
    const [rows] = await pool.query(
      'SELECT permission FROM user_extra_permissions WHERE user_id = ?', [userId]);
    return [...new Set([...base, ...rows.map(r => r.permission)])];
  } catch { return base; }
}
// ── Auth Middleware ─────────────────────────────────────────────────────────────
async function requireAuth(req, res, next) {
  const h = req.headers.authorization;
  if (!h?.startsWith('Bearer ')) return res.status(401).json({ error: 'Authorization required' });
  try {
    const decoded = jwt.verify(h.slice(7), JWT_SECRET);
    const perms   = await getEffectivePermissions(decoded.id, decoded.role);
    req.user      = { ...decoded, permissions: perms };
    next();
  } catch { return res.status(401).json({ error: 'Invalid or expired token' }); }
}
// ── Permission Middleware ─────────────────────────────────────────────────────────────
function requirePermission(perm) {
  return (req, res, next) => {
    if (!req.user?.permissions?.includes(perm))
      return res.status(403).json({ error: `Access denied. Required: ${perm}`, yourRole: req.user?.role });
    next();
  };
}

// ── Kafka ─────────────────────────────────────────────────────────────────────
const kafka    = new Kafka({ clientId: 'ticket-service', brokers: [process.env.KAFKA_BROKER || 'kafka:9092'], retry: { initialRetryTime: 3000, retries: 10 } });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'ticket-service-group' });

async function connectKafka() {
  for (let i = 0; i < 15; i++) {
    try {
      await producer.connect();
      await consumer.connect();
      await consumer.subscribe({ topics: ['user-events'], fromBeginning: false });
      await consumer.run({ eachMessage: async ({ message }) => {
        const ev = JSON.parse(message.value.toString());
        if (ev.eventType === 'UserRegistered') console.log(`📥 New user registered: ${ev.email} [${ev.role}]`);
      }});
      console.log('✅ Kafka connected');
      return;
    } catch { console.log(`Kafka retry ${i+1}/15...`); await new Promise(r => setTimeout(r, 5000)); }
  }
}

// ── DB Init ───────────────────────────────────────────────────────────────────
async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS events (
      id             VARCHAR(36)   PRIMARY KEY,
      name           VARCHAR(255)  NOT NULL,
      venue          VARCHAR(255)  NOT NULL,
      date           VARCHAR(20)   NOT NULL,
      price          DECIMAL(10,2) NOT NULL,
      totalSeats     INT           NOT NULL,
      availableSeats INT           NOT NULL,
      category       VARCHAR(100)  DEFAULT 'General',
      createdBy      VARCHAR(36),
      createdAt      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
    )
  `);
  // add createdAt to existing tables that were created without it (ignore if already exists)
  await pool.query(`ALTER TABLE events ADD COLUMN createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP`).catch(() => {});
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tickets (
      id         VARCHAR(36)   PRIMARY KEY,
      userId     VARCHAR(36)   NOT NULL,
      userRole   VARCHAR(50),
      eventId    VARCHAR(36)   NOT NULL,
      eventName  VARCHAR(255),
      venue      VARCHAR(255),
      date       VARCHAR(20),
      quantity   INT           DEFAULT 1,
      totalPrice DECIMAL(10,2),
      status     VARCHAR(20)   DEFAULT 'confirmed',
      bookedAt   VARCHAR(50)
    )
  `);

  const [[{ cnt }]] = await pool.query('SELECT COUNT(*) AS cnt FROM events');
  if (cnt === 0) {
    const seed = [
      { name: 'Coldplay World Tour 2026',  venue: 'Cairo International Stadium', date: '2026-09-15', price: 150, totalSeats: 500,  category: 'Concert' },
      { name: 'Egypt Tech Summit',          venue: 'Cairo Opera House',           date: '2026-08-20', price: 75,  totalSeats: 300,  category: 'Conference' },
      { name: 'Al Ahly vs Zamalek Derby',  venue: 'Cairo International Stadium', date: '2026-07-30', price: 50,  totalSeats: 1000, category: 'Sports' },
      { name: 'Cairo Jazz Festival',        venue: 'El Sawy Culturewheel',        date: '2026-10-05', price: 100, totalSeats: 200,  category: 'Music' },
    ];
    for (const e of seed) {
      await pool.query(
        'INSERT INTO events (id, name, venue, date, price, totalSeats, availableSeats, category) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [uuidv4(), e.name, e.venue, e.date, e.price, e.totalSeats, e.totalSeats, e.category]
      );
    }
    console.log(' Seeded initial events');
  }
}

// ── Routes ───────────────────────────────────────────────────────────────────
app.get('/health', (req, res) => res.json({ status: 'ok', service: 'ticket-service' }));

app.get('/api/events', async (_req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM events ORDER BY createdAt DESC');
    res.json(rows);
  } catch (err) { console.error('[GET /api/events]', err); res.status(500).json({ error: 'Failed to fetch events' }); }
});

app.get('/api/events/:id', async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM events WHERE id = ?', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'Event not found' });
    res.json(rows[0]);
  } catch (err) { console.error('[GET /api/events/:id]', err); res.status(500).json({ error: 'Failed to fetch event' }); }
});

// ── Create Event ──────────────────────────────────────────────────────────────
app.post('/api/events', requireAuth, requirePermission('events:create'), async (req, res) => {
  try {
    const { name, venue, date, price, totalSeats, category } = req.body;
    if (!name || !venue || !date || !price || !totalSeats) return res.status(400).json({ error: 'Missing fields' });
    const id = uuidv4();
    await pool.query(
      'INSERT INTO events (id, name, venue, date, price, totalSeats, availableSeats, category, createdBy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [id, name, venue, date, +price, +totalSeats, +totalSeats, category || 'General', req.user.id]
    );
    const [rows] = await pool.query('SELECT * FROM events WHERE id = ?', [id]);
    console.log(`✅ Event created: "${name}" by ${req.user.email}`);
    res.status(201).json(rows[0]);
  } catch (err) { console.error('[POST /api/events]', err); res.status(500).json({ error: 'Failed to create event' }); }
});

// ── Update Event ──────────────────────────────────────────────────────────────
app.patch('/api/events/:id', requireAuth, requirePermission('events:update'), async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM events WHERE id = ?', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'Event not found' });
    const updates = [];
    const values  = [];
    ['name','venue','date','price','category'].forEach(k => {
      if (req.body[k] !== undefined) { updates.push(`${k} = ?`); values.push(req.body[k]); }
    });
    if (updates.length) {
      values.push(req.params.id);
      await pool.query(`UPDATE events SET ${updates.join(', ')} WHERE id = ?`, values);
    }
    const [updated] = await pool.query('SELECT * FROM events WHERE id = ?', [req.params.id]);
    res.json(updated[0]);
  } catch (err) { console.error('[PATCH /api/events/:id]', err); res.status(500).json({ error: 'Failed to update event' }); }
});

// ── Delete Event ──────────────────────────────────────────────────────────────
app.delete('/api/events/:id', requireAuth, requirePermission('events:delete'), async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT id FROM events WHERE id = ?', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'Event not found' });
    await pool.query('DELETE FROM events WHERE id = ?', [req.params.id]);
    res.json({ message: 'Event deleted' });
  } catch (err) { console.error('[DELETE /api/events/:id]', err); res.status(500).json({ error: 'Failed to delete event' }); }
});

// ── Book Ticket ───────────────────────────────────────────────────────────────
app.post('/api/tickets/book', requireAuth, requirePermission('tickets:create'), async (req, res) => {
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    const { eventId, quantity = 1 } = req.body;
    const [evRows] = await conn.query('SELECT * FROM events WHERE id = ? FOR UPDATE', [eventId]);
    if (!evRows.length) { await conn.rollback(); return res.status(404).json({ error: 'Event not found' }); }
    const ev = evRows[0];
    if (ev.availableSeats < quantity) { await conn.rollback(); return res.status(409).json({ error: `Only ${ev.availableSeats} seats left` }); }
    await conn.query('UPDATE events SET availableSeats = availableSeats - ? WHERE id = ?', [quantity, eventId]);
    const ticket = {
      id: uuidv4(), userId: req.user.id, userRole: req.user.role, eventId,
      eventName: ev.name, venue: ev.venue, date: ev.date, quantity,
      totalPrice: ev.price * quantity, status: 'confirmed', bookedAt: new Date().toISOString(),
    };
    await conn.query(
      'INSERT INTO tickets (id, userId, userRole, eventId, eventName, venue, date, quantity, totalPrice, status, bookedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [ticket.id, ticket.userId, ticket.userRole, ticket.eventId, ticket.eventName, ticket.venue, ticket.date, ticket.quantity, ticket.totalPrice, ticket.status, ticket.bookedAt]
    );
    await conn.commit();

    await producer.send({ topic: 'ticket-events', messages: [{ key: ticket.id, value: JSON.stringify({ eventType: 'TicketBooked', ticketId: ticket.id, userId: req.user.id, userRole: req.user.role, userEmail: req.user.email, eventName: ev.name, venue: ev.venue, eventDate: ev.date, quantity, totalPrice: ticket.totalPrice, timestamp: new Date().toISOString() }) }] });

    const newAvailable = ev.availableSeats - quantity;
    if (newAvailable < ev.totalSeats * 0.1)
      await producer.send({ topic: 'ticket-events', messages: [{ key: eventId, value: JSON.stringify({ eventType: 'LowSeatsAlert', eventId, eventName: ev.name, availableSeats: newAvailable, timestamp: new Date().toISOString() }) }] });

    res.status(201).json(ticket);
  } catch (err) { await conn.rollback(); console.error(err); res.status(500).json({ error: 'Booking failed' }); }
  finally { conn.release(); }
});

// ── Get User Tickets ──────────────────────────────────────────────────────────
app.get('/api/tickets/user/:userId', requireAuth, requirePermission('tickets:read'), async (req, res) => {
  try {
    const isOwn     = req.params.userId === req.user.id;
    const canSeeAll = ['admin','manager'].includes(req.user.role);
    if (!isOwn && !canSeeAll) return res.status(403).json({ error: 'Cannot view others tickets' });
    const [rows] = await pool.query('SELECT * FROM tickets WHERE userId = ?', [req.params.userId]);
    res.json(rows);
  } catch { res.status(500).json({ error: 'Failed to fetch tickets' }); }
});

// ── Get All Tickets ───────────────────────────────────────────────────────────
app.get('/api/tickets', requireAuth, requirePermission('tickets:read'), async (req, res) => {
  try {
    if (!['admin','manager'].includes(req.user.role)) return res.status(403).json({ error: 'Admin/Manager only' });
    const [rows] = await pool.query('SELECT * FROM tickets');
    res.json(rows);
  } catch { res.status(500).json({ error: 'Failed to fetch tickets' }); }
});

// ── Cancel Ticket ─────────────────────────────────────────────────────────────
app.delete('/api/tickets/:id/cancel', requireAuth, requirePermission('tickets:cancel'), async (req, res) => {
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    const [rows] = await conn.query('SELECT * FROM tickets WHERE id = ? FOR UPDATE', [req.params.id]);
    if (!rows.length) { await conn.rollback(); return res.status(404).json({ error: 'Ticket not found' }); }
    const ticket = rows[0];
    if (ticket.status === 'cancelled') { await conn.rollback(); return res.status(400).json({ error: 'Already cancelled' }); }
    const isOwn = ticket.userId === req.user.id;
    if (!isOwn && !['admin','manager'].includes(req.user.role)) { await conn.rollback(); return res.status(403).json({ error: 'Cannot cancel others tickets' }); }
    await conn.query('UPDATE tickets SET status = ? WHERE id = ?', ['cancelled', ticket.id]);
    await conn.query('UPDATE events SET availableSeats = availableSeats + ? WHERE id = ?', [ticket.quantity, ticket.eventId]);
    await conn.commit();

    await producer.send({ topic: 'ticket-events', messages: [{ key: ticket.id, value: JSON.stringify({ eventType: 'TicketCancelled', ticketId: ticket.id, userId: ticket.userId, eventName: ticket.eventName, cancelledBy: req.user.email, timestamp: new Date().toISOString() }) }] });
    res.json({ message: 'Cancelled', ticket: { ...ticket, status: 'cancelled' } });
  } catch (err) { await conn.rollback(); res.status(500).json({ error: 'Cancellation failed' }); }
  finally { conn.release(); }
});

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3002;
(async () => {
  await waitForDb();
  await initDb();
  await connectKafka();
  app.listen(PORT, () => console.log(`🚀 Ticket Service :${PORT}`));
})();
