const express        = require('express');
const { Kafka }      = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const jwt            = require('jsonwebtoken');
const bcrypt         = require('bcryptjs');
const cors           = require('cors');
const cookieParser   = require('cookie-parser');
const { pool, waitForDb } = require('./db');

const app = express();
app.use(express.json());
app.use(cookieParser());
app.use(cors({ origin: true, credentials: true }));

const JWT_SECRET  = process.env.JWT_SECRET || 'ticketflow-secret-2026';
const SESSION_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days

const ROLE_PERMISSIONS = {
  admin:   ['events:read','events:create','events:update','events:delete',
            'tickets:read','tickets:create','tickets:cancel',
            'users:read','users:manage_roles'],
  manager: ['events:read','events:create','events:update',
            'tickets:read','tickets:create','tickets:cancel'],
  user:    ['events:read','tickets:read','tickets:create','tickets:cancel'],
};

const ALL_GRANTABLE = [
  'events:read','events:create','events:update','events:delete',
  'tickets:read','tickets:create','tickets:cancel',
];

// ── Schema bootstrap ──────────────────────────────────────────────────────────
async function initSchema() {
  await pool.query(`CREATE TABLE IF NOT EXISTS users (
    id            VARCHAR(36)                     PRIMARY KEY,
    name          VARCHAR(255)                    NOT NULL,
    email         VARCHAR(255) UNIQUE             NOT NULL,
    password_hash VARCHAR(255)                    NOT NULL,
    role          ENUM('admin','manager','user')  NOT NULL DEFAULT 'user',
    is_active     BOOLEAN                         NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMP                       DEFAULT CURRENT_TIMESTAMP
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS user_extra_permissions (
    user_id    VARCHAR(36)  NOT NULL,
    permission VARCHAR(100) NOT NULL,
    PRIMARY KEY (user_id, permission)
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS sessions (
    id         VARCHAR(36) PRIMARY KEY,
    user_id    VARCHAR(36) NOT NULL,
    expires_at TIMESTAMP   NOT NULL,
    created_at TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_expires (expires_at)
  )`);

  for (const seed of [
    { id: 'admin-00000001', name: 'Admin User',   email: 'admin@ticketflow.com',   pw: 'admin123',   role: 'admin'   },
    { id: 'manager-000001', name: 'Manager User', email: 'manager@ticketflow.com', pw: 'manager123', role: 'manager' },
  ]) {
    const [[exists]] = await pool.query('SELECT id FROM users WHERE email = ?', [seed.email]);
    if (!exists) {
      const hash = await bcrypt.hash(seed.pw, 10);
      await pool.query(
        'INSERT INTO users (id, name, email, password_hash, role) VALUES (?, ?, ?, ?, ?)',
        [seed.id, seed.name, seed.email, hash, seed.role]
      );
    }
  }
  console.log('✅ DB schema ready');
}

// ── Auth helpers ──────────────────────────────────────────────────────────────
async function getEffectivePermissions(userId, role) {
  const base = ROLE_PERMISSIONS[role] || [];
  const [rows] = await pool.query(
    'SELECT permission FROM user_extra_permissions WHERE user_id = ?', [userId]);
  return [...new Set([...base, ...rows.map(r => r.permission)])];
}

function issueJWT(user) {
  return jwt.sign(
    { id: user.id, email: user.email, name: user.name, role: user.role },
    JWT_SECRET,
    { expiresIn: '7d' }
  );
}

async function createSession(userId) {
  const id = uuidv4();
  await pool.query(
    'INSERT INTO sessions (id, user_id, expires_at) VALUES (?, ?, ?)',
    [id, userId, new Date(Date.now() + SESSION_TTL)]
  );
  return id;
}

function setSessionCookie(res, sid) {
  res.cookie('tf_session', sid, {
    httpOnly: true,
    sameSite: 'lax',
    maxAge: SESSION_TTL,
    path: '/',
  });
}

async function requireAuth(req, res, next) {
  const sid = req.cookies?.tf_session;
  if (!sid) return res.status(401).json({ error: 'Not authenticated' });
  try {
    const [[u]] = await pool.query(
      `SELECT u.id, u.name, u.email, u.role, u.is_active
       FROM sessions s JOIN users u ON u.id = s.user_id
       WHERE s.id = ? AND s.expires_at > NOW()`,
      [sid]
    );
    if (!u) return res.status(401).json({ error: 'Session expired' });
    if (!u.is_active) return res.status(403).json({ error: 'Account deactivated' });
    req.user = {
      id: u.id, name: u.name, email: u.email,
      role: u.role, isActive: !!u.is_active,
      permissions: await getEffectivePermissions(u.id, u.role),
    };
    next();
  } catch { res.status(500).json({ error: 'Auth error' }); }
}

function requireRole(...roles) {
  return (req, res, next) =>
    roles.includes(req.user?.role) ? next() : res.status(403).json({ error: 'Forbidden' });
}

// ── Kafka ─────────────────────────────────────────────────────────────────────
const kafka    = new Kafka({
  clientId: 'user-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
  retry: { initialRetryTime: 3000, retries: 10 },
});
const producer = kafka.producer();

async function connectKafka() {
  for (let i = 0; i < 10; i++) {
    try {
      await producer.connect();
      console.log('✅ Kafka connected');
      return;
    } catch { await new Promise(r => setTimeout(r, 5000)); }
  }
}

// ── Routes ────────────────────────────────────────────────────────────────────
app.get('/health', (req, res) => res.json({ status: 'ok', service: 'user-service' }));

app.get('/api/auth/roles', (req, res) => res.json([
  { key: 'admin',   label: 'Administrator', color: 'red',    permissions: ROLE_PERMISSIONS.admin },
  { key: 'manager', label: 'Manager',       color: 'yellow', permissions: ROLE_PERMISSIONS.manager },
  { key: 'user',    label: 'User',          color: 'blue',   permissions: ROLE_PERMISSIONS.user },
]));
// ── Register ────────────────────────────────────────────────────────────────────
app.post('/api/users/register', async (req, res) => {
  const { name, email, password } = req.body;
  if (!name || !email || !password)
    return res.status(400).json({ error: 'All fields required' });
  try {
    const [[exists]] = await pool.query('SELECT id FROM users WHERE email = ?', [email]);
    if (exists) return res.status(409).json({ error: 'Email already registered' });

    const id   = uuidv4();
    const hash = await bcrypt.hash(password, 10);
    await pool.query(
      'INSERT INTO users (id, name, email, password_hash) VALUES (?, ?, ?, ?)',
      [id, name, email, hash]
    );

    const sid  = await createSession(id);
    setSessionCookie(res, sid);

    const user = {
      id, name, email, role: 'user', isActive: true,
      permissions: ROLE_PERMISSIONS.user,
      createdAt: new Date().toISOString(),
    };

    producer.send({
      topic: 'user-events',
      messages: [{ key: id, value: JSON.stringify({
        eventType: 'UserRegistered', userId: id, email, name, role: 'user',
        timestamp: new Date().toISOString(),
      })}],
    }).catch(() => {});

    res.status(201).json({ user, token: issueJWT(user) });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Registration failed' });
  }
});
// ── Login ────────────────────────────────────────────────────────────────────
app.post('/api/users/login', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password)
    return res.status(400).json({ error: 'Email and password required' });
  try {
    const [[u]] = await pool.query('SELECT * FROM users WHERE email = ?', [email]);
    if (!u || !(await bcrypt.compare(password, u.password_hash)))
      return res.status(401).json({ error: 'Invalid credentials' });
    if (!u.is_active) return res.status(403).json({ error: 'Account deactivated' });

    const sid   = await createSession(u.id);
    setSessionCookie(res, sid);

    const perms = await getEffectivePermissions(u.id, u.role);
    const user  = { id: u.id, name: u.name, email: u.email, role: u.role,
                    isActive: true, permissions: perms, createdAt: u.created_at };
    res.json({ user, token: issueJWT(user) });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Login failed' });
  }
});

// ── Logout ────────────────────────────────────────────────────────────────────
app.post('/api/users/logout', async (req, res) => {
  const sid = req.cookies?.tf_session;
  if (sid) await pool.query('DELETE FROM sessions WHERE id = ?', [sid]).catch(() => {});
  res.clearCookie('tf_session', { path: '/' });
  res.json({ message: 'Logged out' });
});
// ── Get User Info ────────────────────────────────────────────────────────────────────
app.get('/api/users/me', requireAuth, (req, res) => {
  res.json({ user: req.user, token: issueJWT(req.user) });
});
// ── Get All Users ────────────────────────────────────────────────────────────────────
app.get('/api/users', requireAuth, requireRole('admin', 'manager'), async (req, res) => {
  const [users]  = await pool.query(
    'SELECT id, name, email, role, is_active AS isActive, created_at AS createdAt FROM users ORDER BY created_at'
  );
  const [extras] = await pool.query('SELECT user_id, permission FROM user_extra_permissions');
  const extraMap = {};
  extras.forEach(e => { (extraMap[e.user_id] = extraMap[e.user_id] || []).push(e.permission); });
  res.json(users.map(u => ({ ...u, extraPermissions: extraMap[u.id] || [] })));
});
// ── Update User Role ────────────────────────────────────────────────────────────────────
app.patch('/api/users/:id/role', requireAuth, requireRole('admin'), async (req, res) => {
  const { role } = req.body;
  if (!['admin', 'manager', 'user'].includes(role))
    return res.status(400).json({ error: 'Invalid role' });
  if (req.params.id === req.user.id)
    return res.status(400).json({ error: "Cannot change your own role" });
  await pool.query('UPDATE users SET role = ? WHERE id = ?', [role, req.params.id]);
  const [[u]] = await pool.query(
    'SELECT id, name, email, role, is_active AS isActive FROM users WHERE id = ?', [req.params.id]);
  res.json({ user: u });
});
// ── Update User Permissions ────────────────────────────────────────────────────────────────────
app.patch('/api/users/:id/permissions', requireAuth, requireRole('admin'), async (req, res) => {
  const { extraPermissions } = req.body;
  if (!Array.isArray(extraPermissions))
    return res.status(400).json({ error: 'extraPermissions must be an array' });
  const uid   = req.params.id;
  const valid = extraPermissions.filter(p => ALL_GRANTABLE.includes(p));
  await pool.query('DELETE FROM user_extra_permissions WHERE user_id = ?', [uid]);
  if (valid.length)
    await pool.query(
      'INSERT INTO user_extra_permissions (user_id, permission) VALUES ?',
      [valid.map(p => [uid, p])]
    );
  const [[u]] = await pool.query(
    'SELECT id, name, email, role, is_active AS isActive FROM users WHERE id = ?', [uid]);
  const permissions = await getEffectivePermissions(uid, u.role);
  res.json({ user: { ...u, permissions } });
});
// ── Toggle User Active Status ────────────────────────────────────────────────────────────────────
app.patch('/api/users/:id/toggle-active', requireAuth, requireRole('admin'), async (req, res) => {
  if (req.params.id === req.user.id)
    return res.status(400).json({ error: "Cannot deactivate yourself" });
  const [[u]] = await pool.query('SELECT is_active FROM users WHERE id = ?', [req.params.id]);
  if (!u) return res.status(404).json({ error: 'User not found' });
  const newVal = !u.is_active;
  await pool.query('UPDATE users SET is_active = ? WHERE id = ?', [newVal, req.params.id]);
  if (!newVal) await pool.query('DELETE FROM sessions WHERE user_id = ?', [req.params.id]);
  const [[updated]] = await pool.query(
    'SELECT id, name, email, role, is_active AS isActive FROM users WHERE id = ?', [req.params.id]);
  res.json({ user: updated, message: `User ${newVal ? 'activated' : 'deactivated'}` });
});
// ── Delete User ────────────────────────────────────────────────────────────────────
app.delete('/api/users/:id', requireAuth, requireRole('admin'), async (req, res) => {
  if (req.params.id === req.user.id)
    return res.status(400).json({ error: "Cannot delete yourself" });
  await pool.query('DELETE FROM sessions WHERE user_id = ?', [req.params.id]);
  await pool.query('DELETE FROM user_extra_permissions WHERE user_id = ?', [req.params.id]);
  await pool.query('DELETE FROM users WHERE id = ?', [req.params.id]);
  res.json({ message: 'User deleted' });
});
// ── Initialize Service ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
(async () => {
  await waitForDb();
  await initSchema();
  await connectKafka();
  app.listen(PORT, () => console.log(`🚀 User Service :${PORT}`));
})();
