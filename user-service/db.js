const mysql = require('mysql2/promise');

const pool = mysql.createPool({
  host:     process.env.DB_HOST     || 'mysql',
  port:     process.env.DB_PORT     || 3306,
  user:     process.env.DB_USER     || 'ticketflow',
  password: process.env.DB_PASSWORD || 'ticketflow123',
  database: process.env.DB_NAME     || 'ticketflow',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  enableKeepAlive: true
});

async function waitForDb(retries = 20, delay = 4000) {
  for (let i = 0; i < retries; i++) {
    try {
      const conn = await pool.getConnection();
      conn.release();
      console.log('✅ MySQL connected');
      return;
    } catch (err) {
      console.log(`MySQL not ready (${i + 1}/${retries}): ${err.message}`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw new Error('Could not connect to MySQL after retries');
}

module.exports = { pool, waitForDb };