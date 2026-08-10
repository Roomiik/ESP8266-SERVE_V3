const express = require('express');
const cors = require('cors');
require('dotenv').config();
const mysql = require('mysql2/promise');

const pool = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 20,
  queueLimit: 0
});

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
const MAX_RECORDS_PER_SENSOR = process.env.MAX_RECORDS_PER_SENSOR ? parseInt(process.env.MAX_RECORDS_PER_SENSOR) : 1000;

app.post('/api/devices', async (req, res) => {
  const { code, name, type, subtype, unit, status, meta } = req.body;

  if (!code || !type) {
    return res.status(400).json({ error: 'code and type are required' });
  }

  try {
    const [result] = await pool.query(
      `INSERT INTO sensors (code, name, type, subtype, unit, status, meta)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [code, name, type, subtype, unit, status ?? null, meta ? JSON.stringify(meta) : null]
    );
    res.json({ id: result.insertId });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(400).json({ error: 'Device with this code already exists' });
    }
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/devices', async (req, res) => {
  try {
    const [rows] = await pool.query(
      `SELECT code, name, type, subtype, unit, status, meta, created_at FROM sensors`
    );

    const devices = rows.map(d => ({
      ...d,
      status: d.status === null ? null : Boolean(d.status)
    }));

    res.json(devices);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/sensors/latest', async (req, res) => {
  try {
    const [rows] = await pool.query(`
      SELECT sensor_code, value, timestamp
      FROM (
        SELECT
          sensor_code, value, timestamp,
          ROW_NUMBER() OVER (PARTITION BY sensor_code ORDER BY id DESC) AS rn
        FROM sensor_info
      ) ranked
      WHERE rn = 1
    `);
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Прийом даних від ESP8266

app.post('/api/sensors/data', async (req, res) => {
  const data = req.body;
  console.log(data);

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    for (const key of Object.keys(data)) {
      const { code, value } = data[key];
      if (!code || value === undefined) continue;

      await conn.query(
        `INSERT INTO sensor_info (sensor_code, value) VALUES (?, ?)`,
        [code, value]
      );

      const [rows] = await conn.query(
        `SELECT id FROM sensor_info
         WHERE sensor_code = ?
         ORDER BY id DESC
         LIMIT 1 OFFSET ?`,
        [code, MAX_RECORDS_PER_SENSOR]
      );

      if (rows.length > 0) {
        const cutoffId = rows[0].id;
        const [result] = await conn.query(
          `DELETE FROM sensor_info WHERE sensor_code = ? AND id <= ?`,
          [code, cutoffId]
        );
        if (result.affectedRows > 0) {
          console.log(`[${code}] Trimmed ${result.affectedRows} old record(s)`);
        }
      }
    }

    await conn.commit();
    res.json({ received: true });
  } catch (err) {
    await conn.rollback();
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    conn.release();
  }
});

app.get('/api/sensors/:code/history', async (req, res) => {
  const { code } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 100, 1000);  // за замовчуванням 100

  try {
    // Беремо останні N записів (сортуючи по id DESC),
    // потім розвертаємо назад у хронологічний порядок для графіка
    const [rows] = await pool.query(
      `SELECT value, timestamp FROM (
        SELECT value, timestamp, id FROM sensor_info
        WHERE sensor_code = ?
        ORDER BY id DESC
        LIMIT ?
      ) recent
      ORDER BY id ASC`,
      [code, limit]
    );

    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/pumps/:code/toggle', (req, res) => {
  const { code } = req.params;
  try {
    
    res.json({ code, status: code });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Запуск сервера
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
