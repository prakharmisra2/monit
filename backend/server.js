const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

// Database connection pool
const pool = new Pool({
  user: process.env.DB_USER || 'sensor_user',
  host: process.env.DB_HOST || 'localhost',
  database: process.env.DB_NAME || 'sensor_data',
  password: process.env.DB_PASSWORD || 'your_secure_password',
  port: process.env.DB_PORT || 5432,
});

// Middleware
app.use(cors());
app.use(express.json());

// Test database connection
pool.query('SELECT NOW()', (err, result) => {
  if (err) {
    console.error('Database connection error:', err);
  } else {
    console.log('Database connected successfully');
  }
});

// API Routes

// Get all sensor readings with pagination
app.get('/api/readings', async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 100;
    const offset = (page - 1) * limit;

    const query = `
      SELECT * FROM sensor_readings 
      ORDER BY timestamp DESC 
      LIMIT $1 OFFSET $2
    `;

    const countQuery = 'SELECT COUNT(*) FROM sensor_readings';
    
    const [dataResult, countResult] = await Promise.all([
      pool.query(query, [limit, offset]),
      pool.query(countQuery)
    ]);

    const totalRecords = parseInt(countResult.rows[0].count);
    const totalPages = Math.ceil(totalRecords / limit);

    res.json({
      data: dataResult.rows,
      pagination: {
        currentPage: page,
        totalPages,
        totalRecords,
        hasNextPage: page < totalPages,
        hasPrevPage: page > 1
      }
    });
  } catch (err) {
    console.error('Error fetching readings:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get latest sensor reading
app.get('/api/readings/latest', async (req, res) => {
  try {
    const query = `
      SELECT * FROM sensor_readings 
      ORDER BY timestamp DESC 
      LIMIT 1
    `;
    
    const result = await pool.query(query);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'No readings found' });
    }
    
    res.json(result.rows[0]);
  } catch (err) {
    console.error('Error fetching latest reading:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get readings by date range
app.get('/api/readings/range', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;
    
    if (!startDate || !endDate) {
      return res.status(400).json({ error: 'startDate and endDate are required' });
    }

    const query = `
      SELECT * FROM sensor_readings 
      WHERE timestamp >= $1 AND timestamp <= $2
      ORDER BY timestamp DESC
    `;
    
    const result = await pool.query(query, [startDate, endDate]);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching readings by date range:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get readings with specific filters
app.get('/api/readings/filter', async (req, res) => {
  try {
    const { 
      minPressure, 
      maxPressure, 
      minTemp, 
      maxTemp, 
      limit = 100 
    } = req.query;

    let whereConditions = [];
    let queryParams = [];
    let paramIndex = 1;

    if (minPressure) {
      whereConditions.push(`pressure >= $${paramIndex}`);
      queryParams.push(parseFloat(minPressure));
      paramIndex++;
    }

    if (maxPressure) {
      whereConditions.push(`pressure <= $${paramIndex}`);
      queryParams.push(parseFloat(maxPressure));
      paramIndex++;
    }

    if (minTemp) {
      whereConditions.push(`temperature >= $${paramIndex}`);
      queryParams.push(parseFloat(minTemp));
      paramIndex++;
    }

    if (maxTemp) {
      whereConditions.push(`temperature <= $${paramIndex}`);
      queryParams.push(parseFloat(maxTemp));
      paramIndex++;
    }

    let query = 'SELECT * FROM sensor_readings';
    
    if (whereConditions.length > 0) {
      query += ' WHERE ' + whereConditions.join(' AND ');
    }
    
    query += ` ORDER BY timestamp DESC LIMIT $${paramIndex}`;
    queryParams.push(parseInt(limit));

    const result = await pool.query(query, queryParams);
    res.json(result.rows);
  } catch (err) {
    console.error('Error filtering readings:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get statistics
app.get('/api/stats', async (req, res) => {
  try {
    const query = `
      SELECT 
        COUNT(*) as total_readings,
        AVG(pressure) as avg_pressure,
        MIN(pressure) as min_pressure,
        MAX(pressure) as max_pressure,
        AVG(temperature) as avg_temperature,
        MIN(temperature) as min_temperature,
        MAX(temperature) as max_temperature,
        MIN(timestamp) as first_reading,
        MAX(timestamp) as latest_reading
      FROM sensor_readings
    `;
    
    const result = await pool.query(query);
    res.json(result.rows[0]);
  } catch (err) {
    console.error('Error fetching statistics:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

// Start server
app.listen(port, () => {
  console.log(`Sensor API server running on port ${port}`);
  console.log(`Access the API at http://localhost:${port}/api/`);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nShutting down server...');
  await pool.end();
  process.exit(0);
});