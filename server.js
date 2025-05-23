// server.js
require("dotenv").config(); // Load environment variables from .env file

const express = require("express");
const { Pool } = require("pg");
const cors = require("cors");
const NodeCache = require("node-cache"); // Import node-cache

const app = express();
const port = process.env.PORT || 3001; // Use port from environment variable or default to 3001

// Initialize NodeCache with a standard TTL (Time To Live) of 1 hour (3600 seconds)
// and a checkperiod of 10 minutes (600 seconds) for expired keys.
const myCache = new NodeCache({ stdTTL: 3600, checkperiod: 600 });

// Use CORS middleware to allow requests from your React app
app.use(cors());
app.use(express.json()); // Enable JSON body parsing for POST requests

// PostgreSQL database configuration using environment variables
const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_DATABASE,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
  max: 20, // Max number of clients in the pool, adjust as needed
  idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
});

// Test database connection
pool.connect((err, client, release) => {
  if (err) {
    return console.error("Error acquiring client", err.stack);
  }
  console.log("Connected to PostgreSQL database!", client);
  release(); // Release the client back to the pool
});

// Helper to get date range for queries
const getDateRange = (timePeriod) => {
  const now = new Date();
  let startDate;
  let endDate = new Date(
    now.getFullYear(),
    now.getMonth(),
    now.getDate(),
    23,
    59,
    59,
    999
  ); // End of today

  switch (timePeriod) {
    case "today":
      startDate = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      break;
    case "yesterday":
      startDate = new Date(
        now.getFullYear(),
        now.getMonth(),
        now.getDate() - 1
      );
      endDate = new Date(
        now.getFullYear(),
        now.getMonth(),
        now.getDate() - 1,
        23,
        59,
        59,
        999
      );
      break;
    case "last7days":
      startDate = new Date(
        now.getFullYear(),
        now.getMonth(),
        now.getDate() - 6
      );
      break;
    case "thisMonth":
      startDate = new Date(now.getFullYear(), now.getMonth(), 1);
      break;
    case "last3Months":
      startDate = new Date(now.getFullYear(), now.getMonth() - 2, 1);
      break;
    case "last6Months":
      startDate = new Date(now.getFullYear(), now.getMonth() - 5, 1);
      break;
    case "thisYear":
      startDate = new Date(now.getFullYear(), 0, 1);
      break;
    default:
      startDate = new Date(
        now.getFullYear(),
        now.getMonth(),
        now.getDate() - 29
      ); // Default to last 30 days
  }
  startDate.setHours(0, 0, 0, 0); // Ensure start of day
  return {
    startDate: startDate.toISOString().split("T")[0],
    endDate: endDate.toISOString().split("T")[0],
  };
};

// API endpoint to get raw sales data for the transactions table
app.get("/api/sales", async (req, res) => {
  const {
    timePeriod,
    restaurantId,
    productId, // This will be item_code for filtering
    machineId,
    transactionType,
    deliveryChannel, // New filter
    pod, // New filter
  } = req.query;

  let query = `
        SELECT
            fs.sales_id,
            dt.business_date,
            ds.store_id AS restaurant_id,
            dn.node_id AS machine_id,
            di.item_code AS product_id,
            di.item_description AS product_name,
            di.item_family_group,
            di.item_day_part,
            fs.sale_type AS transaction_type,
            fs.delivery_channel, -- Include delivery_channel
            fs.pod, -- Include pod
            fs.total_amount,
            fs.item_qty
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
    `;

  const queryParams = [];
  let whereConditions = [];
  let paramIndex = 1;

  const { startDate, endDate } = getDateRange(timePeriod);
  whereConditions.push(`dt.business_date >= $${paramIndex++}`);
  queryParams.push(startDate);
  whereConditions.push(`dt.business_date <= $${paramIndex++}`);
  queryParams.push(endDate);

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndex++}`);
    queryParams.push(restaurantId);
  }
  if (productId && productId !== "all") {
    whereConditions.push(`di.item_code = $${paramIndex++}`);
    queryParams.push(productId);
  }
  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndex++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndex++}`);
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    // New filter
    whereConditions.push(`fs.delivery_channel = $${paramIndex++}`);
    queryParams.push(deliveryChannel);
  }
  if (pod && pod !== "all") {
    // New filter
    whereConditions.push(`fs.pod = $${paramIndex++}`);
    queryParams.push(pod);
  }

  if (whereConditions.length > 0) {
    query += ` WHERE ` + whereConditions.join(" AND ");
  }

  // Add ORDER BY or LIMIT if needed for performance or specific display order
  query += ` ORDER BY dt.business_date DESC, fs.sales_id DESC`; // Example ordering

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedTransactions = rows.map((row) => {
      const datePart = new Date(row.business_date);

      return {
        id: row.sales_id,
        restaurantId: row.restaurant_id,
        productId: row.product_id,
        productName: row.product_name,
        machineId: row.machine_id,
        transactionType: row.transaction_type,
        deliveryChannel: row.delivery_channel, // Include delivery_channel
        pod: row.pod, // Include pod
        timestamp: datePart.getTime(), // Using only business_date for timestamp
        amount: parseFloat(row.total_amount),
        quantity: parseFloat(row.item_qty),
        itemFamilyGroup: row.item_family_group,
        itemDayPart: row.item_day_part,
      };
    });
    res.json(formattedTransactions);
  } catch (err) {
    console.error("Error executing sales query", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get summary data
app.get("/api/sales/summary", async (req, res) => {
  const {
    timePeriod,
    restaurantId,
    productId,
    machineId,
    transactionType,
    deliveryChannel,
    pod,
  } = req.query;
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            SUM(fs.total_amount) AS total_sales,
            COUNT(DISTINCT fs.sales_id) AS total_orders,
            COALESCE(SUM(fs.total_amount) / NULLIF(COUNT(DISTINCT fs.sales_id), 0), 0) AS avg_order_value,
            COUNT(DISTINCT fs.invoice_number) AS total_invoices
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        LEFT JOIN -- Use LEFT JOIN for dim_node if it's not always present for all sales
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
    `;
  const queryParams = [];
  let whereConditions = [];
  let paramIndex = 1;

  whereConditions.push(`dt.business_date >= $${paramIndex++}`);
  queryParams.push(startDate);
  whereConditions.push(`dt.business_date <= $${paramIndex++}`);
  queryParams.push(endDate);

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndex++}`);
    queryParams.push(restaurantId);
  }
  if (productId && productId !== "all") {
    whereConditions.push(`di.item_code = $${paramIndex++}`);
    queryParams.push(productId);
  }
  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndex++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndex++}`);
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    // New filter
    whereConditions.push(`fs.delivery_channel = $${paramIndex++}`);
    queryParams.push(deliveryChannel);
  }
  if (pod && pod !== "all") {
    // New filter
    whereConditions.push(`fs.pod = $${paramIndex++}`);
    queryParams.push(pod);
  }

  if (whereConditions.length > 0) {
    query += ` WHERE ` + whereConditions.join(" AND ");
  }

  try {
    const { rows } = await pool.query(query, queryParams);
    const summary = rows[0] || {};
    res.json({
      totalSales: parseFloat(summary.total_sales || 0),
      totalOrders: parseInt(summary.total_orders || 0),
      avgOrderValue: parseFloat(summary.avg_order_value || 0),
      totalInvoices: parseInt(summary.total_invoices || 0),
    });
  } catch (err) {
    console.error("Error fetching summary data:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get daily sales trend
app.get("/api/sales/daily-trend", async (req, res) => {
  const {
    timePeriod,
    restaurantId,
    productId,
    machineId,
    transactionType,
    deliveryChannel,
    pod,
  } = req.query;
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            dt.business_date,
            SUM(fs.total_amount) AS sales
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
    `;
  const queryParams = [];
  let whereConditions = [];
  let paramIndex = 1;

  whereConditions.push(`dt.business_date >= $${paramIndex++}`);
  queryParams.push(startDate);
  whereConditions.push(`dt.business_date <= $${paramIndex++}`);
  queryParams.push(endDate);

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndex++}`);
    queryParams.push(restaurantId);
  }
  if (productId && productId !== "all") {
    whereConditions.push(`di.item_code = $${paramIndex++}`);
    queryParams.push(productId);
  }
  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndex++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndex++}`);
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    // New filter
    whereConditions.push(`fs.delivery_channel = $${paramIndex++}`);
    queryParams.push(deliveryChannel);
  }
  if (pod && pod !== "all") {
    // New filter
    whereConditions.push(`fs.pod = $${paramIndex++}`);
    queryParams.push(pod);
  }

  if (whereConditions.length > 0) {
    query += ` WHERE ` + whereConditions.join(" AND ");
  }

  query += `
        GROUP BY
            dt.business_date
        ORDER BY
            dt.business_date;
    `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = rows.map((row) => ({
      name: new Date(row.business_date).toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
      }),
      sales: parseFloat(row.sales || 0),
    }));
    res.json(formattedData);
  } catch (err) {
    console.error("Error fetching daily sales trend:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get hourly sales trend
app.get("/api/sales/hourly-trend", async (req, res) => {
  const {
    timePeriod,
    restaurantId,
    productId,
    machineId,
    transactionType,
    deliveryChannel,
    pod,
  } = req.query;
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            dt.hour AS hour,
            SUM(fs.total_amount) AS sales
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
        WHERE
            dt.business_date >= $1 AND dt.business_date <= $2
    `;
  const queryParams = [startDate, endDate];
  let paramIndex = 3;

  if (restaurantId && restaurantId !== "all") {
    query += ` AND ds.store_id = $${paramIndex++}`;
    queryParams.push(restaurantId);
  }
  if (productId && productId !== "all") {
    query += ` AND di.item_code = $${paramIndex++}`;
    queryParams.push(productId);
  }
  if (machineId && machineId !== "all") {
    query += ` AND dn.node_id = $${paramIndex++}`;
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    query += ` AND fs.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    // New filter
    query += ` AND fs.delivery_channel = $${paramIndex++}`;
    queryParams.push(deliveryChannel);
  }
  if (pod && pod !== "all") {
    // New filter
    query += ` AND fs.pod = $${paramIndex++}`;
    queryParams.push(pod);
  }

  query += `
        GROUP BY
            dt.hour
        ORDER BY
            hour;
    `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = Array.from({ length: 24 }, (_, i) => {
      // Ensure all hours are present, 0-23
      const hourData = rows.find((row) => row.hour === i);
      const hourStart = i;
      const hourEnd = i + 1;
      const hourFormatted = `${hourStart % 12 || 12}${
        hourStart < 12 ? "am" : "pm"
      }-${hourEnd % 12 || 12}${hourEnd < 12 ? "am" : "pm"}`;
      return {
        name: hourFormatted,
        sales: parseFloat(hourData ? hourData.sales : 0),
      };
    }).slice(8, 22); // Assuming 8 am to 10 pm range for display, adjust as needed

    res.json(formattedData);
  } catch (err) {
    console.error("Error fetching hourly sales trend:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get sales by restaurant
app.get("/api/sales/by-restaurant", async (req, res) => {
  const {
    timePeriod,
    productId,
    machineId,
    transactionType,
    deliveryChannel,
    pod,
  } = req.query; // No restaurantId filter here
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            ds.store_id AS name,
            SUM(fs.total_amount) AS value
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
        WHERE
            dt.business_date >= $1 AND dt.business_date <= $2
    `;
  const queryParams = [startDate, endDate];
  let paramIndex = 3;

  if (productId && productId !== "all") {
    query += ` AND di.item_code = $${paramIndex++}`;
    queryParams.push(productId);
  }
  if (machineId && machineId !== "all") {
    query += ` AND dn.node_id = $${paramIndex++}`;
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    query += ` AND fs.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    // New filter
    query += ` AND fs.delivery_channel = $${paramIndex++}`;
    queryParams.push(deliveryChannel);
  }
  if (pod && pod !== "all") {
    // New filter
    query += ` AND fs.pod = $${paramIndex++}`;
    queryParams.push(pod);
  }

  query += `
        GROUP BY
            ds.store_id
        ORDER BY
            value DESC;
    `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = rows.map((row) => ({
      name: row.name,
      value: parseFloat(row.value || 0),
    }));
    res.json(formattedData);
  } catch (err) {
    console.error("Error fetching sales by restaurant:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get sales by product (top 5)
app.get("/api/sales/by-product", async (req, res) => {
  const {
    timePeriod,
    restaurantId,
    machineId,
    transactionType,
    deliveryChannel,
    pod,
  } = req.query; // No productId filter here
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            di.item_description AS name,
            SUM(fs.total_amount) AS value
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
        WHERE
            dt.business_date >= $1 AND dt.business_date <= $2
    `;
  const queryParams = [startDate, endDate];
  let paramIndex = 3;

  if (restaurantId && restaurantId !== "all") {
    query += ` AND ds.store_id = $${paramIndex++}`;
    queryParams.push(restaurantId);
  }
  if (machineId && machineId !== "all") {
    query += ` AND dn.node_id = $${paramIndex++}`;
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    query += ` AND fs.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    // New filter
    query += ` AND fs.delivery_channel = $${paramIndex++}`;
    queryParams.push(deliveryChannel);
  }
  if (pod && pod !== "all") {
    // New filter
    query += ` AND fs.pod = $${paramIndex++}`;
    queryParams.push(pod);
  }

  query += `
        GROUP BY
            di.item_description
        ORDER BY
            value DESC
        LIMIT 5;
    `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = rows.map((row) => ({
      name: row.name,
      value: parseFloat(row.value || 0),
    }));
    res.json(formattedData);
  } catch (err) {
    console.error("Error fetching sales by product:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get sales by product description
app.get("/api/product/by-description", async (req, res) => {
  const {
    timePeriod,
    restaurantId,
    machineId,
    transactionType,
    deliveryChannel,
    pod,
  } = req.query;
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            di.item_description AS name,
            SUM(fs.total_amount) AS value
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
        WHERE
            dt.business_date >= $1 AND dt.business_date <= $2
    `;
  const queryParams = [startDate, endDate];
  let paramIndex = 3;

  if (restaurantId && restaurantId !== "all") {
    query += ` AND ds.store_id = $${paramIndex++}`;
    queryParams.push(restaurantId);
  }
  if (machineId && machineId !== "all") {
    query += ` AND dn.node_id = $${paramIndex++}`;
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    query += ` AND fs.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    // New filter
    query += ` AND fs.delivery_channel = $${paramIndex++}`;
    queryParams.push(deliveryChannel);
  }
  if (pod && pod !== "all") {
    // New filter
    query += ` AND fs.pod = $${paramIndex++}`;
    queryParams.push(pod);
  }

  query += `
        GROUP BY
            di.item_description
        ORDER BY
            value DESC;
    `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = rows.map((row) => ({
      name: row.name,
      value: parseFloat(row.value || 0),
    }));
    res.json(formattedData);
  } catch (err) {
    console.error("Error fetching sales by product description:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get sales by item family group
app.get("/api/product/by-family-group", async (req, res) => {
  const {
    timePeriod,
    restaurantId,
    productId,
    machineId,
    transactionType,
    deliveryChannel,
    pod,
  } = req.query;
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            di.item_family_group AS name,
            SUM(fs.total_amount) AS value
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        LEFT JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
        WHERE
            dt.business_date >= $1 AND dt.business_date <= $2
            AND di.item_family_group IS NOT NULL
    `;
  const queryParams = [startDate, endDate];
  let paramIndex = 3;

  if (restaurantId && restaurantId !== "all") {
    query += ` AND ds.store_id = $${paramIndex++}`;
    queryParams.push(restaurantId);
  }
  if (productId && productId !== "all") {
    query += ` AND di.item_code = $${paramIndex++}`;
    queryParams.push(productId);
  }
  if (machineId && machineId !== "all") {
    query += ` AND dn.node_id = $${paramIndex++}`;
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    query += ` AND fs.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    // New filter
    query += ` AND fs.delivery_channel = $${paramIndex++}`;
    queryParams.push(deliveryChannel);
  }
  if (pod && pod !== "all") {
    // New filter
    query += ` AND fs.pod = $${paramIndex++}`;
    queryParams.push(pod);
  }

  query += `
        GROUP BY
            di.item_family_group
        ORDER BY
            value DESC;
    `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = rows.map((row) => ({
      name: row.name,
      value: parseFloat(row.value || 0),
    }));
    res.json(formattedData);
  } catch (err) {
    console.error("Error fetching sales by item family group:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get sales by item day part
app.get("/api/product/by-day-part", async (req, res) => {
  const {
    timePeriod,
    restaurantId,
    productId,
    machineId,
    transactionType,
    deliveryChannel,
    pod,
  } = req.query;
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            di.item_day_part AS name,
            SUM(fs.total_amount) AS value
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        LEFT JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
        WHERE
            dt.business_date >= $1 AND dt.business_date <= $2
            AND di.item_day_part IS NOT NULL
    `;
  const queryParams = [startDate, endDate];
  let paramIndex = 3;

  if (restaurantId && restaurantId !== "all") {
    query += ` AND ds.store_id = $${paramIndex++}`;
    queryParams.push(restaurantId);
  }
  if (productId && productId !== "all") {
    query += ` AND di.item_code = $${paramIndex++}`;
    queryParams.push(productId);
  }
  if (machineId && machineId !== "all") {
    query += ` AND dn.node_id = $${paramIndex++}`;
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    query += ` AND fs.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    // New filter
    query += ` AND fs.delivery_channel = $${paramIndex++}`;
    queryParams.push(deliveryChannel);
  }
  if (pod && pod !== "all") {
    // New filter
    query += ` AND fs.pod = $${paramIndex++}`;
    queryParams.push(pod);
  }

  query += `
        GROUP BY
            di.item_day_part
        ORDER BY
            value DESC;
    `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = rows.map((row) => ({
      name: row.name,
      value: parseFloat(row.value || 0),
    }));
    res.json(formattedData);
  } catch (err) {
    console.error("Error fetching sales by item day part:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get sales by Sale Type
app.get("/api/sales/by-sale-type", async (req, res) => {
  const {
    timePeriod,
    restaurantId,
    productId,
    machineId,
    deliveryChannel,
    pod,
  } = req.query;
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            fs.sale_type AS name,
            SUM(fs.total_amount) AS value
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        LEFT JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
        WHERE
            dt.business_date >= $1 AND dt.business_date <= $2
            AND fs.sale_type IS NOT NULL
    `;
  const queryParams = [startDate, endDate];
  let paramIndex = 3;

  if (restaurantId && restaurantId !== "all") {
    query += ` AND ds.store_id = $${paramIndex++}`;
    queryParams.push(restaurantId);
  }
  if (productId && productId !== "all") {
    query += ` AND di.item_code = $${paramIndex++}`;
    queryParams.push(productId);
  }
  if (machineId && machineId !== "all") {
    query += ` AND dn.node_id = $${paramIndex++}`;
    queryParams.push(machineId);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    query += ` AND fs.delivery_channel = $${paramIndex++}`;
    queryParams.push(deliveryChannel);
  }
  if (pod && pod !== "all") {
    query += ` AND fs.pod = $${paramIndex++}`;
    queryParams.push(pod);
  }

  query += `
        GROUP BY
            fs.sale_type
        ORDER BY
            value DESC;
    `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = rows.map((row) => ({
      name: row.name,
      value: parseFloat(row.value || 0),
    }));
    res.json(formattedData);
  } catch (err) {
    console.error("Error fetching sales by sale type:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get sales by Delivery Channel
app.get("/api/sales/by-delivery-channel", async (req, res) => {
  const {
    timePeriod,
    restaurantId,
    productId,
    machineId,
    transactionType,
    pod,
  } = req.query;
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            fs.delivery_channel AS name,
            SUM(fs.total_amount) AS value
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        LEFT JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
        WHERE
            dt.business_date >= $1 AND dt.business_date <= $2
            AND fs.delivery_channel IS NOT NULL
    `;
  const queryParams = [startDate, endDate];
  let paramIndex = 3;

  if (restaurantId && restaurantId !== "all") {
    query += ` AND ds.store_id = $${paramIndex++}`;
    queryParams.push(restaurantId);
  }
  if (productId && productId !== "all") {
    query += ` AND di.item_code = $${paramIndex++}`;
    queryParams.push(productId);
  }
  if (machineId && machineId !== "all") {
    query += ` AND dn.node_id = $${paramIndex++}`;
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    query += ` AND fs.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
  }
  if (pod && pod !== "all") {
    query += ` AND fs.pod = $${paramIndex++}`;
    queryParams.push(pod);
  }

  query += `
        GROUP BY
            fs.delivery_channel
        ORDER BY
            value DESC;
    `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = rows.map((row) => ({
      name: row.name,
      value: parseFloat(row.value || 0),
    }));
    res.json(formattedData);
  } catch (err) {
    console.error("Error fetching sales by delivery channel:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get sales by POD (Point of Distribution)
app.get("/api/sales/by-pod", async (req, res) => {
  const {
    timePeriod,
    restaurantId,
    productId,
    machineId,
    transactionType,
    deliveryChannel,
  } = req.query;
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            fs.pod AS name,
            SUM(fs.total_amount) AS value
        FROM
            fact_sales fs
        JOIN
            dim_time dt ON fs.time_id = dt.time_id
        JOIN
            dim_store ds ON fs.store_id = ds.store_id
        LEFT JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
        WHERE
            dt.business_date >= $1 AND dt.business_date <= $2
            AND fs.pod IS NOT NULL
    `;
  const queryParams = [startDate, endDate];
  let paramIndex = 3;

  if (restaurantId && restaurantId !== "all") {
    query += ` AND ds.store_id = $${paramIndex++}`;
    queryParams.push(restaurantId);
  }
  if (productId && productId !== "all") {
    query += ` AND di.item_code = $${paramIndex++}`;
    queryParams.push(productId);
  }
  if (machineId && machineId !== "all") {
    query += ` AND dn.node_id = $${paramIndex++}`;
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    query += ` AND fs.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    query += ` AND fs.delivery_channel = $${paramIndex++}`;
    queryParams.push(deliveryChannel);
  }

  query += `
        GROUP BY
            fs.pod
        ORDER BY
            value DESC;
    `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = rows.map((row) => ({
      name: row.name,
      value: parseFloat(row.value || 0),
    }));
    res.json(formattedData);
  } catch (err) {
    console.error("Error fetching sales by POD:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// API endpoint to get mock data for filter options (cached)
app.get("/api/mock-data", async (req, res) => {
  const cachedData = myCache.get("mockData");
  if (cachedData) {
    console.log("Serving mock data from cache");
    return res.json(cachedData);
  }

  try {
    // Fetch restaurants
    const { rows: restaurantRows } = await pool.query(
      "SELECT store_id FROM dim_store"
    );
    const restaurants = restaurantRows.map((row) => ({
      id: row.store_id,
      name: row.store_id, // Using store_id as name for now, update if store_name column exists
    }));

    // Fetch products
    const { rows: productRows } = await pool.query(
      "SELECT item_code, item_description FROM dim_item"
    );
    const products = productRows.map((row) => ({
      id: row.item_code,
      name: row.item_description,
    }));

    // Fetch machines
    const { rows: machineRows } = await pool.query(
      "SELECT node_id FROM dim_node"
    );
    const machines = machineRows.map((row) => row.node_id);

    // Fetch distinct transaction types
    const { rows: transactionTypeRows } = await pool.query(
      "SELECT DISTINCT sale_type FROM fact_sales WHERE sale_type IS NOT NULL"
    );
    const transactionTypes = transactionTypeRows.map((row) => row.sale_type);

    // Fetch distinct delivery channels
    const { rows: deliveryChannelRows } = await pool.query(
      "SELECT DISTINCT delivery_channel FROM fact_sales WHERE delivery_channel IS NOT NULL"
    );
    const deliveryChannels = deliveryChannelRows.map(
      (row) => row.delivery_channel
    );

    // Fetch distinct PODs
    const { rows: podRows } = await pool.query(
      "SELECT DISTINCT pod FROM fact_sales WHERE pod IS NOT NULL"
    );
    const pods = podRows.map((row) => row.pod);

    // Fetch distinct item_family_group
    const { rows: itemFamilyGroupRows } = await pool.query(
      "SELECT DISTINCT item_family_group FROM dim_item WHERE item_family_group IS NOT NULL"
    );
    const itemFamilyGroups = itemFamilyGroupRows.map(
      (row) => row.item_family_group
    );

    // Fetch distinct item_day_part
    const { rows: itemDayPartRows } = await pool.query(
      "SELECT DISTINCT item_day_part FROM dim_item WHERE item_day_part IS NOT NULL"
    );
    const itemDayParts = itemDayPartRows.map((row) => row.item_day_part);

    const dataToCache = {
      restaurants,
      products,
      machines,
      transactionTypes,
      deliveryChannels, // Include new options
      pods, // Include new options
      itemFamilyGroups,
      itemDayParts,
    };

    myCache.set("mockData", dataToCache); // Cache the fetched data
    res.json(dataToCache);
  } catch (err) {
    console.error("Error fetching mock data:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Start the server
app.listen(port, "0.0.0.0", () => {
  console.log(`Server running on:`);
  console.log(`- Local:   http://localhost:${port}`);
});
