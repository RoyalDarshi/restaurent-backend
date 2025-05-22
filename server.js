// server.js
const express = require("express");
const { Pool } = require("pg");
const cors = require("cors"); // Import cors

const app = express();
const port = 3001; // Port for the backend server

// Use CORS middleware to allow requests from your React app
app.use(cors());
app.use(express.json()); // Enable JSON body parsing for POST requests

// PostgreSQL database configuration
// IMPORTANT: Replace with your actual PostgreSQL connection details
const pool = new Pool({
  user: "postgres",
  host: "192.168.29.91", // or your database host
  database: "vamshi",
  password: "vamshi",
  port: 5432, // Default PostgreSQL port
});

// Test database connection
pool.connect((err, client, release) => {
  if (err) {
    return console.error("Error acquiring client", err.stack);
  }
  console.log("Connected to PostgreSQL database!");
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
  } = req.query;

  let query = `
        SELECT
            fs.sales_id,
            dt.business_date,
            dt.start_time,
            ds.store_id AS restaurant_id,
            dn.node_id AS machine_id,
            di.item_code AS product_id,
            di.item_description AS product_name,
            di.item_family_group,
            di.item_day_part,
            dst.sale_type AS transaction_type,
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
        JOIN
            dim_sale_type dst ON fs.sale_type = dst.sale_type
        WHERE 1=1
    `;

  const queryParams = [];
  let paramIndex = 1;

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
    query += ` AND dst.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
  }

  const { startDate, endDate } = getDateRange(timePeriod);
  query += ` AND dt.business_date >= $${paramIndex++} AND dt.business_date <= $${paramIndex++}`;
  queryParams.push(startDate, endDate);

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedTransactions = rows.map((row) => {
      const datePart = new Date(row.business_date);
      const timePart = new Date(`1970-01-01T${row.start_time}`);
      datePart.setHours(
        timePart.getHours(),
        timePart.getMinutes(),
        timePart.getSeconds()
      );

      return {
        id: row.sales_id,
        restaurantId: row.restaurant_id,
        productId: row.product_id,
        productName: row.product_name,
        machineId: row.machine_id,
        transactionType: row.transaction_type,
        timestamp: datePart.getTime(),
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
  const { timePeriod, restaurantId, productId, machineId, transactionType } =
    req.query;
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            SUM(fs.total_amount) AS total_sales,
            COUNT(DISTINCT fs.sales_id) AS total_orders,
            COALESCE(SUM(fs.total_amount) / NULLIF(COUNT(DISTINCT fs.sales_id), 0), 0) AS avg_order_value,
            COALESCE(SUM(CASE WHEN dst.sale_type = 'Machine' THEN fs.total_amount ELSE 0 END) / NULLIF(COUNT(DISTINCT CASE WHEN dst.sale_type = 'Machine' AND dn.node_id IS NOT NULL THEN dn.node_id ELSE NULL END), 0), 0) AS avg_sales_per_machine
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
        JOIN
            dim_sale_type dst ON fs.sale_type = dst.sale_type
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
    query += ` AND dst.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
  }

  try {
    const { rows } = await pool.query(query, queryParams);
    const summary = rows[0] || {};
    res.json({
      totalSales: parseFloat(summary.total_sales || 0),
      totalOrders: parseInt(summary.total_orders || 0),
      avgOrderValue: parseFloat(summary.avg_order_value || 0),
      avgSalesPerMachine: parseFloat(summary.avg_sales_per_machine || 0),
    });
  } catch (err) {
    console.error("Error fetching summary data:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// New API endpoint to get daily sales trend
app.get("/api/sales/daily-trend", async (req, res) => {
  const { timePeriod, restaurantId, productId, machineId, transactionType } =
    req.query;
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
        JOIN
            dim_sale_type dst ON fs.sale_type = dst.sale_type
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
    query += ` AND dst.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
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
  const { timePeriod, restaurantId, productId, machineId, transactionType } =
    req.query;
  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
        SELECT
            EXTRACT(HOUR FROM dt.start_time::time) AS hour, -- Added ::time cast here
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
        JOIN
            dim_sale_type dst ON fs.sale_type = dst.sale_type
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
    query += ` AND dst.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
  }

  query += `
        GROUP BY
            EXTRACT(HOUR FROM dt.start_time::time)
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
  const { timePeriod, productId, machineId, transactionType } = req.query; // No restaurantId filter here
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
        JOIN
            dim_sale_type dst ON fs.sale_type = dst.sale_type
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
    query += ` AND dst.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
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
  const { timePeriod, restaurantId, machineId, transactionType } = req.query; // No productId filter here
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
        JOIN
            dim_sale_type dst ON fs.sale_type = dst.sale_type
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
    query += ` AND dst.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
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
  const { timePeriod, restaurantId, machineId, transactionType } = req.query;
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
        JOIN
            dim_sale_type dst ON fs.sale_type = dst.sale_type
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
    query += ` AND dst.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
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
  const { timePeriod, restaurantId, productId, machineId, transactionType } =
    req.query;
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
        JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
        JOIN
            dim_sale_type dst ON fs.sale_type = dst.sale_type
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
    query += ` AND dst.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
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
  const { timePeriod, restaurantId, productId, machineId, transactionType } =
    req.query;
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
        JOIN
            dim_node dn ON fs.node_id = dn.node_id
        JOIN
            dim_item di ON fs.item_code = di.item_code
        JOIN
            dim_sale_type dst ON fs.sale_type = dst.sale_type
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
    query += ` AND dst.sale_type = $${paramIndex++}`;
    queryParams.push(transactionType);
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

// API endpoint to get mock data for filter options
app.get("/api/mock-data", async (req, res) => {
  try {
    // Fetch restaurants
    // Changed 'store_name' to 'store_id' as 'store_name' column was reported as non-existent.
    // If you have a different column for store names, please update 'store_id' to that column name.
    const { rows: restaurantRows } = await pool.query(
      "SELECT store_id FROM dim_store"
    );
    const restaurants = restaurantRows.map((row) => ({
      id: row.store_id,
      name: row.store_id, // Using store_id as name for now
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

    // Fetch transaction types
    const { rows: transactionTypeRows } = await pool.query(
      "SELECT sale_type FROM dim_sale_type"
    );
    const transactionTypes = transactionTypeRows.map((row) => row.sale_type);

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

    res.json({
      restaurants,
      products,
      machines,
      transactionTypes,
      itemFamilyGroups, // Include new options
      itemDayParts, // Include new options
    });
  } catch (err) {
    console.error("Error fetching mock data:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Start the server
app.listen(port, () => {
  console.log(`Backend server listening at http://localhost:${port}`);
});
