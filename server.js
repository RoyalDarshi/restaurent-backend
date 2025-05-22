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

// API endpoint to get sales data from fact_sales table
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
            di.item_family_group, -- Added item_family_group
            di.item_day_part,     -- Added item_day_part
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
        LEFT JOIN
            dim_promotion dp ON fs.promotion_id = dp.promotion_id
        WHERE 1=1
    `;

  const queryParams = [];
  let paramIndex = 1;

  // Add filters based on query parameters
  if (restaurantId && restaurantId !== "all") {
    query += ` AND ds.store_id = $${paramIndex++}`;
    queryParams.push(restaurantId);
  }
  if (productId && productId !== "all") {
    query += ` AND di.item_code = $${paramIndex++}`; // Filter by item_code
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

  // Time period filtering
  const now = new Date();
  let startDate;

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
      // No time period filter, or default to a reasonable range (e.g., last 30 days)
      startDate = new Date(
        now.getFullYear(),
        now.getMonth(),
        now.getDate() - 29
      );
      break;
  }

  if (startDate) {
    query += ` AND dt.business_date >= $${paramIndex++}`;
    queryParams.push(startDate.toISOString().split("T")[0]); // Format to YYYY-MM-DD
  }

  try {
    const { rows } = await pool.query(query, queryParams);
    // Map the database rows to match the expected structure of your frontend
    const formattedTransactions = rows.map((row) => {
      // Combine business_date and start_time to create a full timestamp
      const datePart = new Date(row.business_date);
      const timePart = new Date(`1970-01-01T${row.start_time}`); // Use a dummy date for time parsing
      datePart.setHours(
        timePart.getHours(),
        timePart.getMinutes(),
        timePart.getSeconds()
      );

      return {
        id: row.sales_id,
        restaurantId: row.restaurant_id,
        productId: row.product_id, // This is item_code
        productName: row.product_name, // This is item_description
        machineId: row.machine_id,
        transactionType: row.transaction_type,
        timestamp: datePart.getTime(), // Full timestamp in milliseconds
        amount: parseFloat(row.total_amount),
        quantity: parseFloat(row.item_qty),
        itemFamilyGroup: row.item_family_group, // Include new field
        itemDayPart: row.item_day_part, // Include new field
      };
    });
    res.json(formattedTransactions);
  } catch (err) {
    console.error("Error executing sales query", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// API endpoint to provide dynamic filter options from the database
app.get("/api/mock-data", async (req, res) => {
  try {
    // Fetch restaurants
    const { rows: restaurantRows } = await pool.query(
      "SELECT store_id FROM dim_store"
    );
    const restaurants = restaurantRows.map((row) => row.store_id);

    // Fetch products (item_code and item_description)
    const { rows: productRows } = await pool.query(
      "SELECT item_code, item_description FROM dim_item"
    );
    const products = productRows.map((row) => ({
      item_code: row.item_code,
      item_description: row.item_description,
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
    console.error("Error fetching mock data options from DB:", err.stack);
    res.status(500).json({ error: "Internal server error fetching options" });
  }
});

// Start the server
app.listen(port, () => {
  console.log(`Backend server listening at http://localhost:${port}`);
});
