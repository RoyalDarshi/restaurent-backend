require("dotenv").config();

const express = require("express");
const { Pool } = require("pg");
const cors = require("cors");
const NodeCache = require("node-cache");

const app = express();
const port = process.env.PORT || 3001;

const myCache = new NodeCache({ stdTTL: 3600, checkperiod: 600 });

app.use(cors());
app.use(express.json());

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_DATABASE,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
  max: 20,
  idleTimeoutMillis: 30000,
});

pool.connect((err, client, release) => {
  if (err) {
    return console.error("Error acquiring client", err.stack);
  }
  console.log("Connected to PostgreSQL database!");
  release();
});

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
  );

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
      );
  }
  startDate.setHours(0, 0, 0, 0);
  return {
    startDate: startDate.toISOString().split("T")[0],
    endDate: endDate.toISOString().split("T")[0],
  };
};

// Helper function to add common WHERE conditions
const addCommonWhereConditions = (
  whereConditions,
  queryParams,
  reqQuery,
  paramIndexRef
) => {
  const {
    timePeriod,
    restaurantId,
    product,
    machineId,
    transactionType,
    deliveryChannel,
    store,
    ocEmailId, // New filter
    omEmailId, // New filter
  } = reqQuery;

  const { startDate, endDate } = getDateRange(timePeriod);
  whereConditions.push(`dt.business_date >= $${paramIndexRef.current++}`);
  queryParams.push(startDate);
  whereConditions.push(`dt.business_date <= $${paramIndexRef.current++}`);
  queryParams.push(endDate);

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
    queryParams.push(restaurantId);
  }

  let productFilter = null;
  if (product) {
    try {
      productFilter = JSON.parse(product);
    } catch (e) {
      console.error("Error parsing product filter:", e);
    }
  }
  if (productFilter && productFilter.type && productFilter.value) {
    whereConditions.push(
      `pm.${productFilter.type}::TEXT = $${paramIndexRef.current++}`
    );
    queryParams.push(productFilter.value);
  }

  let storeFilter = null;
  if (store) {
    try {
      storeFilter = JSON.parse(store);
    } catch (e) {
      console.error("Error parsing store filter:", e);
    }
  }
  if (storeFilter && storeFilter.type && storeFilter.value) {
    if (storeFilter.type === "state") {
      whereConditions.push(`sm.state = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "city") {
      whereConditions.push(`sm.city = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "storecode") {
      whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    }
  }

  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndexRef.current++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndexRef.current++}`);
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    whereConditions.push(`fs.delivery_channel = $${paramIndexRef.current++}`);
    queryParams.push(deliveryChannel);
  }
  // New OC and OM filters
  if (ocEmailId && ocEmailId !== "all") {
    whereConditions.push(`sm.ocemailid = $${paramIndexRef.current++}`);
    queryParams.push(ocEmailId);
  }
  if (omEmailId && omEmailId !== "all") {
    whereConditions.push(`sm.omemailid = $${paramIndexRef.current++}`);
    queryParams.push(omEmailId);
  }
};

app.get("/api/sales", async (req, res) => {
  let query = `
    SELECT
      fs.sales_id,
      dt.business_date,
      ds.store_id AS restaurant_id,
      dn.node_id AS machine_id,
      pm.productid AS product_id,
      pm.productname AS product_name,
      fs.sale_type AS transaction_type,
      fs.delivery_channel,
      fs.pod,
      fs.total_amount,
      fs.item_qty,
      di.item_family_group,
      di.item_day_part
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    JOIN dim_item di ON fs.item_code = di.item_code
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
  `;

  const queryParams = [];
  let whereConditions = [];
  let paramIndexRef = { current: 1 };

  addCommonWhereConditions(
    whereConditions,
    queryParams,
    req.query,
    paramIndexRef
  );

  if (whereConditions.length > 0) {
    query += ` WHERE ` + whereConditions.join(" AND ");
  }

  query += ` ORDER BY dt.business_date DESC, fs.sales_id DESC`;

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
        deliveryChannel: row.delivery_channel,
        pod: row.pod, // Keeping pod for now in Transaction interface
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

app.get("/api/sales/summary", async (req, res) => {
  let query = `
    SELECT
      SUM(fs.total_amount) AS total_sales,
      COUNT(DISTINCT fs.sales_id) AS total_orders,
      COALESCE(SUM(fs.total_amount) / NULLIF(COUNT(DISTINCT fs.sales_id), 0), 0) AS avg_order_value,
      COUNT(DISTINCT fs.invoice_number) AS total_invoices
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    LEFT JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
  `;

  const queryParams = [];
  const whereConditions = [];
  let paramIndexRef = { current: 1 };

  addCommonWhereConditions(
    whereConditions,
    queryParams,
    req.query,
    paramIndexRef
  );

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

app.get("/api/sales/daily-trend", async (req, res) => {
  const { startDate, endDate } = getDateRange(req.query.timePeriod);

  let query = `
    SELECT
      dt.business_date,
      SUM(fs.total_amount) AS sales
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
    WHERE dt.business_date >= $1 AND dt.business_date <= $2
  `;

  const queryParams = [startDate, endDate];
  let paramIndexRef = { current: 3 }; // Start index after initial date params
  const whereConditions = []; // Use this for additional conditions

  // Extract parameters from req.query and add them to whereConditions and queryParams
  const {
    restaurantId,
    product,
    machineId,
    transactionType,
    deliveryChannel,
    store,
    ocEmailId, // New filter
    omEmailId, // New filter
  } = req.query;

  let productFilter = null;
  if (product) {
    try {
      productFilter = JSON.parse(product);
    } catch (e) {
      console.error("Error parsing product filter:", e);
    }
  }
  if (productFilter && productFilter.type && productFilter.value) {
    whereConditions.push(
      `pm.${productFilter.type}::TEXT = $${paramIndexRef.current++}`
    );
    queryParams.push(productFilter.value);
  }

  let storeFilter = null;
  if (store) {
    try {
      storeFilter = JSON.parse(store);
    } catch (e) {
      console.error("Error parsing store filter:", e);
    }
  }
  if (storeFilter && storeFilter.type && storeFilter.value) {
    if (storeFilter.type === "state") {
      whereConditions.push(`sm.state = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "city") {
      whereConditions.push(`sm.city = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "storecode") {
      whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    }
  }

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
    queryParams.push(restaurantId);
  }
  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndexRef.current++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndexRef.current++}`);
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    whereConditions.push(`fs.delivery_channel = $${paramIndexRef.current++}`);
    queryParams.push(deliveryChannel);
  }
  // New OC and OM filters
  if (ocEmailId && ocEmailId !== "all") {
    whereConditions.push(`sm.ocemailid = $${paramIndexRef.current++}`);
    queryParams.push(ocEmailId);
  }
  if (omEmailId && omEmailId !== "all") {
    whereConditions.push(`sm.omemailid = $${paramIndexRef.current++}`);
    queryParams.push(omEmailId);
  }

  // Append additional WHERE conditions if any
  if (whereConditions.length > 0) {
    query += ` AND ` + whereConditions.join(" AND ");
  }

  query += `
    GROUP BY dt.business_date
    ORDER BY dt.business_date;
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

app.get("/api/sales/hourly-trend", async (req, res) => {
  const { startDate, endDate } = getDateRange(req.query.timePeriod);

  let query = `
    SELECT
      dt.hour AS hour,
      SUM(fs.total_amount) AS sales
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    LEFT JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
    WHERE dt.business_date >= $1 AND dt.business_date <= $2
  `;

  const queryParams = [startDate, endDate];
  let paramIndexRef = { current: 3 }; // Start index after initial date params
  const whereConditions = []; // Use this for additional conditions

  // Extract parameters from req.query and add them to whereConditions and queryParams
  const {
    restaurantId,
    product,
    machineId,
    transactionType,
    deliveryChannel,
    store,
    ocEmailId, // New filter
    omEmailId, // New filter
  } = req.query;

  let productFilter = null;
  if (product) {
    try {
      productFilter = JSON.parse(product);
    } catch (e) {
      console.error("Error parsing product filter:", e);
    }
  }
  if (productFilter && productFilter.type && productFilter.value) {
    whereConditions.push(
      `pm.${productFilter.type}::TEXT = $${paramIndexRef.current++}`
    );
    queryParams.push(productFilter.value);
  }

  let storeFilter = null;
  if (store) {
    try {
      storeFilter = JSON.parse(store);
    } catch (e) {
      console.error("Error parsing store filter:", e);
    }
  }
  if (storeFilter && storeFilter.type && storeFilter.value) {
    if (storeFilter.type === "state") {
      whereConditions.push(`sm.state = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "city") {
      whereConditions.push(`sm.city = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "storecode") {
      whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    }
  }

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
    queryParams.push(restaurantId);
  }
  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndexRef.current++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndexRef.current++}`);
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    whereConditions.push(`fs.delivery_channel = $${paramIndexRef.current++}`);
    queryParams.push(deliveryChannel);
  }
  // New OC and OM filters
  if (ocEmailId && ocEmailId !== "all") {
    whereConditions.push(`sm.ocemailid = $${paramIndexRef.current++}`);
    queryParams.push(ocEmailId);
  }
  if (omEmailId && omEmailId !== "all") {
    whereConditions.push(`sm.omemailid = $${paramIndexRef.current++}`);
    queryParams.push(omEmailId);
  }

  // Append additional WHERE conditions if any
  if (whereConditions.length > 0) {
    query += ` AND ` + whereConditions.join(" AND ");
  }

  query += `
    GROUP BY dt.hour
    ORDER BY hour;
  `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = Array.from({ length: 24 }, (_, i) => {
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
    }).slice(10, 24);

    res.json(formattedData);
  } catch (err) {
    console.error("Error fetching hourly sales trend:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/sales/by-restaurant", async (req, res) => {
  const { startDate, endDate } = getDateRange(req.query.timePeriod);

  let query = `
    SELECT
      ds.store_id AS name,
      SUM(fs.total_amount) AS value
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
    WHERE dt.business_date >= $1 AND dt.business_date <= $2
  `;

  const queryParams = [startDate, endDate];
  let paramIndexRef = { current: 3 };
  const whereConditions = [];

  // Extract parameters from req.query and add them to whereConditions and queryParams
  const {
    product,
    machineId,
    transactionType,
    deliveryChannel,
    store,
    ocEmailId, // New filter
    omEmailId, // New filter
  } = req.query;

  let productFilter = null;
  if (product) {
    try {
      productFilter = JSON.parse(product);
    } catch (e) {
      console.error("Error parsing product filter:", e);
    }
  }
  if (productFilter && productFilter.type && productFilter.value) {
    whereConditions.push(
      `pm.${productFilter.type}::TEXT = $${paramIndexRef.current++}`
    );
    queryParams.push(productFilter.value);
  }

  let storeFilter = null;
  if (store) {
    try {
      storeFilter = JSON.parse(store);
    } catch (e) {
      console.error("Error parsing store filter:", e);
    }
  }
  if (storeFilter && storeFilter.type && storeFilter.value) {
    if (storeFilter.type === "state") {
      whereConditions.push(`sm.state = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "city") {
      whereConditions.push(`sm.city = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "storecode") {
      whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    }
  }

  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndexRef.current++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndexRef.current++}`);
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    whereConditions.push(`fs.delivery_channel = $${paramIndexRef.current++}`);
    queryParams.push(deliveryChannel);
  }
  // New OC and OM filters
  if (ocEmailId && ocEmailId !== "all") {
    whereConditions.push(`sm.ocemailid = $${paramIndexRef.current++}`);
    queryParams.push(ocEmailId);
  }
  if (omEmailId && omEmailId !== "all") {
    whereConditions.push(`sm.omemailid = $${paramIndexRef.current++}`);
    queryParams.push(omEmailId);
  }

  if (whereConditions.length > 0) {
    query += ` AND ` + whereConditions.join(" AND ");
  }

  query += `
    GROUP BY ds.store_id
    ORDER BY value DESC;
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

app.get("/api/sales/by-product", async (req, res) => {
  const { startDate, endDate } = getDateRange(req.query.timePeriod);

  let query = `
    SELECT
      pm.productname AS name,
      SUM(fs.total_amount) AS value
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
    WHERE dt.business_date >= $1 AND dt.business_date <= $2
  `;

  const queryParams = [startDate, endDate];
  let paramIndexRef = { current: 3 };
  const whereConditions = [];

  // Extract parameters from req.query and add them to whereConditions and queryParams
  const {
    restaurantId,
    machineId,
    transactionType,
    deliveryChannel,
    store,
    ocEmailId, // New filter
    omEmailId, // New filter
  } = req.query;

  let storeFilter = null;
  if (store) {
    try {
      storeFilter = JSON.parse(store);
    } catch (e) {
      console.error("Error parsing store filter:", e);
    }
  }
  if (storeFilter && storeFilter.type && storeFilter.value) {
    if (storeFilter.type === "state") {
      whereConditions.push(`sm.state = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "city") {
      whereConditions.push(`sm.city = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "storecode") {
      whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    }
  }

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
    queryParams.push(restaurantId);
  }
  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndexRef.current++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndexRef.current++}`);
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    whereConditions.push(`fs.delivery_channel = $${paramIndexRef.current++}`);
    queryParams.push(deliveryChannel);
  }
  // New OC and OM filters
  if (ocEmailId && ocEmailId !== "all") {
    whereConditions.push(`sm.ocemailid = $${paramIndexRef.current++}`);
    queryParams.push(ocEmailId);
  }
  if (omEmailId && omEmailId !== "all") {
    whereConditions.push(`sm.omemailid = $${paramIndexRef.current++}`);
    queryParams.push(omEmailId);
  }

  if (whereConditions.length > 0) {
    query += ` AND ` + whereConditions.join(" AND ");
  }

  query += `
    GROUP BY pm.productname
    ORDER BY value DESC
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

app.get("/api/product/by-description", async (req, res) => {
  const { startDate, endDate } = getDateRange(req.query.timePeriod);

  let query = `
    SELECT
      pm.productname AS name,
      SUM(fs.total_amount) AS value
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
    WHERE dt.business_date >= $1 AND dt.business_date <= $2
  `;

  const queryParams = [startDate, endDate];
  let paramIndexRef = { current: 3 };
  const whereConditions = [];

  // Extract parameters from req.query and add them to whereConditions and queryParams
  const {
    restaurantId,
    product,
    machineId,
    transactionType,
    deliveryChannel,
    store,
    ocEmailId, // New filter
    omEmailId, // New filter
  } = req.query;

  let productFilter = null;
  if (product) {
    try {
      productFilter = JSON.parse(product);
    } catch (e) {
      console.error("Error parsing product filter:", e);
    }
  }
  if (productFilter && productFilter.type && productFilter.value) {
    whereConditions.push(
      `pm.${productFilter.type}::TEXT = $${paramIndexRef.current++}`
    );
    queryParams.push(productFilter.value);
  }

  let storeFilter = null;
  if (store) {
    try {
      storeFilter = JSON.parse(store);
    } catch (e) {
      console.error("Error parsing store filter:", e);
    }
  }
  if (storeFilter && storeFilter.type && storeFilter.value) {
    if (storeFilter.type === "state") {
      whereConditions.push(`sm.state = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "city") {
      whereConditions.push(`sm.city = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "storecode") {
      whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    }
  }

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
    queryParams.push(restaurantId);
  }
  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndexRef.current++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndexRef.current++}`);
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    whereConditions.push(`fs.delivery_channel = $${paramIndexRef.current++}`);
    queryParams.push(deliveryChannel);
  }
  // New OC and OM filters
  if (ocEmailId && ocEmailId !== "all") {
    whereConditions.push(`sm.ocemailid = $${paramIndexRef.current++}`);
    queryParams.push(ocEmailId);
  }
  if (omEmailId && omEmailId !== "all") {
    whereConditions.push(`sm.omemailid = $${paramIndexRef.current++}`);
    queryParams.push(omEmailId);
  }

  if (whereConditions.length > 0) {
    query += ` AND ` + whereConditions.join(" AND ");
  }

  query += `
    GROUP BY pm.productname
    ORDER BY value DESC;
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

app.get("/api/product/by-family-group", async (req, res) => {
  const { startDate, endDate } = getDateRange(req.query.timePeriod);

  let query = `
    SELECT
      di.item_family_group AS name,
      SUM(fs.total_amount) AS value
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    LEFT JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN dim_item di ON fs.item_code = di.item_code
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
    WHERE dt.business_date >= $1 AND dt.business_date <= $2
      AND di.item_family_group IS NOT NULL
  `;

  const queryParams = [startDate, endDate];
  let paramIndexRef = { current: 3 };
  const whereConditions = [];

  // Extract parameters from req.query and add them to whereConditions and queryParams
  const {
    restaurantId,
    product,
    machineId,
    transactionType,
    deliveryChannel,
    store,
    ocEmailId, // New filter
    omEmailId, // New filter
  } = req.query;

  let productFilter = null;
  if (product) {
    try {
      productFilter = JSON.parse(product);
    } catch (e) {
      console.error("Error parsing product filter:", e);
    }
  }
  if (productFilter && productFilter.type && productFilter.value) {
    whereConditions.push(
      `pm.${productFilter.type}::TEXT = $${paramIndexRef.current++}`
    );
    queryParams.push(productFilter.value);
  }

  let storeFilter = null;
  if (store) {
    try {
      storeFilter = JSON.parse(store);
    } catch (e) {
      console.error("Error parsing store filter:", e);
    }
  }
  if (storeFilter && storeFilter.type && storeFilter.value) {
    if (storeFilter.type === "state") {
      whereConditions.push(`sm.state = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "city") {
      whereConditions.push(`sm.city = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "storecode") {
      whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    }
  }

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
    queryParams.push(restaurantId);
  }
  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndexRef.current++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndexRef.current++}`);
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    whereConditions.push(`fs.delivery_channel = $${paramIndexRef.current++}`);
    queryParams.push(deliveryChannel);
  }
  // New OC and OM filters
  if (ocEmailId && ocEmailId !== "all") {
    whereConditions.push(`sm.ocemailid = $${paramIndexRef.current++}`);
    queryParams.push(ocEmailId);
  }
  if (omEmailId && omEmailId !== "all") {
    whereConditions.push(`sm.omemailid = $${paramIndexRef.current++}`);
    queryParams.push(omEmailId);
  }

  if (whereConditions.length > 0) {
    query += ` AND ` + whereConditions.join(" AND ");
  }

  query += `
    GROUP BY di.item_family_group
    ORDER BY value DESC;
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

app.get("/api/product/by-day-part", async (req, res) => {
  const { startDate, endDate } = getDateRange(req.query.timePeriod);

  let query = `
    SELECT
      di.item_day_part AS name,
      SUM(fs.total_amount) AS value
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    LEFT JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN dim_item di ON fs.item_code = di.item_code
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
    WHERE dt.business_date >= $1 AND dt.business_date <= $2
      AND di.item_day_part IS NOT NULL
  `;

  const queryParams = [startDate, endDate];
  let paramIndexRef = { current: 3 };
  const whereConditions = [];

  // Extract parameters from req.query and add them to whereConditions and queryParams
  const {
    restaurantId,
    product,
    machineId,
    transactionType,
    deliveryChannel,
    store,
    ocEmailId, // New filter
    omEmailId, // New filter
  } = req.query;

  let productFilter = null;
  if (product) {
    try {
      productFilter = JSON.parse(product);
    } catch (e) {
      console.error("Error parsing product filter:", e);
    }
  }
  if (productFilter && productFilter.type && productFilter.value) {
    whereConditions.push(
      `pm.${productFilter.type}::TEXT = $${paramIndexRef.current++}`
    );
    queryParams.push(productFilter.value);
  }

  let storeFilter = null;
  if (store) {
    try {
      storeFilter = JSON.parse(store);
    } catch (e) {
      console.error("Error parsing store filter:", e);
    }
  }
  if (storeFilter && storeFilter.type && storeFilter.value) {
    if (storeFilter.type === "state") {
      whereConditions.push(`sm.state = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "city") {
      whereConditions.push(`sm.city = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "storecode") {
      whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    }
  }

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
    queryParams.push(restaurantId);
  }
  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndexRef.current++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndexRef.current++}`);
    queryParams.push(transactionType);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    whereConditions.push(`fs.delivery_channel = $${paramIndexRef.current++}`);
    queryParams.push(deliveryChannel);
  }
  // New OC and OM filters
  if (ocEmailId && ocEmailId !== "all") {
    whereConditions.push(`sm.ocemailid = $${paramIndexRef.current++}`);
    queryParams.push(ocEmailId);
  }
  if (omEmailId && omEmailId !== "all") {
    whereConditions.push(`sm.omemailid = $${paramIndexRef.current++}`);
    queryParams.push(omEmailId);
  }

  if (whereConditions.length > 0) {
    query += ` AND ` + whereConditions.join(" AND ");
  }

  query += `
    GROUP BY di.item_day_part
    ORDER BY value DESC;
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

app.get("/api/sales/by-sale-type", async (req, res) => {
  const { startDate, endDate } = getDateRange(req.query.timePeriod);

  let query = `
    SELECT
      fs.sale_type AS name,
      SUM(fs.total_amount) AS value
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    LEFT JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
    WHERE dt.business_date >= $1 AND dt.business_date <= $2
      AND fs.sale_type IS NOT NULL
  `;

  const queryParams = [startDate, endDate];
  let paramIndexRef = { current: 3 };
  const whereConditions = [];

  // Extract parameters from req.query and add them to whereConditions and queryParams
  const {
    restaurantId,
    product,
    machineId,
    deliveryChannel,
    store,
    ocEmailId, // New filter
    omEmailId, // New filter
  } = req.query;

  let productFilter = null;
  if (product) {
    try {
      productFilter = JSON.parse(product);
    } catch (e) {
      console.error("Error parsing product filter:", e);
    }
  }
  if (productFilter && productFilter.type && productFilter.value) {
    whereConditions.push(
      `pm.${productFilter.type}::TEXT = $${paramIndexRef.current++}`
    );
    queryParams.push(productFilter.value);
  }

  let storeFilter = null;
  if (store) {
    try {
      storeFilter = JSON.parse(store);
    } catch (e) {
      console.error("Error parsing store filter:", e);
    }
  }
  if (storeFilter && storeFilter.type && storeFilter.value) {
    if (storeFilter.type === "state") {
      whereConditions.push(`sm.state = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "city") {
      whereConditions.push(`sm.city = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "storecode") {
      whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    }
  }

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
    queryParams.push(restaurantId);
  }
  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndexRef.current++}`);
    queryParams.push(machineId);
  }
  if (deliveryChannel && deliveryChannel !== "all") {
    whereConditions.push(`fs.delivery_channel = $${paramIndexRef.current++}`);
    queryParams.push(deliveryChannel);
  }
  // New OC and OM filters
  if (ocEmailId && ocEmailId !== "all") {
    whereConditions.push(`sm.ocemailid = $${paramIndexRef.current++}`);
    queryParams.push(ocEmailId);
  }
  if (omEmailId && omEmailId !== "all") {
    whereConditions.push(`sm.omemailid = $${paramIndexRef.current++}`);
    queryParams.push(omEmailId);
  }

  if (whereConditions.length > 0) {
    query += ` AND ` + whereConditions.join(" AND ");
  }

  query += `
    GROUP BY fs.sale_type
    ORDER BY value DESC;
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

app.get("/api/sales/by-delivery-channel", async (req, res) => {
  const { startDate, endDate } = getDateRange(req.query.timePeriod);

  let query = `
    SELECT
      fs.delivery_channel AS name,
      SUM(fs.total_amount) AS value
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    LEFT JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
    WHERE dt.business_date >= $1 AND dt.business_date <= $2
      AND fs.delivery_channel IS NOT NULL
  `;

  const queryParams = [startDate, endDate];
  let paramIndexRef = { current: 3 };
  const whereConditions = [];

  // Extract parameters from req.query and add them to whereConditions and queryParams
  const {
    restaurantId,
    product,
    machineId,
    transactionType,
    store,
    ocEmailId, // New filter
    omEmailId, // New filter
  } = req.query;

  let productFilter = null;
  if (product) {
    try {
      productFilter = JSON.parse(product);
    } catch (e) {
      console.error("Error parsing product filter:", e);
    }
  }
  if (productFilter && productFilter.type && productFilter.value) {
    whereConditions.push(
      `pm.${productFilter.type}::TEXT = $${paramIndexRef.current++}`
    );
    queryParams.push(productFilter.value);
  }

  let storeFilter = null;
  if (store) {
    try {
      storeFilter = JSON.parse(store);
    } catch (e) {
      console.error("Error parsing store filter:", e);
    }
  }
  if (storeFilter && storeFilter.type && storeFilter.value) {
    if (storeFilter.type === "state") {
      whereConditions.push(`sm.state = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "city") {
      whereConditions.push(`sm.city = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    } else if (storeFilter.type === "storecode") {
      whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
      queryParams.push(storeFilter.value);
    }
  }

  if (restaurantId && restaurantId !== "all") {
    whereConditions.push(`ds.store_id = $${paramIndexRef.current++}`);
    queryParams.push(restaurantId);
  }
  if (machineId && machineId !== "all") {
    whereConditions.push(`dn.node_id = $${paramIndexRef.current++}`);
    queryParams.push(machineId);
  }
  if (transactionType && transactionType !== "all") {
    whereConditions.push(`fs.sale_type = $${paramIndexRef.current++}`);
    queryParams.push(transactionType);
  }
  // New OC and OM filters
  if (ocEmailId && ocEmailId !== "all") {
    whereConditions.push(`sm.ocemailid = $${paramIndexRef.current++}`);
    queryParams.push(ocEmailId);
  }
  if (omEmailId && omEmailId !== "all") {
    whereConditions.push(`sm.omemailid = $${paramIndexRef.current++}`);
    queryParams.push(omEmailId);
  }

  if (whereConditions.length > 0) {
    query += ` AND ` + whereConditions.join(" AND ");
  }

  query += `
    GROUP BY fs.delivery_channel
    ORDER BY value DESC;
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

// Removed the /api/sales/by-pod endpoint

// New API endpoint for sales summary by store, filtered by OC/OM
app.get("/api/sales/by-oc-om-store-summary", async (req, res) => {
  const {
    timePeriod,
    ocEmailId,
    omEmailId,
    restaurantId,
    product,
    machineId,
    transactionType,
    deliveryChannel,
    store,
  } = req.query;

  const { startDate, endDate } = getDateRange(timePeriod);

  let query = `
    SELECT
      ds.store_id AS store_name,
      SUM(fs.total_amount) AS total_sales,
      COUNT(DISTINCT fs.sales_id) AS total_orders,
      COALESCE(SUM(fs.total_amount) / NULLIF(COUNT(DISTINCT fs.sales_id), 0), 0) AS avg_order_value,
      COUNT(DISTINCT fs.invoice_number) AS total_invoices
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_id = dt.time_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    LEFT JOIN dim_node dn ON fs.node_id = dn.node_id
    JOIN product_master pm ON fs.item_code = pm.productid::TEXT
    LEFT JOIN store_master sm ON ds.store_id = sm.storecode::TEXT
  `;

  const queryParams = [];
  const whereConditions = [];
  let paramIndexRef = { current: 1 };

  addCommonWhereConditions(
    whereConditions,
    queryParams,
    req.query,
    paramIndexRef
  );

  // Specific filter for ocEmailId or omEmailId
  if (ocEmailId && ocEmailId !== "all") {
    whereConditions.push(`sm.ocemailid = $${paramIndexRef.current++}`);
    queryParams.push(ocEmailId);
  }
  if (omEmailId && omEmailId !== "all") {
    whereConditions.push(`sm.omemailid = $${paramIndexRef.current++}`);
    queryParams.push(omEmailId);
  }

  if (whereConditions.length > 0) {
    query += ` WHERE ` + whereConditions.join(" AND ");
  }

  query += `
    GROUP BY ds.store_id
    ORDER BY ds.store_id;
  `;

  try {
    const { rows } = await pool.query(query, queryParams);
    const formattedData = rows.map((row) => ({
      storeName: row.store_name,
      totalSales: parseFloat(row.total_sales || 0),
      totalOrders: parseInt(row.total_orders || 0),
      avgOrderValue: parseFloat(row.avg_order_value || 0),
      totalInvoices: parseInt(row.total_invoices || 0),
    }));
    res.json(formattedData);
  } catch (err) {
    console.error(
      "Error fetching sales by OC/OM and store summary:",
      err.stack
    );
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/mock-data", async (req, res) => {
  const cachedData = myCache.get("mockData");
  if (cachedData) {
    return res.json(cachedData);
  }

  try {
    const { rows: restaurantRows } = await pool.query(
      "SELECT DISTINCT store_id AS id, store_id AS name FROM dim_store ORDER BY name"
    );
    const restaurants = restaurantRows.map((row) => ({
      id: row.id,
      name: row.name,
    }));

    const { rows: productRows } = await pool.query(`
      SELECT
        productid,
        productname,
        subcategory_1,
        reporting_2,
        piecategory_3,
        reporting_id_4
      FROM product_master
    `);

    const allProductsFlat = productRows.map((row) => ({
      id: row.productid,
      name: row.productname,
      subcategory_1: row.subcategory_1,
      reporting_2: row.reporting_2,
      piecategory_3: row.piecategory_3,
      reporting_id_4: row.reporting_id_4,
    }));

    const { rows: storeRows } = await pool.query(`
      SELECT storecode AS id, storename AS name, state, city
      FROM store_master
      ORDER BY state, city, name
    `);
    const allStoresFlat = storeRows.map((row) => ({
      id: row.id,
      name: row.name,
      state: row.state,
      city: row.city,
    }));

    const { rows: machineRows } = await pool.query(
      "SELECT DISTINCT node_id AS id, node_id AS name FROM dim_node ORDER BY name"
    );
    const machines = machineRows.map((row) => ({
      id: row.id,
      name: row.name,
    }));

    const { rows: transactionTypeRows } = await pool.query(
      "SELECT DISTINCT sale_type FROM fact_sales WHERE sale_type IS NOT NULL"
    );
    const transactionTypes = transactionTypeRows.map((row) => row.sale_type);

    const { rows: deliveryChannelRows } = await pool.query(
      "SELECT DISTINCT delivery_channel FROM fact_sales WHERE delivery_channel IS NOT NULL"
    );
    const deliveryChannels = deliveryChannelRows.map(
      (row) => row.delivery_channel
    );

    const { rows: ocEmailIdRows } = await pool.query(
      "SELECT DISTINCT ocemailid FROM store_master WHERE ocemailid IS NOT NULL ORDER BY ocemailid"
    );
    const ocEmailIds = ocEmailIdRows.map((row) => row.ocemailid);

    const { rows: omEmailIdRows } = await pool.query(
      "SELECT DISTINCT omemailid FROM store_master WHERE omemailid IS NOT NULL ORDER BY omemailid"
    );
    const omEmailIds = omEmailIdRows.map((row) => row.omemailid);

    const dataToCache = {
      restaurants,
      allProductsFlat,
      allStoresFlat,
      machines,
      transactionTypes,
      deliveryChannels,
      ocEmailIds, // Added OC emails to cache
      omEmailIds, // Added OM emails to cache
    };

    myCache.set("mockData", dataToCache);
    res.json(dataToCache);
  } catch (err) {
    console.error("Error fetching mock data:", err.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
