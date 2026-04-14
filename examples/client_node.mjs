/**
 * Connect to OpenDuck from Node.js using the duckdb-async package.
 *
 * Like MotherDuck's `md:`, OpenDuck uses `openduck:` or `od:`.
 *
 * Prerequisites:
 *   npm install duckdb-async
 *   export OPENDUCK_TOKEN=your-token
 *
 * Usage:
 *   node examples/client_node.mjs
 */

import { Database } from "duckdb-async";

// --- Option 1: Direct connect to default database ---
const db1 = await Database.create("od:");
const conn1 = await db1.connect();
console.log("Tables:", await conn1.all("SHOW TABLES;"));
await conn1.close();
await db1.close();

// --- Option 2: Named database ---
const db2 = await Database.create("openduck:my_database");
const conn2 = await db2.connect();
console.log("Tables:", await conn2.all("SHOW TABLES;"));
await conn2.close();
await db2.close();

// --- Option 3: Short alias ---
const db3 = await Database.create("od:analytics");
const conn3 = await db3.connect();
// const events = await conn3.all("SELECT * FROM events LIMIT 10;");
await conn3.close();
await db3.close();

// --- Option 4: Custom endpoint ---
const db4 = await Database.create("openduck:staging?endpoint=http://localhost:7878");
await db4.close();

// --- Option 5: Hybrid — OpenDuck primary, local file attached ---
const db5 = await Database.create("openduck:warehouse");
const conn5 = await db5.connect();
await conn5.run("ATTACH 'local.duckdb' AS local;");
// await conn5.all("SELECT * FROM local.products JOIN warehouse.events ON ...");
await conn5.close();
await db5.close();
