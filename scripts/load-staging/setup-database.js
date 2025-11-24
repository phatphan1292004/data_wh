require("dotenv").config();
const mysql = require("mysql2/promise");
const fs = require("fs").promises;
const path = require("path");
const {
  startProcessLog,
  endProcessLog,
  generateBatchId,
} = require("../../src/control/utils/logger");

/**
 * Setup all databases (Control, Staging, Warehouse)
 */
async function setupDatabases() {
  console.log("🚀 Setting up databases...");
  const batchId = generateBatchId("setup_db");

  const connection = await mysql.createConnection({
    host: process.env.DB_HOST || "localhost",
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || "root",
    password: process.env.DB_PASSWORD,
    multipleStatements: true,
  });

  try {
    // 1. Setup Control Database (first, for logging)
    console.log("🎛️ Creating control database...");
    const controlSQL = await fs.readFile(
      path.join(__dirname, "../../src/control/models/create-control-db.sql"),
      "utf8"
    );
    await connection.query(controlSQL);
    console.log("✅ Control database created");

    // Start logging
    await startProcessLog(batchId, "setup_databases");

    // 2. Setup Staging Database
    console.log("📦 Creating staging database...");
    const stagingSQL = await fs.readFile(
      path.join(__dirname, "../../src/staging/models/create-tables.sql"),
      "utf8"
    );
    await connection.query(stagingSQL);
    console.log("✅ Staging database created");

    // 3. Setup Warehouse Database
    console.log("🏢 Creating warehouse schema...");
    const schemaFiles = [
      "dim_date.sql",
      "dim_genre.sql",
      "dim_country.sql",
      "dim_person.sql",
      "fact_movie.sql",
    ];
    for (const file of schemaFiles) {
      const sql = await fs.readFile(
        path.join(__dirname, `../../src/warehouse/schema/${file}`),
        "utf8"
      );
      await connection.query(sql);
      console.log(`✅ Created ${file}`);
    }
    console.log("🎉 Database setup completed successfully!");

    // End logging with success
    await endProcessLog(batchId, "success", 0, 0, 0);
  } catch (error) {
    status = "failed";
    errorMessage = error.message || String(error);
    console.error("❌ Error setting up databases:", error);

    // End logging with failure
    await endProcessLog(batchId, "failed", 0, 0, 0, errorMessage);
    throw error;
  } finally {
    await connection.end();
  }
}

// Run if executed directly
if (require.main === module) {
  setupDatabases().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { setupDatabases };
