const fs = require("fs").promises;
const path = require("path");
const logger = require("../../utils/logger");

class RawDataSaver {
  constructor() {
    this.rawDir = path.join(__dirname, "../../data/raw");
  }

  async ensureDirectory() {
    try {
      await fs.mkdir(this.rawDir, { recursive: true });
    } catch (error) {
      logger.error("Failed to create raw directory:", error);
      throw error;
    }
  }

  async save(data, source = "kkphim") {
    await this.ensureDirectory();

    const timestamp = new Date().toISOString().split("T")[0];
    const filename = `${source}_raw_${timestamp}.json`;
    const filepath = path.join(this.rawDir, filename);

    const rawData = {
      metadata: {
        source: source,
        crawledAt: new Date().toISOString(),
        totalRecords: data.length,
        version: "1.0",
      },
      data: data,
    };

    try {
      await fs.writeFile(filepath, JSON.stringify(rawData, null, 2), "utf8");
      logger.info(`Raw data saved to ${filename}`);
      return filepath;
    } catch (error) {
      logger.error("Failed to save raw data:", error);
      throw error;
    }
  }

  async load(filepath) {
    try {
      const content = await fs.readFile(filepath, "utf8");
      return JSON.parse(content);
    } catch (error) {
      logger.error("Failed to load raw data:", error);
      throw error;
    }
  }
}

module.exports = RawDataSaver;