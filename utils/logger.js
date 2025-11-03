const fs = require("fs");
const path = require("path");

class Logger {
  constructor(logFile) {
    this.logDir = path.join(__dirname, "../logs");
    this.logFile = path.join(this.logDir, logFile);
    this.ensureLogDir();
  }

  ensureLogDir() {
    if (!fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }
  }

  log(level, message, data = null) {
    const timestamp = new Date().toISOString();
    const logEntry = {
      timestamp,
      level,
      message,
      ...(data && { data }),
    };

    const logLine = JSON.stringify(logEntry) + "\n";

    fs.appendFileSync(this.logFile, logLine);
    console.log(`[${timestamp}] [${level}] ${message}`);
  }

  info(message, data) {
    this.log("INFO", message, data);
  }

  error(message, data) {
    this.log("ERROR", message, data);
  }

  warn(message, data) {
    this.log("WARN", message, data);
  }
}

module.exports = new Logger("crawler.log");