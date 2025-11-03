require('dotenv').config();
const mysql = require('mysql2/promise');
const logger = require('../../utils/logger');
const fs = require('fs').promises;
const path = require('path');

class SystemMonitor {
  constructor() {
    this.dbConfig = {
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 3306,
      user: process.env.DB_USER || 'root',
      password: process.env.DB_PASSWORD,
    };
  }

  async checkDatabaseConnection() {
    try {
      const connection = await mysql.createConnection(this.dbConfig);
      await connection.ping();
      await connection.end();
      return { status: 'OK', message: 'Database connection successful' };
    } catch (error) {
      return { status: 'ERROR', message: error.message };
    }
  }

  async checkStagingDatabase() {
    try {
      const connection = await mysql.createConnection({
        ...this.dbConfig,
        database: process.env.STAGING_DB_NAME || 'movie_staging'
      });

      const [rows] = await connection.query(
        'SELECT COUNT(*) as count FROM staging_movies'
      );
      
      await connection.end();
      
      return {
        status: 'OK',
        recordCount: rows[0].count,
        message: `Staging has ${rows[0].count} records`
      };
    } catch (error) {
      return { status: 'ERROR', message: error.message };
    }
  }

  async checkWarehouseDatabase() {
    try {
      const connection = await mysql.createConnection({
        ...this.dbConfig,
        database: process.env.WAREHOUSE_DB_NAME || 'movie_dwh'
      });

      const [movieRows] = await connection.query(
        'SELECT COUNT(*) as count FROM fact_movie'
      );

      const [genreRows] = await connection.query(
        'SELECT COUNT(*) as count FROM dim_genre'
      );

      const [countryRows] = await connection.query(
        'SELECT COUNT(*) as count FROM dim_country'
      );
      
      await connection.end();
      
      return {
        status: 'OK',
        movies: movieRows[0].count,
        genres: genreRows[0].count,
        countries: countryRows[0].count,
        message: `Warehouse: ${movieRows[0].count} movies, ${genreRows[0].count} genres, ${countryRows[0].count} countries`
      };
    } catch (error) {
      return { status: 'ERROR', message: error.message };
    }
  }

  async checkRawDataFiles() {
    try {
      const rawDataPath = path.join(__dirname, '../../data/raw');
      const files = await fs.readdir(rawDataPath);
      const jsonFiles = files.filter(f => f.endsWith('.json'));
      
      return {
        status: 'OK',
        fileCount: jsonFiles.length,
        message: `${jsonFiles.length} raw data files found`
      };
    } catch (error) {
      return { status: 'ERROR', message: error.message };
    }
  }

  async checkDiskSpace() {
    try {
      const dataPath = path.join(__dirname, '../../data');
      
      // Đơn giản check có thể ghi file không
      const testFile = path.join(dataPath, '.monitor_test');
      await fs.writeFile(testFile, 'test');
      await fs.unlink(testFile);
      
      return {
        status: 'OK',
        message: 'Disk space available'
      };
    } catch (error) {
      return { status: 'ERROR', message: 'Cannot write to disk' };
    }
  }

  async getSystemHealth() {
    logger.info('🔍 [MONITOR] Checking system health...');

    const checks = {
      database: await this.checkDatabaseConnection(),
      staging: await this.checkStagingDatabase(),
      warehouse: await this.checkWarehouseDatabase(),
      rawFiles: await this.checkRawDataFiles(),
      disk: await this.checkDiskSpace()
    };

    const allHealthy = Object.values(checks).every(check => check.status === 'OK');

    const report = {
      timestamp: new Date().toISOString(),
      overallStatus: allHealthy ? 'HEALTHY' : 'ISSUES_DETECTED',
      checks
    };

    if (allHealthy) {
      logger.info('✅ [MONITOR] System is healthy');
    } else {
      logger.warn('⚠️ [MONITOR] System has issues:', report);
    }

    return report;
  }

  async logHealthReport() {
    const report = await this.getSystemHealth();
    
    const logPath = path.join(__dirname, '../../logs/health.log');
    const logEntry = `${report.timestamp} - ${report.overallStatus}\n${JSON.stringify(report.checks, null, 2)}\n\n`;
    
    await fs.appendFile(logPath, logEntry);
    
    return report;
  }
}

async function monitorSystem() {
  const monitor = new SystemMonitor();
  return await monitor.logHealthReport();
}

// Chạy monitoring nếu file được execute trực tiếp
if (require.main === module) {
  monitorSystem()
    .then(report => {
      console.log('📊 System Health Report:');
      console.log(JSON.stringify(report, null, 2));
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Monitoring failed:', error);
      process.exit(1);
    });
}

module.exports = { SystemMonitor, monitorSystem };
