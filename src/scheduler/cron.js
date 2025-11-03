require('dotenv').config();
const cron = require('node-cron');
const { crawlKKPhim } = require('../crawler');
const logger = require('../../utils/logger');
const { runStagingPipeline } = require('../staging/steps/pipeline');
const { monitorSystem } = require('./monitor');

class Scheduler {
  constructor() {
    this.tasks = [];
  }

  // Crawl phim mỗi ngày lúc 2h sáng
  scheduleDailyCrawl() {
    const task = cron.schedule('0 2 * * *', async () => {
      logger.info('🕐 [SCHEDULER] Starting daily crawl job...');
      try {
        const result = await crawlKKPhim();
        logger.info(`✅ [SCHEDULER] Daily crawl completed: ${result.totalMovies} movies`);
      } catch (error) {
        logger.error('❌ [SCHEDULER] Daily crawl failed:', error);
      }
    });

    this.tasks.push({ name: 'Daily Crawl', task });
    logger.info('✅ Scheduled daily crawl at 2:00 AM');
  }

  // Chạy staging pipeline mỗi 6 tiếng
  scheduleStaging() {
    const task = cron.schedule('0 */6 * * *', async () => {
      logger.info('🕐 [SCHEDULER] Starting staging pipeline...');
      try {
        await runStagingPipeline();
        logger.info('✅ [SCHEDULER] Staging pipeline completed');
      } catch (error) {
        logger.error('❌ [SCHEDULER] Staging pipeline failed:', error);
      }
    });

    this.tasks.push({ name: 'Staging Pipeline', task });
    logger.info('✅ Scheduled staging pipeline every 6 hours');
  }

  // Monitor hệ thống mỗi 15 phút
  scheduleMonitoring() {
    const task = cron.schedule('*/15 * * * *', async () => {
      try {
        await monitorSystem();
      } catch (error) {
        logger.error('❌ [SCHEDULER] Monitoring failed:', error);
      }
    });

    this.tasks.push({ name: 'System Monitor', task });
    logger.info('✅ Scheduled system monitoring every 15 minutes');
  }

  // Crawl nhanh mỗi 4 tiếng (lấy ít phim hơn)
  scheduleQuickCrawl() {
    const task = cron.schedule('0 */4 * * *', async () => {
      logger.info('🕐 [SCHEDULER] Starting quick crawl...');
      try {
        const result = await crawlKKPhim({ maxMovies: 5 });
        logger.info(`✅ [SCHEDULER] Quick crawl completed: ${result.totalMovies} movies`);
      } catch (error) {
        logger.error('❌ [SCHEDULER] Quick crawl failed:', error);
      }
    });

    this.tasks.push({ name: 'Quick Crawl', task });
    logger.info('✅ Scheduled quick crawl every 4 hours');
  }

  // Khởi động tất cả tasks
  start() {
    logger.info('🚀 Starting scheduler...');
    
    this.scheduleDailyCrawl();
    this.scheduleQuickCrawl();
    this.scheduleStaging();
    this.scheduleMonitoring();

    logger.info(`📅 Scheduler started with ${this.tasks.length} tasks`);
    logger.info('Press Ctrl+C to stop...');
  }

  // Dừng tất cả tasks
  stop() {
    logger.info('🛑 Stopping scheduler...');
    this.tasks.forEach(({ name, task }) => {
      task.stop();
      logger.info(`✅ Stopped: ${name}`);
    });
    logger.info('Scheduler stopped');
  }
}

// Chạy scheduler nếu file được execute trực tiếp
if (require.main === module) {
  const scheduler = new Scheduler();
  scheduler.start();

  // Graceful shutdown
  process.on('SIGINT', () => {
    logger.info('\n📢 Received SIGINT, shutting down gracefully...');
    scheduler.stop();
    process.exit(0);
  });

  process.on('SIGTERM', () => {
    logger.info('\n📢 Received SIGTERM, shutting down gracefully...');
    scheduler.stop();
    process.exit(0);
  });
}

module.exports = { Scheduler };
