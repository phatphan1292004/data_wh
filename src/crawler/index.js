const KKPhimCrawler = require("./kkphim.crawler");
const RawDataSaver = require("./save-raw");
const logger = require("../../utils/logger");

async function crawlKKPhim() {
  const crawler = new KKPhimCrawler();
  const saver = new RawDataSaver();

  const mysql = require('mysql2/promise');
  let logId = null;
  const startTime = new Date();
  let status = 'success';
  let errorMessage = null;
  let movieCount = 0;
  let savedPath = null;
  let connection = null;
  try {
    logger.info("Starting KKPhim crawl...");
    await crawler.initialize();
    const movieList = await crawler.getMovieList();
    logger.info(`Processing ${movieList.length} movies`);
    const enrichedMovies = [];
    for (let movie of movieList) {
      if (!movie.detailUrl) continue;
      logger.info(`Crawling: ${movie.title}`);
      const detail = await crawler.getMovieDetail(movie);
      if (detail) {
        const enrichedMovie = {
          ...movie,
          ...detail,
          crawledAt: new Date().toISOString(),
        };
        enrichedMovies.push(enrichedMovie);
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    savedPath = await saver.save(enrichedMovies, "kkphim");
    movieCount = enrichedMovies.length;
    logger.info(`Crawl completed. Saved ${movieCount} movies`);
    // Ghi log vào bảng processing_log
    connection = await mysql.createConnection({
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 3306,
      user: process.env.DB_USER || 'root',
      password: process.env.DB_PASSWORD,
      database: process.env.STAGING_DB_NAME || 'movie_staging'
    });
    const [logResult] = await connection.query(
      `INSERT INTO processing_log (batch_id, step_name, status, records_processed, records_success, records_failed, start_time, end_time, error_message)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        `batch_${startTime.getTime()}`,
        'crawl',
        status,
        movieCount,
        movieCount,
        0,
        startTime,
        new Date(),
        null
      ]
    );
    logId = logResult.insertId;
    return {
      success: true,
      count: movieCount,
      path: savedPath,
      logId
    };
  } catch (error) {
    status = 'failed';
    errorMessage = error.message || String(error);
    logger.error("Crawl failed:", error);
    // Ghi log lỗi vào bảng processing_log
    if (!connection) {
      const mysql = require('mysql2/promise');
      connection = await mysql.createConnection({
        host: process.env.DB_HOST || 'localhost',
        port: process.env.DB_PORT || 3306,
        user: process.env.DB_USER || 'root',
        password: process.env.DB_PASSWORD,
        database: process.env.STAGING_DB_NAME || 'movie_staging'
      });
    }
    await connection.query(
      `INSERT INTO processing_log (batch_id, step_name, status, records_processed, records_success, records_failed, start_time, end_time, error_message)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        `batch_${startTime.getTime()}`,
        'crawl',
        status,
        movieCount,
        0,
        movieCount,
        startTime,
        new Date(),
        errorMessage
      ]
    );
    throw error;
  } finally {
    await crawler.close();
    if (connection) await connection.end();
  }
}

module.exports = { crawlKKPhim };