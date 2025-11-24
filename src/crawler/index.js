const KKPhimCrawler = require("./kkphim.crawler");
const RawDataSaver = require("./save-raw");
const logger = require("../../utils/logger");
const {
  startProcessLog,
  endProcessLog,
  generateBatchId,
} = require("../control/utils/logger");

async function crawlKKPhim() {
  const crawler = new KKPhimCrawler();
  const saver = new RawDataSaver();

  const batchId = generateBatchId("crawl");
  let movieCount = 0;
  let savedPath = null;

  try {
    await startProcessLog(batchId, "crawl_kkphim");
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

    await endProcessLog(batchId, "success", movieCount, movieCount, 0);
    return {
      success: true,
      count: movieCount,
      path: savedPath,
      batchId,
    };
  } catch (error) {
    logger.error("Crawl failed:", error);
    await endProcessLog(
      batchId,
      "failed",
      movieCount,
      0,
      movieCount,
      error.message
    );
    throw error;
  } finally {
    await crawler.close();
  }
}

module.exports = { crawlKKPhim };
