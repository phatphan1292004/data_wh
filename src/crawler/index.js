const KKPhimCrawler = require("./kkphim.crawler");
const RawDataSaver = require("./save-raw");
const logger = require("../../utils/logger");

async function crawlKKPhim() {
  const crawler = new KKPhimCrawler();
  const saver = new RawDataSaver();

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

      // Rate limiting
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    const savedPath = await saver.save(enrichedMovies, "kkphim");
    logger.info(`Crawl completed. Saved ${enrichedMovies.length} movies`);

    return {
      success: true,
      count: enrichedMovies.length,
      path: savedPath,
    };
  } catch (error) {
    logger.error("Crawl failed:", error);
    throw error;
  } finally {
    await crawler.close();
  }
}

module.exports = { crawlKKPhim };