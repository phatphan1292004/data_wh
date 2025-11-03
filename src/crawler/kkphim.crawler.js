const puppeteer = require("puppeteer");
const logger = require("../../utils/logger");
const sourceConfig = require("../../configs/source.json").kkphim;

class KKPhimCrawler {
  constructor() {
    this.browser = null;
    this.page = null;
    this.config = sourceConfig;
  }

  async initialize() {
    try {
      this.browser = await puppeteer.launch({
        headless: true,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-blink-features=AutomationControlled",
        ],
        defaultViewport: null,
      });
      this.page = await this.browser.newPage();
      await this.page.setUserAgent(this.config.headers["User-Agent"]);
      logger.info("Crawler initialized successfully");
    } catch (error) {
      logger.error("Failed to initialize crawler:", error);
      throw error;
    }
  }

  async getMovieList() {
    try {
      await this.page.goto(this.config.baseUrl, {
        waitUntil: "networkidle2",
        timeout: this.config.limits.timeout,
      });

      const movies = await this.page.evaluate((selectors) => {
        const rows = Array.from(
          document.querySelectorAll(selectors.movieTable)
        ).slice(1);
        const results = [];

        rows.forEach((row) => {
          const tds = row.querySelectorAll("td");
          if (tds.length < 7) return;

          const title =
            tds[0]
              .querySelector("a")
              ?.innerText.trim()
              .replace(/\n/g, " ") || "";
          const detailUrl = tds[0].querySelector("a")?.href || "";
          const tmdbId = tds[3].querySelector("a")?.innerText.trim() || "";
          const status = tds[2].innerText.trim();
          const category = tds[4].innerText.trim();
          const updatedAt = tds[6].innerText.trim();

          results.push({
            title,
            detailUrl,
            tmdbId,
            status,
            category,
            updatedAt,
          });
        });

        return results;
      }, this.config.selectors);

      logger.info(`Found ${movies.length} movies`);
      return movies.slice(0, this.config.limits.maxMovies);
    } catch (error) {
      logger.error("Failed to get movie list:", error);
      throw error;
    }
  }

  async getMovieDetail(movie) {
    const detailPage = await this.browser.newPage();
    await detailPage.setUserAgent(this.config.headers["User-Agent"]);

    try {
      await detailPage.goto(movie.detailUrl, {
        waitUntil: "networkidle2",
        timeout: this.config.limits.timeout,
      });

      const fieldMap = require("../../configs/mapping.json").fieldMapping;

      const detail = await detailPage.evaluate(
        (selectors, fieldMap) => {
          const poster = document.querySelector(selectors.poster)?.src || "";

          let flatInfo = {};
          document.querySelectorAll(selectors.infoTable).forEach((tr) => {
            const keyVN =
              tr.querySelector("td:first-child")?.innerText.trim() || "";
            const value =
              tr.querySelector("td:last-child")?.innerText.trim() || "";
            const keyEN = fieldMap[keyVN] || keyVN;
            if (keyEN && value) flatInfo[keyEN] = value;
          });

          const description =
            document
              .querySelector(
                ".card-collapse-content article > header > p"
              )
              ?.innerText.trim() ||
            document
              .querySelector(".card-collapse-content")
              ?.innerText.trim() ||
            "";

          let episodes = [];
          document
            .querySelectorAll(selectors.episodes)
            .forEach((serverBlock) => {
              let grid = serverBlock.nextElementSibling;
              if (grid) {
                episodes.push(
                  ...Array.from(grid.querySelectorAll("a")).map((a) =>
                    a.innerText.trim()
                  )
                );
              }
            });

          return {
            poster,
            description,
            episodes,
            ...flatInfo,
          };
        },
        this.config.selectors,
        fieldMap
      );

      await detailPage.close();
      return detail;
    } catch (error) {
      logger.error(`Failed to get detail for ${movie.title}:`, error);
      await detailPage.close();
      return null;
    }
  }

  async close() {
    if (this.browser) {
      await this.browser.close();
      logger.info("Crawler closed");
    }
  }
}

module.exports = KKPhimCrawler;
