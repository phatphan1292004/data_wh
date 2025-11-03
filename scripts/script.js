const { crawlKKPhim } = require("../src/crawler");

(async () => {
  try {
    const result = await crawlKKPhim();
    console.log("Crawl completed:", result);
    process.exit(0);
  } catch (error) {
    console.error("Crawl failed:", error);
    process.exit(1);
  }
})();