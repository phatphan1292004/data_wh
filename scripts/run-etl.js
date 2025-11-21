// ETL Orchestrator: Chạy toàn bộ quy trình từ crawl đến warehouse
require('dotenv').config();
const path = require('path');

async function runETLPipeline() {
  console.log('=== Bắt đầu ETL Pipeline: Crawl -> Staging -> Transform -> AccessTool ===');

  // 1. Crawl dữ liệu
  console.log('\n[1] Crawling...');
  await require('./crawling/crawl-once').main();

  // 2. Load raw vào staging
  console.log('\n[2] Load raw to staging...');
  await require('./load-staging/load-raw-to-staging').main();

  // 3. Setup database (nếu cần)
  console.log('\n[3] Setup database...');
  await require('./load-staging/setup-database').main();

  // 4. Chạy pipeline staging (deduplicate, standardize, validate, load_to_dw)
  console.log('\n[4] Run staging pipeline...');
  await require('../src/staging/steps/pipeline').runStagingPipeline();

  // 5. Transform (nếu có bước riêng)
  if (require('./transform/run-staging.js').main) {
    console.log('\n[5] Transform...');
    await require('./transform/run-staging').main();
  }

  // 6. AccessTool & kiểm tra hệ thống
  console.log('\n[6] AccessTool & System Health...');
  await require('./accesstool-scheduler/index').runAccessToolScheduler();

  console.log('\n=== ETL Pipeline hoàn tất ===');
}

if (require.main === module) {
  runETLPipeline()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('❌ ETL Pipeline failed:', err);
      process.exit(1);
    });
}

module.exports = { runETLPipeline };
