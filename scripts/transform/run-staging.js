const { runStagingPipeline } = require('../../src/staging/steps/pipeline');

(async () => {
  try {
    console.log('🚀 Starting staging pipeline...');
    const result = await runStagingPipeline();
    console.log('✅ Staging pipeline completed:', result);
    process.exit(0);
  } catch (error) {
    console.error('❌ Staging pipeline failed:', error);
    process.exit(1);
  }
})();
