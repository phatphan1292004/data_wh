const logger = require('../../../utils/logger');
const { deduplicateMovies } = require('./01_deduplicate');
const { standardizeData } = require('./02_standardize');
const { validateData } = require('./03_validate');
const { loadToWarehouse } = require('./04_load_to_dw');

/**
 * Chạy toàn bộ staging pipeline
 */
async function runStagingPipeline() {
  logger.info('🚀 Starting Staging Pipeline...');
  const startTime = Date.now();

  try {
    // Step 1: Deduplicate
    logger.info('📍 Step 1/4: Deduplicating data...');
    await deduplicateMovies();
    
    // Step 2: Standardize
    logger.info('📍 Step 2/4: Standardizing data...');
    await standardizeData();
    
    // Step 3: Validate
    logger.info('📍 Step 3/4: Validating data...');
    const validationResult = await validateData();
    
    if (validationResult.invalidCount > 0) {
      logger.warn(`⚠️ Found ${validationResult.invalidCount} invalid records`);
    }
    
    // Step 4: Load to Data Warehouse
    logger.info('📍 Step 4/4: Loading to Data Warehouse...');
    await loadToWarehouse();
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    logger.info(`✅ Staging Pipeline completed in ${duration}s`);
    
    return {
      success: true,
      duration,
      validation: validationResult
    };
  } catch (error) {
    logger.error('❌ Staging Pipeline failed:', error);
    throw error;
  }
}

module.exports = { runStagingPipeline };
