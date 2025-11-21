// Scheduler: Định kỳ chạy kiểm tra hệ thống và báo cáo dữ liệu
const cron = require('node-cron');
const { runAccessToolScheduler } = require('./index');

// Chạy mỗi ngày lúc 2h sáng
cron.schedule('0 2 * * *', async () => {
  console.log('⏰ [Scheduler] Running AccessTool Scheduler (2AM)...');
  await runAccessToolScheduler();
});

// Nếu chạy trực tiếp thì chạy luôn một lần
if (require.main === module) {
  runAccessToolScheduler()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('❌ Scheduler failed:', err);
      process.exit(1);
    });
}
