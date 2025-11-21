const { SystemMonitor, monitorSystem } = require('../../src/scheduler/monitor');

async function runAccessToolScheduler() {
  // Chạy kiểm tra hệ thống và log kết quả
  const report = await monitorSystem();
  console.log('AccessTool Scheduler Health Report:');
  console.log(JSON.stringify(report, null, 2));
}

if (require.main === module) {
  runAccessToolScheduler()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('❌ AccessTool Scheduler failed:', err);
      process.exit(1);
    });
}

module.exports = { runAccessToolScheduler };
