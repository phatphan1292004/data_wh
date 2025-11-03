# Movie Data Warehouse

ETL pipeline for crawling and analyzing movie data from KKPhim.

## Project Structure

- **configs/**: Configuration files for data sources, mappings, and databases
- **src/crawler/**: Extract layer - web scraping with Puppeteer
- **src/staging/**: Transform layer - data cleaning and validation
- **src/warehouse/**: Load layer - dimensional model and data loading
- **src/access/**: Access layer - reporting and data mining
- **src/scheduler/**: Scheduling and monitoring
- **data/**: Storage for raw, staging, and warehouse data
- **logs/**: Application logs
- **scripts/**: Utility scripts for manual operations
- **tests/**: Test files
