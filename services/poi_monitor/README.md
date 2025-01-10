# POI Monitor Service

## Description

A service that monitors Proof of Indexing (POI) submissions across indexers in The Graph network,
detecting discrepancies and POI reuse and forwarding notifications to Slack when an issue is detected.

## Features

- Monitors POI submissions in real-time
- Detects when indexers submit different POIs for the same deployment/block
- Identifies POI reuse across different deployments/blocks
- Sends notifications to Slack when issues are found
- Prevents duplicate notifications for known issues
- Uses connection pooling for efficient database access
- Handles service restarts gracefully

## Setup

### Prerequisites

- Python 3.12+
- PostgreSQL 
- Docker (for containerized deployment)
- Slack webhook URL

### Environment Variables

Create a `.env` file with:
```
POSTGRES_DB=graphix
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
GRAPHIX_API_URL=http://localhost:8000/graphql
SLACK_WEBHOOK_URL=your_webhook_url_here # Required to run the service for notifications in Slack
CHECK_INTERVAL=300  # seconds between checks
```

### Local Development

1. Create a virtual environment:
```bash
# Create and activate virtual environment
python -m venv venv
# For Git Bash on Windows:
source venv/Scripts/activate
# For Linux/Mac:
source venv/bin/activate
```

2. Install dependencies:
```bash
# With virtual environment activated:
pip install -r services/poi_monitor/requirements.txt
pip install -r services/poi_monitor/requirements-dev.txt  # For development/testing
# Install package in editable mode (for development tools like pytest, mypy, etc.)
pip install -e services/poi_monitor
```

## Running the Service

### Using Docker Compose

The POI monitor requires several steps to run:

```bash
# 1. Start postgres database
docker-compose -f compose/dev.dependencies.yml up -d postgres
# pgAdmin 4 should now be able to connect to the database
# localhost:8000/graphql should not be accessible yet

# 2. Before we start the graphix service, we need to set the database URL:
export GRAPHIX_DB_URL=postgresql://postgres:password@localhost:5433/graphix

# 3. Build and start the graphix service
cargo build
./target/debug/graphix --database-url postgresql://postgres:password@localhost:5433/graphix
# The graphix service should now be accessible at http://localhost:8000/graphql

# 4. In a separate Git Bash terminal, build and start the POI monitor:
docker-compose -f compose/dev.dependencies.yml build poi-monitor # Build the POI monitor service
docker-compose -f compose/dev.dependencies.yml up poi-monitor # Start the POI monitor service
```

### Running Tests

```bash
pytest services/poi_monitor/tests/ --cov=services/poi_monitor/src --cov-report=term-missing
```

### Project Structure
```
graphix/                         # Root project directory
├── Dockerfile                   # Main graphix service Dockerfile
├── compose/
│   ├── dependencies.yml         # Base services configuration
│   ├── dev.dependencies.yml     # POI monitor service configuration
│   └── .env                     # Environment variables for Docker
└── services/
    └── poi_monitor/             # POI Monitor service
        ├── Dockerfile           # POI monitor service Dockerfile
        ├── src/                 # Source code
        ├── tests/               # Test files
        ├── migrations/          # Database migrations
        └── .env                 # Local environment variables
```

## Restarting the Service

Note: After making code changes, rebuild and restart the service:
```bash
docker-compose -f compose/dev.dependencies.yml build poi-monitor
docker-compose -f compose/dev.dependencies.yml up poi-monitor
```

For restarting without code changes (e.g., after updating environment variables):
```bash
docker-compose -f compose/dev.dependencies.yml restart poi-monitor
```

## Notifications

When a POI discrepancy is detected, a Slack message is sent with:
- Deployment CID and block number
- Different POI hashes submitted
- Which indexers submitted each POI
- Any instances of POI reuse from other deployments

Example notification:
```
🚨 *POI Discrepancy Detected*
*Deployment:* QmYyB6sr2366Vw2mcWBXy2pTwqJKNkqyZnmxPeQJGHyXav
*Block:* 1234567

*Submitted POIs:*
• Hash: 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
*Submitted by:* 0x1234567890abcdef1234567890abcdef12345678

• Hash: 0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890
Previously used 5 days ago:
• Network: mainnet
• Deployment: QmXyz789abcdef1234567890abcdef1234567890abcdef1234567890abcd
• Block: 1234000
• Indexer: 0xabcdef1234567890abcdef1234567890abcdef1234
```

### Technical Implementation
The POI monitor service compares POIs at the deployment level, where POIs for each deployment are only compared
when they share the same block height. This means we can effectively compare POIs across different networks, even
when their chain heads are at vastly different heights (e.g., Arbitrum at block 15M vs Ethereum at block 5M).
What matters is that for any given deployment, all POIs being compared were generated at the same block height
for that specific deployment.

1. Deployment-Scoped Comparisons:
   ```python
   def analyze_pois(self, deployment_id: str, block_number: int):
       # POIs are only compared within the same [deployment, block-height] combination
   ```

2. Independent Block Heights:
   - Each deployment is processed independently
   - Block numbers are compared within the same deployment
   - Different networks (e.g., Arbitrum at block 15M vs Ethereum at block 5M) 
     are handled correctly because their deployments are different

3. POI Reuse Detection:
   - Checks for POI reuse by comparing POI hashes across all deployments
   - Does not explicitly differentiate between networks
   - Deployment IDs are naturally network-specific because they include:
     - The network configuration in the manifest
     - Network-specific data sources
     - Other chain-specific parameters

### Limitations

The POI monitor service focuses on the most recent POI submissions and has some limitations regarding historical data:

- The service processes POIs at the latest block heights it finds for each deployment. 
- If multiple discrepancies exist across different block heights, only recent ones will be detected
- Therefore historical discrepancies may be missed if they occurred during service downtime

For example:
- The service was live, running during blocks 0 to 220,000,000
- The servce goes offline for some large number of blocks. e.g. 100,000,000 blocks
- If POI discrepancies may exist at blocks:
  - 230,000,000
  - 240,000,000
  - 250,000,000
  ...
  - 310,000,000
  - 320,000,000
- The service will only detect and notify about discrepancies at the latest blocks (e.g., 310M, 320M).
- Historical discrepancies at blocks 230M, 240M, 250M, etc. will not be detected

This behavior is acceptable for normal operation because:
1. Epochs typically last 24 hours
2. The service checks for discrepancies every few minutes (configurable via CHECK_INTERVAL)
3. Under normal operation, no discrepancies should be missed

However, developers should be aware that:
- If the service experiences downtime
- And POI discrepancies occur during that downtime
- Those historical discrepancies might not be detected when service resumes
