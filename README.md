# Velora Subnet

## What is Velora?

Velora is a specialized subnet built to fetch and manage pool data from Uniswap V3. It enables miners to extract crucial information about liquidity pools based on specific parameters such as token pairs, fee tiers, and time ranges. Validators coordinate this process by sending queries to miners, who in turn fetch the data and store it in the database for analysis and further use.

## Why is Velora Necessary in the Real World?

The decentralized finance (DeFi) ecosystem relies on liquidity pools for token swaps and market-making. Velora serves a critical role in improving the efficiency of pool data retrieval and management, particularly from Uniswap V3. It enables faster, more accurate access to key liquidity metrics, reducing the need for centralized infrastructure while ensuring data is efficiently captured and stored.

## Efficiency

Velora's architecture is designed for scalability and performance. By utilizing individual miners' own RPC endpoints—either through local Ethereum nodes or paid services—Velora ensures optimized data fetching speeds. This leads to higher throughput and more reliable query responses, enhancing both the user and miner experience in the network.

## Workflow

![Workflow](https://github.com/drunest/velora-subnet/blob/main/images/velora-workflow.png)

## Setup

### Running Ethereum node [Optional]

You can run your own Ethereum node locally using the following command:

```bash
docker compose up -d geth prysm
```

Please note that this process may take a significant amount of time to fetch all the necessary data.

If you have an alternative Ethereum node that you’d like to use, you can specify it in your `.env` file.


### Running Miner

1. Prerequisites:
   ```bash
   git clone https://github.com/drunest/velora-subnet.git
   cd velora-subnet
   python3 -m venv venv
   source venv/bin/activate
   export PYTHONPATH=.
   pip3 install -r requirements.txt
   ```

2. Set environment variables
    ```bash
    cp .env.miner .env
    ```

3. Fill .env variables

4. To run the miner:
   ```bash
   python3 -m src.subnet.miner.cli <your-key-name> <your-subnet-netuid> [--network <text>] [--ip <text>] [--port <number>]
   ```

### Running Validator

1. Prerequisites (same as for miners).

2. Set environment variables
    ```bash
    cp .env.validator .env
    ```

3. Fill .env variables

4. To run PostgreSQL Server:
    ```bash
    docker compose up -d postgres_db
    ```

5. To run the validator:
   ```bash
   python3 -m src.subnet.cli <name-of-your-com-key> [--network <text>] [--ip <text>] [--port <number>]
   ```

### Running Miner with PM2

To run the miner using PM2 for process management:
```bash
pm2 start "python3 -m src/subnet/miner/cli.py <your-key-name> --network mainnet --ip <ip address of registered module> --port <port number of registered module>" --name velora-miner
```

### Running Validator with PM2

To run the validator using PM2:
```bash
pm2 start "python3 -m src.subnet.cli <your-key-name> --network mainnet --ip <ip address of registered module> --port <port number of registered module>" --name velora-validator
```

## Scoring Miners

To evaluate miners based on their responses:

1. **Collect Results**:
   Gather the results from all miners, ensuring to include their unique identifiers and response data.

2. **Benchmarking**:
   Use the trusted results from trusted miners as a benchmark for comparison.

3. **Accuracy Assessment**:
   Score each miner's response based on its accuracy relative to the trusted miner's results. 

4. **Processing Time Measurement**:
   Evaluate the processing time for each miner's response. Normalize these times to derive a processing time score, ensuring faster responses are scored higher.

5. **Overall Scoring**:
   Calculate the overall score for each miner by averaging their accuracy score and processing time score. The final scores will range from 0 to 1, with higher scores indicating better performance.
