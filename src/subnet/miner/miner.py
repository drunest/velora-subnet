from communex.module import Module, endpoint
from communex.key import generate_keypair
from communex.compat.key import classic_load_key
from keylimiter import TokenBucketLimiter

import os
import json
import pool_data_fetcher


class Miner(Module):
    """
    A module class for mining and generating responses to prompts.

    Attributes:
        None

    Methods:
        generate: Generates a response to a given prompt using a specified model.
    """
    def __init__(self) -> None:
        super().__init__()
        
        self.pool_data_fetcher = pool_data_fetcher.BlockchainClient(os.getenv('ETHEREUM_RPC_NODE_URL'))

    @endpoint
    def fetch(self, query: dict) -> str:
        # Generate a response from scraping the rpc server
        token_pairs = query.get("token_pairs", None)
        start_datetime = query.get("start_datetime", None)
        end_datetime = query.get("end_datetime", None)
        token_pairs_for_pool = [tuple(token_pair) for token_pair in token_pairs]
        result = self.pool_data_fetcher.fetch_pool_data(token_pairs_for_pool, start_datetime, end_datetime, "1h")
        
        return json.dumps(result)


if __name__ == "__main__":
    """
    Example
    """
    from communex.module.server import ModuleServer
    import uvicorn

    key = classic_load_key("your_key_file")
    miner = Miner()
    refill_rate = 1 / 400
    # Implementing custom limit
    bucket = TokenBucketLimiter(20, refill_rate)
    server = ModuleServer(miner, key, limiter=bucket, subnets_whitelist=[41], use_testnet=True)
    app = server.get_fastapi_app()
    # token0 = "0xaea46a60368a7bd060eec7df8cba43b7ef41ad85"
    # token1 = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    # start_datetime = "2024-09-27 11:24:56"
    # end_datetime = "2024-09-27 15:25:56"
    # interval = "1h"
    # print(pool_data_fetcher.fetch_pool_data_py(token0, token1, start_datetime, end_datetime, interval))

    # Only allow local connections
    uvicorn.run(app, host="0.0.0.0", port=9962)
