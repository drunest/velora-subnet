use pyo3::prelude::*;
use tokio::runtime::Runtime;
use chrono::{Utc, NaiveDateTime, TimeZone};
use ethers::{abi::Abi, contract::Contract, providers:: { Http, Middleware, Provider}, types::Address};
use ethers::core::types::U256;
use serde::Serialize;
use sha2::{Sha256, Digest};
use std::sync::Arc;
use serde_json::{self, Value};
use std::marker::Send;
use ethers::types::{Filter, Log, H160, H256, U64, I256, Block, BlockNumber};
use ethers::abi::RawLog;
use ethers::contract::EthLogDecode;
use ethers::contract::EthEvent;
use ethers::utils::hex;

use std::str::FromStr;
use pyo3::{IntoPy, PyObject};
use pyo3::types::{PyList, PyDict};
use futures::future::join_all;

const NUM_BLOCKS: u64 = 100; // Number of blocks to consider for average block time calculation
const FACTORY_ADDRESS: &str = "0x1F98431c8aD98523631AE4a59f267346ea31F984";
const POOL_CREATED_SIGNATURE: &str = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";
const SWAP_EVENT_SIGNATURE: &str = "c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const MINT_EVENT_SIGNATURE: &str = "7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde";
const BURN_EVENT_SIGNATURE: &str = "0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c";
const COLLECT_EVENT_SIGNATURE: &str = "70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0";

struct PyValue(Value);

impl IntoPy<PyObject> for PyValue {
    fn into_py(self, py: Python) -> PyObject {
        match self.0 {
            Value::Null => py.None(),
            Value::Bool(b) => b.into_py(py),
            Value::Number(n) => n.as_i64().unwrap().into_py(py),
            Value::String(s) => s.into_py(py),
            Value::Array(a) => {
                let py_list = PyList::empty(py);
                for item in a {
                    py_list.append(PyValue(item).into_py(py)).unwrap();
                }
                py_list.into_py(py)
            },
            Value::Object(o) => {
                let py_dict = PyDict::new(py);
                for (k, v) in o {
                    py_dict.set_item(k, PyValue(v).into_py(py)).unwrap();
                }
                py_dict.into_py(py)
            },
        }
    }
}

#[derive(Debug, EthEvent, Serialize)]
#[ethevent(name = "Swap", abi = "Swap(address indexed sender, address indexed to, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)")]
struct SwapEvent {
    sender: Address,
    to: Address,
    amount0: I256,
    amount1: I256,
    sqrt_price_x96: U256,
    liquidity: U256,
    tick: i32,  // ABI's int24 can fit in i32 in Rust
}

#[derive(Debug, EthEvent, Serialize)]
#[ethevent(name = "Mint", abi = "Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)")]
struct MintEvent {
    sender: Address,
    owner: Address,
    tick_lower: i32,  // int24 fits in i32
    tick_upper: i32,  // int24 fits in i32
    amount: U256,
    amount0: U256,
    amount1: U256,
}

#[derive(Debug, EthEvent, Serialize)]
#[ethevent(name = "Burn", abi = "Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)")]
struct BurnEvent {
    owner: Address,
    tick_lower: i32,  // int24 fits in i32
    tick_upper: i32,  // int24 fits in i32
    amount: U256,
    amount0: U256,
    amount1: U256,
}

#[derive(Debug, EthEvent, Serialize)]
#[ethevent(name = "Collect", abi = "Collect(address indexed owner, address recipient, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount0, uint128 amount1)")]
struct CollectEvent {
    owner: Address,
    recipient: Address,
    tick_lower: i32,  // int24 fits in i32
    tick_upper: i32,  // int24 fits in i32
    amount0: U256,
    amount1: U256,
}

#[derive(Debug, Serialize)]
enum UniswapEvent {
    Swap(SwapEvent),
    Mint(MintEvent),
    Burn(BurnEvent),
    Collect(CollectEvent),
}

impl EthLogDecode for UniswapEvent {
    fn decode_log(log: &RawLog) -> Result<Self, ethers::abi::Error> {
        if let Ok((event, _, _)) = decode_uniswap_event(&Log {
            address: H160::zero(),
            topics: log.topics.clone(),
            data: log.data.clone().into(),
            block_hash: None,
            block_number: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            transaction_log_index: None,
            log_type: None,
            removed: None,
        }) {
            Ok(event)
        } else {
            Err(ethers::abi::Error::InvalidData)
        }
    }
}

fn decode_uniswap_event(log: &Log) -> Result<(UniswapEvent, H256, u64), Box<dyn std::error::Error + Send + Sync>> {
    // Event signatures for Uniswap V3 pool events
    let swap_signature = H256::from_slice(&hex::decode(SWAP_EVENT_SIGNATURE).unwrap());
    let mint_signature = H256::from_slice(&hex::decode(MINT_EVENT_SIGNATURE).unwrap());
    let burn_signature = H256::from_slice(&hex::decode(BURN_EVENT_SIGNATURE).unwrap());
    let collect_signature = H256::from_slice(&hex::decode(COLLECT_EVENT_SIGNATURE).unwrap());

    // Parse the raw log data
    let raw_log = RawLog {
        topics: log.topics.clone(),
        data: log.data.to_vec(),
    };

    let hash = log.transaction_hash.ok_or("Missing transaction hash")?;
    let block_number = log.block_number.ok_or("Missing block number")?.as_u64();

    // Match based on event signature and decode the appropriate event
    if log.topics[0] == swap_signature {
        match <SwapEvent as EthLogDecode>::decode_log(&raw_log) {
            Ok(event) => return Ok((UniswapEvent::Swap(event), hash, block_number)),
            Err(err) => return Err(Box::new(err)),
        }
    } else if log.topics[0] == mint_signature {
        match <MintEvent as EthLogDecode>::decode_log(&raw_log) {
            Ok(event) => return Ok((UniswapEvent::Mint(event), hash, block_number)),
            Err(err) => return Err(Box::new(err)),
        }
    } else if log.topics[0] == burn_signature {
        match <BurnEvent as EthLogDecode>::decode_log(&raw_log) {
            Ok(event) => return Ok((UniswapEvent::Burn(event), hash, block_number)),
            Err(err) => return Err(Box::new(err)),
        }
    } else if log.topics[0] == collect_signature {
        match <CollectEvent as EthLogDecode>::decode_log(&raw_log) {
            Ok(event) => return Ok((UniswapEvent::Collect(event), hash, block_number)),
            Err(err) => return Err(Box::new(err)),
        }
    } else {
        println!("Unknown event signature: {:?}", log);
    }
    Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Unknown event signature")))
}


#[derive(Debug, EthEvent, Serialize)]
#[ethevent(name = "PoolCreated", abi = "PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool)")]
struct PoolCreatedEvent {
    token0: Address,
    token1: Address,
    fee: u32,
    tick_spacing: i32,
    pool: Address,
}


#[pyclass]
pub struct BlockchainClient {
    provider: Arc<Provider<Http>>,
}

#[pymethods]
impl BlockchainClient {
    #[new]
    fn new(rpc_url: String) -> Self {
        let provider: Arc<Provider<Http>> = Arc::new(Provider::<Http>::try_from(rpc_url).unwrap());
        BlockchainClient { provider }
    }

    fn get_pool_events_by_token_pairs(&self, py: Python, token_pairs: Vec<(String, String, u32)> , from_block: u64, to_block: u64) -> PyResult<PyObject> {
        let rt = Runtime::new().unwrap();
        match rt.block_on(get_pool_events_by_token_pairs(self.provider.clone(), token_pairs, U64::from(from_block), U64::from(to_block))) {
            Ok(result) => Ok(PyValue(serde_json::json!(result)).into_py(py)),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn get_block_number_range(&self, _py: Python, start_datetime: &str, end_datetime: &str) -> (u64, u64) {
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(get_block_number_range(self.provider.clone(), start_datetime, end_datetime)).unwrap();
        (result.0.as_u64(), result.1.as_u64())
    }

    fn fetch_pool_data(&self, py: Python, token_pairs: Vec<(String, String, u32)>, start_datetime: String, end_datetime: String, interval: String) -> PyResult<PyObject> {
        let rt = Runtime::new().unwrap();
        match rt.block_on(fetch_pool_data(self.provider.clone(), token_pairs, &start_datetime, &end_datetime, &interval)) {
            Ok(result) => Ok(PyValue(serde_json::json!(result)).into_py(py)),
            Err(e) => return Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn get_pool_created_events_between_two_timestamps(&self, py: Python, start_timestamp: &str, end_timestamp: &str) -> PyResult<PyObject> {
        let rt = Runtime::new().unwrap();
        match rt.block_on(get_pool_created_events_between_two_timestamps(self.provider.clone(), Address::from_str(FACTORY_ADDRESS).unwrap(), start_timestamp, end_timestamp)) {
            Ok(result) => Ok(PyValue(serde_json::json!(result)).into_py(py)),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }
    
}

async fn get_pool_address(provider: Arc<Provider<Http>>, factory_address: Address, token0: Address, token1: Address, fee: u32) -> Result<Address, Box<dyn std::error::Error + Send + Sync>> {
    // Load the Uniswap V3 factory ABI
    let abi_json = include_str!("contracts/uniswap_pool_factory_abi.json");
    let abi: Abi = serde_json::from_str(abi_json)?;

    // Instantiate the contract
    let factory = Contract::new(factory_address, abi, provider.clone());

    // Call the getPool function
    let pool_address: Address = factory.method("getPool", (token0, token1, U256::from(fee)))?.call().await?;

    Ok(pool_address)
}


async fn get_pool_events_by_pool_address(
    provider: Arc<Provider<Http>>,
    pool_addresses: Vec<H160>,
    from_block: U64,
    to_block: U64
) -> Result<Vec<Log>, Box<dyn std::error::Error + Send + Sync>> {
    let filter = Filter::new()
        .address(pool_addresses)
        .from_block(from_block)
        .to_block(to_block)
        .topic0(vec![
            H256::from_str(SWAP_EVENT_SIGNATURE).unwrap(),
            H256::from_str(MINT_EVENT_SIGNATURE).unwrap(),
            H256::from_str(BURN_EVENT_SIGNATURE).unwrap(),
            H256::from_str(COLLECT_EVENT_SIGNATURE).unwrap(),
        ]);
    println!("from_block: {:?}, to_block: {:?}", from_block, to_block);
    let logs = provider.get_logs(&filter).await?;
    
    Ok(logs)
}

async fn get_pool_events_by_token_pairs(
    provider: Arc<Provider<Http>>,
    token_pairs: Vec<(String, String, u32)>,
    from_block: U64,
    to_block: U64,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {

    // Get the Uniswap V3 factory address
    let factory_address = Address::from_str("0x1F98431c8aD98523631AE4a59f267346ea31F984")?;

    let futures = token_pairs.into_iter().map(|(token0, token1, fee)| {
        let provider = provider.clone();
        async move {
            let token0_address = Address::from_str(&token0)?;
            let token1_address = Address::from_str(&token1)?;
            let pool_address = get_pool_address(provider.clone(), factory_address, token0_address, token1_address, fee).await?;
            Ok(pool_address) as Result<Address, Box<dyn std::error::Error + Send + Sync>>
        }
    });

    let pool_addresses_results = join_all(futures).await;

    let mut pool_addresses = Vec::new();
    for result in pool_addresses_results {
        match result {
            Ok(pool_address) => pool_addresses.push(pool_address),
            Err(e) => return Err(e),
        }
    }

    println!("Fetched pool address: {:?}", pool_addresses);

    let logs = get_pool_events_by_pool_address(provider, pool_addresses, from_block, to_block).await?;
    
    let mut data = Vec::new();
    for log in logs {
        match decode_uniswap_event(&log) {
            Ok(event) => {
                let (uniswap_event, transaction_hash, block_number) = event;
                let mut uniswap_event_with_metadata = match uniswap_event {
                    UniswapEvent::Swap(event) => serde_json::json!({ "event": { "type": "swap", "data": event } }),
                    UniswapEvent::Mint(event) => serde_json::json!({ "event": { "type": "mint", "data": event } }),
                    UniswapEvent::Burn(event) => serde_json::json!({ "event": { "type": "burn", "data": event } }),
                    UniswapEvent::Collect(event) => serde_json::json!({ "event": { "type": "collect", "data": event } }),
                };
                uniswap_event_with_metadata.as_object_mut().unwrap().insert("transaction_hash".to_string(), serde_json::Value::String(hex::encode(transaction_hash.as_bytes())));
                uniswap_event_with_metadata.as_object_mut().unwrap().insert("block_number".to_string(), serde_json::Value::Number(serde_json::Number::from(block_number)));
                uniswap_event_with_metadata.as_object_mut().unwrap().insert("pool_address".to_string(), serde_json::Value::String(format!("{:?}", log.address)));
                data.push(uniswap_event_with_metadata);
            },
            Err(e) => return Err(e),
        }
    }

    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_string(&data)?);
    let overall_data_hash = format!("{:x}", hasher.finalize());
    Ok(serde_json::json!({ "data": data, "overall_data_hash": overall_data_hash }))
}


async fn get_block_number_range(provider:Arc::<Provider<Http>>, start_datetime: &str, end_datetime: &str) -> Result<(U64, U64), Box<dyn std::error::Error + Send + Sync>>{
    let first_naive_datetime = NaiveDateTime::parse_from_str(start_datetime, "%Y-%m-%d %H:%M:%S")
        .expect("Failed to parse date");
    let first_datetime_utc = Utc.from_utc_datetime(&first_naive_datetime);
    let first_timestamp = first_datetime_utc.timestamp() as u64;

    let second_naive_datetime = NaiveDateTime::parse_from_str(end_datetime, "%Y-%m-%d %H:%M:%S")
        .expect("Failed to parse date");
    let second_datetime_utc = Utc.from_utc_datetime(&second_naive_datetime);
    let second_timestamp = second_datetime_utc.timestamp() as u64;

    // Check if the given date time is more than the current date time
    let current_timestamp = Utc::now().timestamp() as u64;
    if first_timestamp > current_timestamp {
        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Given date time is in the future")));
    }

    // let block_number = provider.get_block_number().await?;
    let average_block_time = get_average_block_time(provider.clone()).await?;

    let start_block_number = get_block_number_from_timestamp(provider.clone(), first_timestamp, average_block_time).await?;
    let end_block_number = start_block_number + (second_timestamp - first_timestamp) / average_block_time;

    Ok((start_block_number, end_block_number))
}

async fn get_average_block_time(provider: Arc<Provider<Http>>) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    // Fetch the latest block
    let latest_block: Block<H256> = provider.get_block(BlockNumber::Latest).await?.ok_or("Latest block not found")?;
    let latest_block_number = latest_block.number.ok_or("Latest block number not found")?;

    // Create a vector of tasks to fetch block timestamps concurrently
    let mut tasks = Vec::new();
    for i in 0..NUM_BLOCKS {
        let provider = provider.clone();
        let block_number = latest_block_number - U64::from(i);
        tasks.push(tokio::spawn(async move {
            let block: Block<H256> = provider.get_block(block_number).await?.ok_or("Block not found")?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(block.timestamp.as_u64())
        }));
    }

    // Collect the results
    let mut timestamps = Vec::new();
    for task in tasks {
        timestamps.push(task.await??);
    }

    // Calculate the time differences between consecutive blocks
    let mut time_diffs = Vec::new();
    for i in 1..timestamps.len() {
        time_diffs.push(timestamps[i - 1] - timestamps[i]);
    }

    // Compute the average block time
    let total_time_diff: u64 = time_diffs.iter().sum();
    let average_block_time = total_time_diff / time_diffs.len() as u64;

    Ok(average_block_time)
}

async fn get_block_number_from_timestamp(
    provider: Arc<Provider<Http>>,
    timestamp: u64,
    average_block_time: u64
) -> Result<U64, Box<dyn std::error::Error + Send + Sync>> {
    // Fetch the latest block
    let latest_block: Block<H256> = provider.get_block(BlockNumber::Latest).await?.ok_or("Latest block not found")?;
    let latest_block_number = latest_block.number.ok_or("Latest block number not found")?;
    let latest_block_timestamp = latest_block.timestamp.as_u64();

    // Estimate the block number using the average block time
    let estimated_block_number = latest_block_number.as_u64() - (latest_block_timestamp - timestamp) / average_block_time;

    // Perform exponential search to find the range
    let mut low = U64::zero();
    let mut high = latest_block_number;
    let mut mid = U64::from(estimated_block_number);

    while low < high {
        let block: Block<H256> = provider.get_block(mid).await?.ok_or("Block not found")?;
        let block_timestamp = block.timestamp.as_u64();

        if block_timestamp < timestamp {
            low = mid + 1;
        } else {
            high = mid;
        }

        // Adjust mid for exponential search
        mid = (low + high) / 2;
    }

    Ok(low)
}

async fn fetch_pool_data(provider: Arc::<Provider<Http>>, token_pairs: Vec<(String, String, u32)>, start_datetime: &str, end_datetime: &str, _interval: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    // let date_str = "2024-09-27 19:34:56";
    let (from_block, to_block) = get_block_number_range(provider.clone(), start_datetime, end_datetime).await?;

    let pool_events = get_pool_events_by_token_pairs(provider.clone(), token_pairs, from_block, to_block,).await?;
    Ok(pool_events)
}

async fn get_pool_created_events_between_two_timestamps(
    provider: Arc<Provider<Http>>,
    factory_address: Address,
    start_timestamp: &str,
    end_timestamp: &str,
) -> Result<Vec<Value>, Box<dyn std::error::Error + Send + Sync>> {
    let (start_block_number, end_block_number) = get_block_number_range(provider.clone(), start_timestamp, end_timestamp).await?;

    let filter = Filter::new()
        .address(factory_address)
        .topic0(H256::from_str(POOL_CREATED_SIGNATURE).unwrap())
        .from_block(start_block_number)
        .to_block(end_block_number);

    let logs = provider.get_logs(&filter).await?;

    let mut pool_created_events = Vec::new();
    for log in logs {
        let raw_log = RawLog {
            topics: log.topics.clone(),
            data: log.data.to_vec(),
        };

        if log.topics[0] == H256::from_str(POOL_CREATED_SIGNATURE).unwrap() {
            let pool_created_event = <PoolCreatedEvent as EthLogDecode>::decode_log(&raw_log)?;
            pool_created_events.push(serde_json::json!({
                "token0": pool_created_event.token0,
                "token1": pool_created_event.token1,
                "fee": pool_created_event.fee,
                "tick_spacing": pool_created_event.tick_spacing,
                "pool": pool_created_event.pool,
                "block_number": log.block_number.unwrap().as_u64(),
            }));
        }
    }

    Ok(pool_created_events)
}

#[pymodule]
fn pool_data_fetcher(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<BlockchainClient>()?;
    Ok(())
}

// implement test logic
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_pool_data() {
        let token0 = "0xaea46a60368a7bd060eec7df8cba43b7ef41ad85";
        let token1 = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
        let start_datetime = "2024-10-11 10:34:56";
        let end_datetime = "2024-10-11 12:35:56";
        let interval = "1h";
        let rpc_url = "http://localhost:8545";
        let fee = 3000;

        let provider = Arc::new(Provider::<Http>::try_from(rpc_url).unwrap());
        let token_pairs = vec![(token0.to_string(), token1.to_string(), fee)];

        let __result = fetch_pool_data(provider, token_pairs, start_datetime, end_datetime, interval).await;
        assert!(__result.is_ok());
    }
}