"""
CommuneX example of a Text Validator Module

This module provides an example VeloraValidator class for validating text generated by modules in subnets.
The VeloraValidator retrieves module addresses from the subnet, prompts the modules to generate answers to a given question,
and scores the generated answers against the validator's own answers.

Classes:
    VeloraValidator: A class for validating text generated by modules in a subnet.

Functions:
    set_weights: Blockchain call to set weights for miners based on their scores.
    cut_to_max_allowed_weights: Cut the scores to the maximum allowed weights.
    extract_address: Extract an address from a string.
    get_subnet_netuid: Retrieve the network UID of the subnet.
    get_ip_port: Get the IP and port information from module addresses.

Constants:
    IP_REGEX: A regular expression pattern for matching IP addresses.
"""

import asyncio
import concurrent.futures
import json
import re
import time
from functools import partial
from datetime import timedelta, datetime

from communex.client import CommuneClient  # type: ignore
from communex.module.client import ModuleClient  # type: ignore
from communex.module.module import Module  # type: ignore
from communex.types import Ss58Address  # type: ignore
from substrateinterface import Keypair  # type: ignore

from ._config import ValidatorSettings
from ..utils import log
import pool_data_fetcher

from db.db_manager import DBManager

import random
import os
from dotenv import load_dotenv

load_dotenv()

IP_REGEX = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+")


def set_weights(
    settings: ValidatorSettings,
    score_dict: dict[
        int, float
    ],  # implemented as a float score from 0 to 1, one being the best
    # you can implement your custom logic for scoring
    netuid: int,
    client: CommuneClient,
    key: Keypair,
) -> None:
    """
    Set weights for miners based on their scores.

    Args:
        score_dict: A dictionary mapping miner UIDs to their scores.
        netuid: The network UID.
        client: The CommuneX client.
        key: The keypair for signing transactions.
    """

    # you can replace with `max_allowed_weights` with the amount your subnet allows
    score_dict = cut_to_max_allowed_weights(score_dict, settings.max_allowed_weights)

    # Create a new dictionary to store the weighted scores
    weighted_scores: dict[int, int] = {}

    # Calculate the sum of all inverted scores
    scores = sum(score_dict.values())

    # process the scores into weights of type dict[int, int] 
    # Iterate over the items in the score_dict
    for uid, score in score_dict.items():
        # Calculate the normalized weight as an integer
        weight = int(score * 1000 / scores)

        # Add the weighted score to the new dictionary
        weighted_scores[uid] = weight


    # filter out 0 weights
    weighted_scores = {k: v for k, v in weighted_scores.items() if v != 0}

    uids = list(weighted_scores.keys())
    weights = list(weighted_scores.values())
    # send the blockchain call
    client.vote(key=key, uids=uids, weights=weights, netuid=netuid)


def cut_to_max_allowed_weights(
    score_dict: dict[int, float], max_allowed_weights: int
) -> dict[int, float]:
    """
    Cut the scores to the maximum allowed weights.

    Args:
        score_dict: A dictionary mapping miner UIDs to their scores.
        max_allowed_weights: The maximum allowed weights (default: 420).

    Returns:
        A dictionary mapping miner UIDs to their scores, where the scores have been cut to the maximum allowed weights.
    """
    # sort the score by highest to lowest
    sorted_scores = sorted(score_dict.items(), key=lambda x: x[1], reverse=True)

    # cut to max_allowed_weights
    cut_scores = sorted_scores[:max_allowed_weights]

    return dict(cut_scores)


def extract_address(string: str):
    """
    Extracts an address from a string.
    """
    return re.search(IP_REGEX, string)


def get_subnet_netuid(clinet: CommuneClient, subnet_name: str = "replace-with-your-subnet-name"):
    """
    Retrieve the network UID of the subnet.

    Args:
        client: The CommuneX client.
        subnet_name: The name of the subnet (default: "foo").

    Returns:
        The network UID of the subnet.

    Raises:
        ValueError: If the subnet is not found.
    """

    subnets = clinet.query_map_subnet_names()
    for netuid, name in subnets.items():
        if name == subnet_name:
            return netuid
    raise ValueError(f"Subnet {subnet_name} not found")


def get_ip_port(modules_adresses: dict[int, str]):
    """
    Get the IP and port information from module addresses.

    Args:
        modules_addresses: A dictionary mapping module IDs to their addresses.

    Returns:
        A dictionary mapping module IDs to their IP and port information.
    """

    filtered_addr = {id: extract_address(addr) for id, addr in modules_adresses.items()}
    ip_port = {
        id: x.group(0).split(":") for id, x in filtered_addr.items() if x is not None
    }
    return ip_port


class VeloraValidator(Module):
    """
    A class for validating text generated by modules in a subnet.

    Attributes:
        client: The CommuneClient instance used to interact with the subnet.
        key: The keypair used for authentication.
        netuid: The unique identifier of the subnet.
        val_model: The validation model used for scoring answers.
        call_timeout: The timeout value for module calls in seconds (default: 60).

    Methods:
        get_modules: Retrieve all module addresses from the subnet.
        _get_miner_prediction: Prompt a miner module to generate an answer to the given question.
        _score_miner: Score the generated answer against the validator's own answer.
        get_miner_prompt: Generate a prompt for the miner modules.
        validate_step: Perform a validation step by generating questions, prompting modules, and scoring answers.
        validation_loop: Run the validation loop continuously based on the provided settings.
    """

    def __init__(
        self,
        key: Keypair,
        netuid: int,
        client: CommuneClient,
        call_timeout: int = 60,
    ) -> None:
        super().__init__()
        self.client = client
        self.key = key
        self.netuid = netuid
        self.val_model = "foo"
        self.call_timeout = call_timeout
        
        self.db_manager = DBManager()
        
        self.pool_data_fetcher = pool_data_fetcher.BlockchainClient(os.getenv('ETHEREUM_RPC_NODE_URL'))

    def get_addresses(self, client: CommuneClient, netuid: int) -> dict[int, str]:
        """
        Retrieve all module addresses from the subnet.

        Args:
            client: The CommuneClient instance used to query the subnet.
            netuid: The unique identifier of the subnet.

        Returns:
            A dictionary mapping module IDs to their addresses.
        """

        # Makes a blockchain query for the miner addresses
        module_addreses = client.query_map_address(netuid)
        return module_addreses

    def _get_miner_prediction(
        self,
        question: tuple[int, int],
        miner_info: tuple[list[str], Ss58Address],
    ) -> str | None:
        """
        Prompt a miner module to generate an answer to the given question.

        Args:
            question: The question to ask the miner module.
            miner_info: A tuple containing the miner's connection information and key.

        Returns:
            The generated answer from the miner module, or None if the miner fails to generate an answer.
        """
        connection, miner_key = miner_info
        module_ip, module_port = connection
        client = ModuleClient(module_ip, int(module_port), self.key)
        try:
            # handles the communication with the miner
            current_time = datetime.now()
            miner_answer = asyncio.run(
                client.call(
                    "fetch",
                    miner_key,
                    {"query": question},
                    timeout=self.call_timeout,  #  type: ignore
                )
            )
            miner_answer = json.loads(miner_answer)
            process_time = datetime.now() - current_time
            miner_answer["process_time"] = process_time

        except Exception as e:
            log(f"Miner {module_ip}:{module_port} failed to generate an answer")
            print(e)
            miner_answer = None
        return miner_answer

    def _score_miner(self, miner_prompt: dict, miner_answer: dict | None, ground_truth: dict) -> float:
        """
        Score the generated answer against the validator's own answer.

        Args:
            miner_answer: The generated answer from the miner module.

        Returns:
            The score assigned to the miner's answer.
        """

        # Implement your custom scoring logic here
        if not miner_answer:
            return 0
        
        # count the number of correct entries
        cnt_correct_entry = sum([int(miner_answer["data"][i]["transaction_hash"] == ground_truth["data"][i]["transaction_hash"]) for i in range(len(miner_answer["data"]))])
        cnt_all = len(miner_answer["data"])
        
        accuracy_score = ((cnt_correct_entry - cnt_all * 0.75) / cnt_all * 4) ^ 3

        return accuracy_score
    
    def add_new_time_range(self) -> None:
        """
        Add a new timetable entry to the database.
        """
        last_time_range = self.db_manager.fetch_last_time_range()
        if last_time_range == None:
            start = datetime(2021, 5, 4, 0, 0, 0)
            end = datetime(2021, 5, 5, 0, 0, 0)
        else:
            start = last_time_range["end"]
            end = last_time_range["end"] + timedelta(days=1)
        
        self.db_manager.add_timetable_entry(start, end)
        self.db_manager.create_token_pairs_table(start, end)
        
        if last_time_range:
            previous_token_pairs = self.db_manager.fetch_token_pairs(last_time_range["start"], last_time_range["end"])
            self.db_manager.add_token_pairs(start, end, previous_token_pairs)

        start_date_str = start.strftime("%Y-%m-%d %H:%M:%S")
        end_date_str = end.strftime("%Y-%m-%d %H:%M:%S")
        
        log(f"Fetching token pairs between {start_date_str} and {end_date_str}")
        
        token_pairs = self.pool_data_fetcher.get_pool_created_events_between_two_timestamps(start_date_str, end_date_str)
        self.db_manager.add_token_pairs(start, end, token_pairs)
        
        return start, end
    
    def get_time_range(self) -> tuple[datetime, datetime]:
        """
        Get the time range for the miner modules.

        Returns:
            The time range for the miner modules.
        """
        incompleted_time_range = self.db_manager.fetch_incompleted_time_range()
        
        if not incompleted_time_range:
            return self.add_new_time_range()
        else:
            return incompleted_time_range[0]["start"], incompleted_time_range[0]["end"]
    
    def get_token_pair(self, start: datetime, end: datetime) -> list[dict[str, str]]:
        """
        Get the token pairs for the miner modules.

        Args:
            start: The start datetime.
            end: The end datetime.

        Returns:
            The token pairs for the miner modules.
        """
        token_pairs = self.db_manager.fetch_incompleted_token_pairs(start, end)
        
        if not token_pairs:
            self.db_manager.mark_time_range_as_complete(start, end)
            return None
        return token_pairs[0]

    def get_miner_prompt(self) -> dict:
        """
        Generate a prompt for the miner modules.

        Returns:
            The generated prompt for the miner modules.
        """
        while True:
            time_range = self.get_time_range()
            token_pair = self.get_token_pair(time_range[0], time_range[1])
            
            if token_pair:
                break

        # Implement your custom prompt generation logic here
        token_a=token_pair["token_a"]
        token_b=token_pair["token_b"]
        token_fee=f'{token_pair["fee"]}'
        start_datetime=time_range[0].strftime("%Y-%m-%d %H:%M:%S")
        end_datetime=time_range[1].strftime("%Y-%m-%d %H:%M:%S")
        print(f"Prompting miners with token pair {type(token_a)} {type(token_b)} {type(token_fee)} {type(start_datetime)} {type(end_datetime)}")
        return {"token_a": token_a, "token_b": token_b, "fee": token_fee, "start_datetime": start_datetime, "end_datetime": end_datetime}
        
    def check_miner_answer(self, miner_prompt: dict, miner_answer: dict | None) -> bool:
        """
        Check if the miner answers are valid.
        
        Args:
            miner_prompt: The prompt for the miner modules.
            miner_answer: The generated answer from the miner module.
        """
        token_a = miner_prompt.get("token_a", None)
        token_b = miner_prompt.get("token_b", None)
        token_fee = int(miner_prompt.get("fee", None))
        start_datetime = miner_prompt.get("start_datetime", None)
        end_datetime = miner_prompt.get("end_datetime", None)
        
        block_number_start, block_number_end = self.pool_data_fetcher.get_block_number_range(start_datetime, end_datetime)
        
        miner_data = miner_answer.get("data", None)
        if miner_data is None:
            return False
        ANSWER_CHECK_COUNT = 10
        for _ in range(ANSWER_CHECK_COUNT):
            block_data = random.choice(miner_data)
            block_number = block_data.get("block_number", None)
            
            if block_number is None:
                return False
            if block_number < block_number_start or block_number > block_number_end:
                return False
            
            block_data_from_pools = self.pool_data_fetcher.get_pool_events_by_token_pairs(token_a, token_b, block_number, block_number, token_fee)
            for block_data_of_pool in block_data_from_pools.get("data", []):
                if block_data_of_pool.get("transaction_hash") == block_data.get("transaction_hash"):
                    return True
        return False

    def save_pool_data(self, miner_prompt: dict, miner_answer: dict) -> None:
        """
        Save the pool data to the database.
        
        Args:
            miner_prompt: The prompt for the miner modules.
            miner_answer: The generated answer from the miner module
        """
        token_a = miner_prompt.get("token_a", None)
        token_b = miner_prompt.get("token_b", None)
        token_fee = miner_prompt.get("fee", None)
        start_datetime = miner_prompt.get("start_datetime", None)
        end_datetime = miner_prompt.get("end_datetime", None)
        
        self.db_manager.create_pool_data_table(token_a, token_b, token_fee)
        self.db_manager.add_pool_data(token_a, token_b, token_fee, miner_answer)
        
        self.db_manager.mark_token_pair_as_complete(start_datetime, end_datetime, token_a, token_b, token_fee)
        
        token_pairs = self.db_manager.fetch_incompleted_token_pairs(start_datetime, end_datetime)
        
        if not token_pairs:
            self.db_manager.mark_time_range_as_complete(start_datetime, end_datetime)

    def score_miners(self, miner_results, trust_miner_result):
        """
        Score the miners based on their answers.
        
        Args:
            miner_results: The results of the miner modules.
            trust_miner_results: The results of the trusted miner module.
        """
        accuracy_score: dict[int, float] = {}
        for uid, miner_response in miner_results:
            miner_answer = miner_response
            if not miner_answer:
                log(f"Skipping miner {uid} that didn't answer")
                continue

            score = self._score_miner(miner_answer, trust_miner_result)
            time.sleep(0.5)
            # score has to be lower or eq to 1, as one is the best score, you can implement your custom logic
            assert score <= 1
            accuracy_score[uid] = score
        
        process_time_score = {uid: miner_answer["process_time"].total_seconds() for uid, miner_answer in miner_results}
        max_time = max(process_time_score.values())
        min_time = min(process_time_score.values())
        process_time_score = {uid: 1 - 0.5 * (process_time - min_time) / (max_time - min_time) for uid, process_time in process_time_score.items()}
        
        overall_score = {uid: (accuracy_score[uid] + process_time_score[uid]) / 2 for uid in accuracy_score.keys()}
        return overall_score
    
    async def validate_step(
        self, syntia_netuid: int, settings: ValidatorSettings
    ) -> None:
        """
        Perform a validation step.

        Generates questions based on the provided settings, prompts modules to generate answers,
        and scores the generated answers against the validator's own answers.

        Args:
            syntia_netuid: The network UID of the subnet.
        """

        # retrive the miner information
        modules_adresses = self.get_addresses(self.client, syntia_netuid)
        modules_keys = self.client.query_map_key(syntia_netuid)
        val_ss58 = self.key.ss58_address
        if val_ss58 not in modules_keys.values():
            raise RuntimeError(f"validator key {val_ss58} is not registered in subnet")

        modules_info: dict[int, tuple[list[str], Ss58Address]] = {}

        modules_filtered_address = get_ip_port(modules_adresses)
        for module_id in modules_keys.keys():
            module_addr = modules_filtered_address.get(module_id, None)
            if not module_addr:
                continue
            modules_info[module_id] = (module_addr, modules_keys[module_id])

        score_dict: dict[int, float] = {}

        miner_prompt = self.get_miner_prompt()
        get_miner_prediction = partial(self._get_miner_prediction, miner_prompt)

        log(f"Selected the following miners: {modules_info.keys()}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            it = executor.map(get_miner_prediction, modules_info.values())
            miner_answers = [*it]
        
        miner_results = zip(modules_info.keys(), miner_answers)
        if not miner_results:
            log("No miner managed to give an answer")
            return None
        overall_hashes = [miner_answer['overall_data_hash'] for miner_answer in miner_answers if miner_answer != None]
        if not overall_hashes:
            log("No miner managed to give a valid answer")
            return None
        most_common_hash = max(set(overall_hashes), key=overall_hashes.count)
        
        trust_miner_results = [(key, miner_answer) for key, miner_answer in miner_results if miner_answer !=None and miner_answer['overall_data_hash'] == most_common_hash]
        
        if not self.check_miner_answer(miner_prompt, trust_miner_results[0][1]):
            log("Miner answers are not valid")
            return None
        
        self.save_pool_data(miner_prompt, trust_miner_results[0][1])

        score_dict = self.score_miners(miner_results, trust_miner_results[0][1])

        if not score_dict:
            log("No miner managed to give a valid answer")
            return None

        # the blockchain call to set the weights
        _ = set_weights(settings, score_dict, self.netuid, self.client, self.key)

    def validation_loop(self, settings: ValidatorSettings) -> None:
        """
        Run the validation loop continuously based on the provided settings.

        Args:
            settings: The validator settings to use for the validation loop.
        """

        while True:
            start_time = time.time()
            _ = asyncio.run(self.validate_step(self.netuid, settings))

            elapsed = time.time() - start_time
            if elapsed < settings.iteration_interval:
                sleep_time = settings.iteration_interval - elapsed
                log(f"Sleeping for {sleep_time}")
                time.sleep(sleep_time)
