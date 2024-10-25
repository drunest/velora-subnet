import typer 
from typing import Annotated

from communex._common import get_node_url  # type: ignore
from communex.client import CommuneClient  # type: ignore
from communex.compat.key import classic_load_key  # type: ignore

from src.subnet.validator._config import ValidatorSettings
from src.subnet.validator.validator import get_subnet_netuid, VeloraValidator

app = typer.Typer()


@app.command("serve-subnet")
def serve(
    commune_key: Annotated[
        str, typer.Argument(help="Name of the key present in `~/.commune/key`")
    ],
    netuid: int = typer.Option(38, help="Netuid of the subnet"),
    network: str = typer.Option("testnet", help="Network to connect to"),
    call_timeout: int = 65,
):
    keypair = classic_load_key(commune_key)  # type: ignore
    settings = ValidatorSettings()  # type: ignore

    c_client = CommuneClient(get_node_url(use_testnet = network == "testnet"))  # type: ignore
    validator = VeloraValidator(
        keypair,
        netuid,
        c_client,
        call_timeout=call_timeout,
    )
    validator.validation_loop(settings)


if __name__ == "__main__":
    typer.run(serve)
