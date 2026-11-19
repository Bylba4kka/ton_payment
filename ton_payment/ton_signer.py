import base64
from dataclasses import dataclass
from typing import Literal

from pytoniq import WalletV4R2, WalletV5R1
from pytoniq.contract.contract import Contract
from pytoniq.contract.wallets.wallet import WALLET_V4_R2_CODE
from pytoniq.contract.wallets.wallet_v5 import WALLET_V5_R1_CODE
from pytoniq_core import Address, Cell
from pytoniq_core.crypto.keys import mnemonic_is_valid, mnemonic_to_private_key
from pytoniq_core.tlb.account import StateInit
from pytoniq_core.tlb.custom.wallet import WalletV4Data, WalletV5R1Data

WalletVersion = Literal["v4r2", "v5r1"]


@dataclass(slots=True, frozen=True)
class SignedMessage:
    """Signed external message: base64 BOC and its base64 hash."""

    boc: str
    msg_hash: str


class TonWalletSigner:
    """Sign outgoing TON transfers offline with the wallet mnemonic."""

    def __init__(
        self,
        *,
        mnemonic: str,
        version: WalletVersion,
        network_global_id: int,
    ) -> None:
        words = mnemonic.split()
        if not mnemonic_is_valid(words):
            msg = "Invalid wallet seed phrase (24 TON words expected)"
            raise ValueError(msg)
        public_key, self._private_key = mnemonic_to_private_key(words)
        self._version = version

        if version == "v5r1":
            data = WalletV5R1.create_data_cell(
                public_key,
                network_global_id=network_global_id,
            )
            code = WALLET_V5_R1_CODE
            self._wallet_id = WalletV5R1Data.deserialize(
                data.begin_parse(),
            ).wallet_id
        else:
            data = WalletV4R2.create_data_cell(public_key)
            code = WALLET_V4_R2_CODE
            self._wallet_id = WalletV4Data.deserialize(
                data.begin_parse(),
            ).wallet_id
        self._state_init = StateInit(code=code, data=data)
        self.address = Address((0, self._state_init.serialize().hash))

    def ensure_address(self, expected: str) -> None:
        if Address(expected) != self.address:
            msg = (
                f"Address from the seed phrase ({self.address.to_str()}) "
                " does not match with TON_WALLET_ADDRESS ({expected}). "
                "Checj TON_WALLET_VERSION and TON_NETWORK_GLOBAL_ID."
            )
            raise ValueError(
                msg,
            )

    def sign_transfer(
        self,
        *,
        destination: str,
        amount_nano: int,
        comment: str | None,
        valid_until: int,
        seqno: int,
    ) -> SignedMessage:
        wallet_message = WalletV5R1.create_wallet_internal_message(
            destination=Address(destination),
            value=amount_nano,
            body=comment or Cell.empty(),
            bounce=False,
        )
        if self._version == "v5r1":
            body = WalletV5R1.raw_create_transfer_msg(
                WalletV5R1,  # type: ignore[arg-type]
                self._private_key,
                seqno,
                self._wallet_id,
                [wallet_message],
                valid_until,
            )
        else:
            body = WalletV4R2.raw_create_transfer_msg(
                private_key=self._private_key,
                seqno=seqno,
                wallet_id=self._wallet_id,
                messages=[wallet_message],
                valid_until=valid_until,
            )

        external = Contract.create_external_msg(
            dest=self.address,
            state_init=self._state_init if seqno == 0 else None,
            body=body,
        )
        cell = external.serialize()
        return SignedMessage(
            boc=base64.b64encode(cell.to_boc()).decode(),
            msg_hash=base64.b64encode(cell.hash).decode(),
        )
