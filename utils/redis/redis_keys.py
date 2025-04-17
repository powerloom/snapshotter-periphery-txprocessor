def block_tx_htable_key(namespace: str, block_number: int) -> str:
    return f'block_txs:{block_number}:{namespace}'

