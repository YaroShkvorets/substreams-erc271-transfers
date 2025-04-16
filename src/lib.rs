mod abi;
mod pb;

use pb::transfers::Transfer;
use pb::transfers::Transfers;

use substreams::Hex;
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_ethereum::Event;

use abi::erc721::events::Transfer as ERC721TransferEvent;

substreams_ethereum::init!();

/// Extracts transfers events from the contract(s)
#[substreams::handlers::map]
fn map_transfers(blk: eth::Block) -> Result<Transfers, substreams::errors::Error> {
    Ok(Transfers {
        transfers: get_transfers(&blk).collect(),
    })
}

fn get_transfers<'a>(blk: &'a eth::Block) -> impl Iterator<Item = Transfer> + 'a {
    blk.receipts().flat_map(|receipt| {
        let hash = &receipt.transaction.hash;

        receipt.receipt.logs.iter().flat_map(|log| {
            if let Some(event) = ERC721TransferEvent::match_and_decode(log) {
                return vec![new_erc721_transfer(hash, log.block_index, event)];
            }
            vec![]
        })
    })
}

fn new_erc721_transfer(hash: &[u8], log_index: u32, event: ERC721TransferEvent) -> Transfer {
    Transfer {
        from: Hex(&event.from).to_string(),
        to: Hex(&event.to).to_string(),
        quantity: "1".to_string(),
        trx_hash: Hex(hash).to_string(),
        log_index: log_index as u64,
        token_id: event.token_id.to_string(),
    }
}
