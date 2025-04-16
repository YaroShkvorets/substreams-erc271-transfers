mod abi;
mod pb;

use pb::events::{Burn, Events, Mint, Transfer};
use substreams::Hex;
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_ethereum::Event;

use abi::erc721::events::Transfer as ERC721TransferEvent;

substreams_ethereum::init!();

const ZERO_ADDRESS: &str = "0000000000000000000000000000000000000000";

/// Extracts events events from the contract(s)
#[substreams::handlers::map]
fn map_events(blk: eth::Block) -> Result<Events, substreams::errors::Error> {
    Ok(Events {
        transfers: get_transfers(&blk).collect(),
        mints: get_mints(&blk).collect(),
        burns: get_burns(&blk).collect(),
    })
}

fn get_transfers<'a>(blk: &'a eth::Block) -> impl Iterator<Item = Transfer> + 'a {
    blk.receipts().flat_map(|receipt| {
        let hash = &receipt.transaction.hash;
        receipt.receipt.logs.iter().filter_map(move |log| {
            if let Some(event) = ERC721TransferEvent::match_and_decode(log) {
                let from = Hex(&event.from).to_string();
                let to = Hex(&event.to).to_string();
                if !is_zero_address(&from) && !is_zero_address(&to) {
                    Some(Transfer {
                        from: Hex(&event.from).to_string(),
                        to: Hex(&event.to).to_string(),
                        trx_hash: Hex(hash).to_string(),
                        log_index: log.block_index as u64,
                        token_id: event.token_id.to_string(),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
    })
}

fn get_mints<'a>(blk: &'a eth::Block) -> impl Iterator<Item = Mint> + 'a {
    blk.receipts().flat_map(|receipt| {
        let hash = &receipt.transaction.hash;
        receipt.receipt.logs.iter().filter_map(move |log| {
            if let Some(event) = ERC721TransferEvent::match_and_decode(log) {
                let from = Hex(&event.from).to_string();
                if is_zero_address(&from) {
                    Some(Mint {
                        to: Hex(&event.to).to_string(),
                        trx_hash: Hex(hash).to_string(),
                        log_index: log.block_index as u64,
                        token_id: event.token_id.to_string(),
                        uri: "".into(),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
    })
}

fn get_burns<'a>(blk: &'a eth::Block) -> impl Iterator<Item = Burn> + 'a {
    blk.receipts().flat_map(|receipt| {
        let hash = &receipt.transaction.hash;
        receipt.receipt.logs.iter().filter_map(move |log| {
            if let Some(event) = ERC721TransferEvent::match_and_decode(log) {
                let to = Hex(&event.to).to_string();
                if is_zero_address(&to) {
                    Some(Burn {
                        from: Hex(&event.from).to_string(),
                        trx_hash: Hex(hash).to_string(),
                        log_index: log.block_index as u64,
                        token_id: event.token_id.to_string(),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
    })
}

fn is_zero_address(addr: &str) -> bool {
    addr.trim_start_matches("0x")
        .eq_ignore_ascii_case(ZERO_ADDRESS)
}
