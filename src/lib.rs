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

// Helper that extracts ERC721 transfer events from a block
fn extract_erc721_events<'a, T, F>(
    blk: &'a eth::Block,
    process_event: F,
) -> impl Iterator<Item = T> + 'a
where
    F: Fn(u64, &[u8], u64, ERC721TransferEvent) -> Option<T> + 'a + Copy,
{
    let block_num = blk.number;
    blk.receipts().flat_map(move |receipt| {
        let hash = &receipt.transaction.hash;
        receipt.receipt.logs.iter().filter_map(move |log| {
            if let Some(event) = ERC721TransferEvent::match_and_decode(log) {
                process_event(block_num, hash, log.block_index as u64, event)
            } else {
                None
            }
        })
    })
}

fn get_transfers<'a>(blk: &'a eth::Block) -> impl Iterator<Item = Transfer> + 'a {
    extract_erc721_events(blk, |block_num, hash, log_index, event| {
        let from = Hex(&event.from).to_string();
        let to = Hex(&event.to).to_string();

        if !is_zero_address(&from) && !is_zero_address(&to) {
            Some(Transfer {
                block_num,
                trx_hash: Hex(hash).to_string(),
                log_index,
                from,
                to,
                token_id: event.token_id.to_string(),
            })
        } else {
            None
        }
    })
}

fn get_mints<'a>(blk: &'a eth::Block) -> impl Iterator<Item = Mint> + 'a {
    extract_erc721_events(blk, |block_num, hash, log_index, event| {
        let from = Hex(&event.from).to_string();

        if is_zero_address(&from) {
            Some(Mint {
                block_num,
                trx_hash: Hex(hash).to_string(),
                log_index,
                to: Hex(&event.to).to_string(),
                token_id: event.token_id.to_string(),
                uri: None,
            })
        } else {
            None
        }
    })
}

fn get_burns<'a>(blk: &'a eth::Block) -> impl Iterator<Item = Burn> + 'a {
    extract_erc721_events(blk, |block_num, hash, log_index, event| {
        let to = Hex(&event.to).to_string();

        if is_zero_address(&to) {
            Some(Burn {
                block_num,
                trx_hash: Hex(hash).to_string(),
                log_index,
                from: Hex(&event.from).to_string(),
                token_id: event.token_id.to_string(),
            })
        } else {
            None
        }
    })
}

fn is_zero_address(addr: &str) -> bool {
    addr.trim_start_matches("0x")
        .eq_ignore_ascii_case(ZERO_ADDRESS)
}
