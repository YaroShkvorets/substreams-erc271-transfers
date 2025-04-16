mod abi;
mod pb;

use pb::events::{Burn, Events, Mint, Transaction, Transfer};
use substreams::Hex;
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_ethereum::Event;

use abi::erc721::events::Transfer as ERC721TransferEvent;
use ethereum_types::U256;

const ZERO_ADDRESS: &str = "0000000000000000000000000000000000000000";

/// Extracts events events from the contract(s)
#[substreams::handlers::map]
fn map_events(blk: eth::Block) -> Result<Events, substreams::errors::Error> {
    let transfers: Vec<Transfer> = get_transfers(&blk).collect();
    let mints: Vec<Mint> = get_mints(&blk).collect();
    let burns: Vec<Burn> = get_burns(&blk).collect();

    // Collect all transaction hashes involved in any ERC721 event
    let mut event_tx_hashes = std::collections::HashSet::new();
    transfers.iter().for_each(|t| {
        event_tx_hashes.insert(&t.trx_hash);
    });
    mints.iter().for_each(|m| {
        event_tx_hashes.insert(&m.trx_hash);
    });
    burns.iter().for_each(|b| {
        event_tx_hashes.insert(&b.trx_hash);
    });

    let transactions = get_transactions(&blk, &event_tx_hashes);

    Ok(Events {
        transfers,
        mints,
        burns,
        transactions,
    })
}

fn get_transactions(
    blk: &eth::Block,
    event_tx_hashes: &std::collections::HashSet<&String>,
) -> Vec<Transaction> {
    let block_number = blk.number;
    let block_timestamp = blk
        .header
        .as_ref()
        .and_then(|h| h.timestamp.as_ref())
        .map(|t| t.seconds as u64)
        .unwrap_or(0);
    let block_hash = format!("0x{}", Hex(&blk.hash));

    blk.transaction_traces
        .iter()
        .filter(|trace| event_tx_hashes.contains(&format!("0x{}", Hex(&trace.hash))))
        .map(|trace| Transaction {
            block_number,
            block_timestamp,
            block_hash: block_hash.clone(),
            tx_hash: format!("0x{}", Hex(&trace.hash)),
            nonce: trace.nonce,
            position: trace.index,
            from_address: format!("0x{}", Hex(&trace.from)),
            to_address: format!("0x{}", Hex(&trace.to)),
            value: trace
                .value
                .as_ref()
                .map(|v| format!("0x{}", Hex(&v.bytes)))
                .unwrap_or_else(|| "0x0".to_string()),
            tx_fee: trace
                .gas_price
                .as_ref()
                .map(|v| {
                    let fee = U256::from_big_endian(&v.bytes) * trace.gas_used;
                    format!("0x{:x}", fee)
                })
                .unwrap_or_else(|| "0x0".to_string()),
            gas_price: trace
                .gas_price
                .as_ref()
                .map(|v| format!("0x{}", Hex(&v.bytes)))
                .unwrap_or_else(|| "0x0".to_string()),
            gas_limit: trace.gas_limit,
            gas_used: trace.gas_used,
            v: format!("0x{}", Hex(&trace.v)),
            r: format!("0x{}", Hex(&trace.r)),
            s: format!("0x{}", Hex(&trace.s)),
        })
        .collect()
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
        let from = format!("0x{}", Hex(&event.from));
        let to = format!("0x{}", Hex(&event.to));

        if !is_zero_address(&from) && !is_zero_address(&to) {
            Some(Transfer {
                block_num,
                trx_hash: format!("0x{}", Hex(hash)),
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
        let from = format!("0x{}", Hex(&event.from));

        if is_zero_address(&from) {
            Some(Mint {
                block_num,
                trx_hash: format!("0x{}", Hex(hash)),
                log_index,
                to: format!("0x{}", Hex(&event.to)),
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
                trx_hash: format!("0x{}", Hex(hash)),
                log_index,
                from: format!("0x{}", Hex(&event.from)),
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
