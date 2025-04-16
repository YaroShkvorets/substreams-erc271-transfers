mod abi;
mod pb;

use pb::events::{Burn, Events, Mint, Mints, Transaction, Transfer};
use substreams::Hex;
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_ethereum::Event;

use abi::erc721::events::Transfer as ERC721TransferEvent;
use ethereum_types::U256;
use prost::bytes::Bytes;

const ZERO_ADDRESS: [u8; 20] = [0u8; 20];

/// Extracts events events from the logs
#[substreams::handlers::map]
fn map_events(blk: eth::Block) -> Result<Events, substreams::errors::Error> {
    let transfers: Vec<Transfer> = get_transfers(&blk).collect();
    let mints: Vec<Mint> = get_mints(&blk).collect();
    let burns: Vec<Burn> = get_burns(&blk).collect();

    // Collect all transaction hashes involved in any ERC721 event
    let mut event_tx_hashes = std::collections::HashSet::new();
    transfers.iter().for_each(|t| {
        event_tx_hashes.insert(t.trx_hash.clone());
    });
    mints.iter().for_each(|m| {
        event_tx_hashes.insert(m.trx_hash.clone());
    });
    burns.iter().for_each(|b| {
        event_tx_hashes.insert(b.trx_hash.clone());
    });

    let transactions = get_transactions(&blk, &event_tx_hashes);

    Ok(Events {
        transfers,
        mints,
        burns,
        transactions,
    })
}

/// Extracts mints with uri from the logs
/// We do this to avoid re-making RPC calls if we change something in map_events
#[substreams::handlers::map]
fn map_mints_with_uri(blk: eth::Block) -> Result<Mints, substreams::errors::Error> {
    let mints: Vec<Mint> = get_mints(&blk).collect();

    let mints_with_uri = mints
        .into_iter()
        .map(|m| {
            let token_id = m.token_id.parse::<substreams::scalar::BigInt>().unwrap();
            let uri = get_uri(m.contract.clone().into(), token_id);
            Mint { uri, ..m }
        })
        .collect();

    Ok(Mints {
        mints: mints_with_uri,
    })
}

fn get_uri(address: Vec<u8>, token_id: substreams::scalar::BigInt) -> Option<String> {
    abi::erc721::functions::TokenUri { token_id }.call(address)
}

fn get_transactions(
    blk: &eth::Block,
    event_tx_hashes: &std::collections::HashSet<Bytes>,
) -> Vec<Transaction> {
    let block_number = blk.number;
    let block_timestamp = blk
        .header
        .as_ref()
        .and_then(|h| h.timestamp.as_ref())
        .map(|t| t.seconds as u64)
        .unwrap_or(0);
    let block_hash = blk.hash.clone();

    blk.transaction_traces
        .iter()
        .filter(|trace| event_tx_hashes.contains(&Bytes::from(trace.hash.clone())))
        .map(|trace| Transaction {
            block_number,
            block_timestamp,
            block_hash: block_hash.clone().into(),
            tx_hash: trace.hash.clone().into(),
            nonce: trace.nonce,
            position: trace.index,
            from_address: trace.from.clone().into(),
            to_address: trace.to.clone().into(),
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
            v: trace.v.clone().into(),
            r: trace.r.clone().into(),
            s: trace.s.clone().into(),
        })
        .collect()
}

/// Helper that extracts ERC721 transfer events from a block
fn extract_erc721_events<'a, T, F>(
    blk: &'a eth::Block,
    process_event: F,
) -> impl Iterator<Item = T> + 'a
where
    F: Fn(u64, &[u8], u64, &[u8], ERC721TransferEvent) -> Option<T> + 'a + Copy,
{
    let block_num = blk.number;
    blk.receipts().flat_map(move |receipt| {
        let hash = &receipt.transaction.hash;
        let contract = &receipt.transaction.to;
        receipt.receipt.logs.iter().filter_map(move |log| {
            if let Some(event) = ERC721TransferEvent::match_and_decode(log) {
                process_event(block_num, hash, log.block_index as u64, contract, event)
            } else {
                None
            }
        })
    })
}

fn get_transfers<'a>(blk: &'a eth::Block) -> impl Iterator<Item = Transfer> + 'a {
    extract_erc721_events(blk, |block_num, hash, log_index, contract, event| {
        let from = &event.from;
        let to = &event.to;

        if !is_zero_address(from) && !is_zero_address(to) {
            Some(Transfer {
                block_num,
                trx_hash: hash.to_vec().into(),
                log_index,
                contract: contract.to_vec().into(),
                from: from.to_vec().into(),
                to: to.to_vec().into(),
                token_id: event.token_id.to_string(),
            })
        } else {
            None
        }
    })
}

fn get_mints<'a>(blk: &'a eth::Block) -> impl Iterator<Item = Mint> + 'a {
    extract_erc721_events(blk, |block_num, hash, log_index, contract, event| {
        let from = &event.from;
        let to = &event.to;

        if is_zero_address(from.as_ref() as &[u8]) {
            Some(Mint {
                block_num,
                trx_hash: hash.to_vec().into(),
                log_index,
                contract: contract.to_vec().into(),
                to: to.to_vec().into(),
                token_id: event.token_id.to_string(),
                uri: None,
            })
        } else {
            None
        }
    })
}

fn get_burns<'a>(blk: &'a eth::Block) -> impl Iterator<Item = Burn> + 'a {
    extract_erc721_events(blk, |block_num, hash, log_index, contract, event| {
        let to = &event.to;
        let from = &event.from;
        if is_zero_address(to.as_ref() as &[u8]) {
            Some(Burn {
                block_num,
                trx_hash: hash.to_vec().into(),
                log_index,
                contract: contract.to_vec().into(),
                from: from.to_vec().into(),
                token_id: event.token_id.to_string(),
            })
        } else {
            None
        }
    })
}

fn is_zero_address<T: AsRef<[u8]>>(addr: T) -> bool {
    addr.as_ref() == ZERO_ADDRESS
}
