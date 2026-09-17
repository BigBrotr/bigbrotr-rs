//! Deterministic signed research events. Keys are public fixtures, never identities.
use rayon::prelude::*;
use secp256k1::{Keypair, Secp256k1, SecretKey, SignOnly};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::io::{self, BufWriter, Read, Write};

fn event(
    i: u64,
    content_bytes: usize,
    keys: &[(Keypair, String)],
    secp: &Secp256k1<SignOnly>,
) -> String {
    let (key, pubkey) = &keys[(i % keys.len() as u64) as usize];
    let words = [
        "relay",
        "archive",
        "science",
        "privacy",
        "performance",
        "nostr",
        "community",
        "history",
        "研究",
        "città",
        "résilience",
        "🧪",
    ];
    let mut content = format!("Research fixture {i}: ");
    let mut block = 0_u64;
    while content.len() < content_bytes {
        let hash = Sha256::digest([i.to_le_bytes(), block.to_le_bytes()].concat());
        content.push_str(words[(hash[0] as usize) % words.len()]);
        content.push(' ');
        // Unique hash text prevents repeated padding from faking good compression.
        content.push_str(&hex::encode(hash));
        content.push_str(" \"quoted\"\n");
        block += 1;
    }
    let kind = if i % 20 == 0 { 30023 } else { 1 };
    let created_at = 1_700_000_000 + (i.wrapping_mul(7919) % (90 * 86400));
    let tags = json!([
        ["t", words[i as usize % words.len()]],
        ["p", keys[((i + 17) % keys.len() as u64) as usize].1],
        ["d", format!("fixture-{}", i % 1000)]
    ]);
    let canonical = serde_json::to_vec(&json!([0, pubkey, created_at, kind, tags, content]))
        .expect("fixture JSON is representable");
    let digest: [u8; 32] = Sha256::digest(&canonical).into();
    let sig = secp.sign_schnorr_no_aux_rand(&digest, key);
    json!({"ordinal": i, "id": hex::encode(digest), "pubkey": pubkey,
        "created_at": created_at, "kind": kind, "tags": tags,
        "content": content, "sig": sig.to_string()})
    .to_string()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let list_mode = args.len() == 4 && args[1] == "list";
    if !list_mode && args.len() != 5 {
        return Err("usage: fixture START COUNT CONTENT_BYTES THREADS; or fixture list CONTENT_BYTES THREADS (JSON ordinal array on stdin)".into());
    }
    let (start, count, size, threads) = if list_mode {
        (
            0_u64,
            0_u64,
            args[2].parse::<usize>()?,
            args[3].parse::<usize>()?,
        )
    } else {
        (
            args[1].parse()?,
            args[2].parse()?,
            args[3].parse()?,
            args[4].parse()?,
        )
    };
    if size > 131072 || threads == 0 || threads > 32 {
        return Err("research envelope: 0..128KiB content, 1..32 threads".into());
    }
    let end = start.checked_add(count).ok_or("ordinal overflow")?;
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()?;
    let secp = Secp256k1::signing_only();
    let keys: Vec<_> = (1_u64..=4096)
        .map(|i| {
            let mut secret = [0_u8; 32];
            secret[24..].copy_from_slice(&i.to_be_bytes());
            let key = Keypair::from_secret_key(&secp, &SecretKey::from_byte_array(secret).unwrap());
            let public = key.x_only_public_key().0.to_string();
            (key, public)
        })
        .collect();
    let mut out = BufWriter::with_capacity(1024 * 1024, io::stdout().lock());
    if list_mode {
        let mut input = String::new();
        io::stdin().take(65537).read_to_string(&mut input)?;
        if input.len() > 65536 {
            return Err("ordinal input exceeds 64KiB".into());
        }
        let ordinals: Vec<u64> = serde_json::from_str(&input)?;
        if ordinals.len() > 4096 {
            return Err("at most 4096 sample ordinals".into());
        }
        for i in ordinals {
            writeln!(out, "{}", event(i, size, &keys, &secp))?;
        }
        out.flush()?;
        return Ok(());
    }
    let mut offset = start;
    while offset < end {
        let stop = end.min(offset.saturating_add(8192));
        let rows: Vec<String> = (offset..stop)
            .into_par_iter()
            .map(|i| event(i, size, &keys, &secp))
            .collect();
        for row in rows {
            writeln!(out, "{row}")?;
        }
        offset = stop;
    }
    out.flush()?;
    Ok(())
}
