use super::{DnsError, direct_hostname};
use hickory_proto::{
    op::{Message, ResponseCode},
    rr::{DNSClass, Name, RData, RecordType},
    serialize::binary::{BinDecodable, BinDecoder},
};
use std::net::IpAddr;

const MAX_ALIASES: usize = 8;

pub(super) fn check_name(name: &Name) -> Result<(), DnsError> {
    let ascii = name.to_ascii().to_ascii_lowercase();
    let host = ascii.strip_suffix('.').unwrap_or(&ascii);
    direct_hostname(host).map_err(|_| DnsError::NamePolicy)?;
    // These names require local/special-use handling, not a DNS observation.
    // Explicit loopback IP fixtures still use the ordinary private IP policy.
    if [
        "localhost",
        "invalid",
        "local",
        "127.in-addr.arpa",
        "1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa",
    ]
    .iter()
    .any(|zone| host == *zone || host.strip_suffix(zone).is_some_and(|p| p.ends_with('.')))
    {
        return Err(DnsError::NamePolicy);
    }
    Ok(())
}

pub(super) fn decode(frame: &[u8], request: &Message) -> Result<Message, DnsError> {
    // Header counts must be checked before the upstream decoder reserves its
    // question vector. The entire frame is already bounded by the TCP reader.
    let word = |offset| u16::from_be_bytes([frame[offset], frame[offset + 1]]);
    let flags = word(2);
    if word(4) != 1 || u32::from(word(6)) + u32::from(word(8)) + u32::from(word(10)) > 64 {
        return Err(DnsError::ResponseLimit);
    }
    if word(0) != request.id || flags & 0x8000 == 0 || flags & 0x7800 != 0 || flags & 0x0240 != 0
    // truncation or reserved Z bit
    {
        return Err(DnsError::InvalidResponse);
    }
    let mut decoder = BinDecoder::new(frame);
    let message = Message::read(&mut decoder).map_err(|_| DnsError::InvalidResponse)?;
    if !decoder.is_empty() || message.queries != request.queries {
        return Err(DnsError::InvalidResponse);
    }

    for record in message.all_sections() {
        if let RData::CNAME(cname) = &record.data {
            check_name(&cname.0)?;
        }
    }

    Ok(message)
}

pub(super) enum Answer {
    Addresses(Vec<IpAddr>),
    Alias,
    NoRecords,
}

/// One bounded, case-insensitive chain across all response packets of a family.
/// Eight edges mean at most nine queries; packet layout cannot reset this limit.
pub(super) struct Chain {
    names: Vec<Name>,
}

impl Chain {
    pub(super) fn new(name: Name) -> Self {
        Self { names: vec![name] }
    }

    pub(super) fn terminal(&self) -> &Name {
        self.names.last().expect("chain starts with the question")
    }

    pub(super) fn accept(
        &mut self,
        message: &Message,
        kind: RecordType,
    ) -> Result<Answer, DnsError> {
        let initial_length = self.names.len();
        loop {
            let mut next = None;
            let mut has_address = false;
            for record in &message.answers {
                if record.dns_class != DNSClass::IN || &record.name != self.terminal() {
                    continue;
                }
                match &record.data {
                    RData::CNAME(cname) => {
                        if next.is_some_and(|name| name != &cname.0) {
                            return Err(DnsError::InvalidResponse);
                        }
                        next = Some(&cname.0);
                    }
                    RData::A(_) | RData::AAAA(_) => has_address = true,
                    _ => {}
                }
            }
            let Some(next) = next else { break };
            if has_address || self.names.contains(next) {
                return Err(DnsError::InvalidResponse);
            }
            if self.names.len() > MAX_ALIASES {
                return Err(DnsError::ResponseLimit);
            }
            self.names.push(next.clone());
        }
        // A negative/error reply cannot hide malformed alias data. Validate the
        // selected chain first, then stop without following a negative referral.
        match message.response_code {
            ResponseCode::NoError => {}
            ResponseCode::NXDomain => {
                if message.answers.iter().any(|record| {
                    record.dns_class == DNSClass::IN
                        && matches!(record.data, RData::A(_) | RData::AAAA(_))
                }) {
                    return Err(DnsError::InvalidResponse);
                }
                return Ok(Answer::NoRecords);
            }
            ResponseCode::ServFail | ResponseCode::Refused | ResponseCode::NotImp => {
                return Err(DnsError::Unavailable);
            }
            _ => return Err(DnsError::InvalidResponse),
        }
        // Authority/additional data, unrelated names and unsupported record types
        // never become dial targets or redirect the next query. RR order is free.
        let addresses: Vec<_> = message
            .answers
            .iter()
            .filter_map(|record| {
                if record.dns_class != DNSClass::IN || &record.name != self.terminal() {
                    return None;
                }
                match (&record.data, kind) {
                    (RData::A(address), RecordType::A) => Some(IpAddr::V4(address.0)),
                    (RData::AAAA(address), RecordType::AAAA) => Some(IpAddr::V6(address.0)),
                    _ => None,
                }
            })
            .collect();
        if !addresses.is_empty() {
            Ok(Answer::Addresses(addresses))
        } else if self.names.len() != initial_length {
            Ok(Answer::Alias)
        } else {
            Ok(Answer::NoRecords)
        }
    }
}
