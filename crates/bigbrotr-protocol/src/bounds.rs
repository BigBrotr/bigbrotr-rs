use crate::event::EventError;

/// Per-input bounds; an eventual runtime grant must reserve these before receiving.
#[derive(Debug, Clone, Copy)]
pub struct EventLimits {
    pub wire_bytes: usize,
    pub event_bytes: usize,
    pub string_bytes: usize,
    pub nodes: usize,
    pub depth: usize,
    pub parse_bytes: usize,
}

impl Default for EventLimits {
    fn default() -> Self {
        Self {
            wire_bytes: 2 * 1024 * 1024,
            event_bytes: 256 * 1024,
            string_bytes: 2 * 1024 * 1024,
            nodes: 65_536,
            depth: 32,
            parse_bytes: 8 * 1024 * 1024,
        }
    }
}

impl EventLimits {
    pub fn validate(&self) -> Result<(), EventError> {
        if [
            self.wire_bytes,
            self.event_bytes,
            self.string_bytes,
            self.nodes,
            self.depth,
            self.parse_bytes,
        ]
        .contains(&0)
            || self.depth > 128
        {
            return Err(EventError::InvalidLimits);
        }
        Ok(())
    }
}

/// Allocation-free lexical counting; Serde remains the JSON grammar/UTF-8 parser.
/// Count keys, containers and scalar tokens, including unknown extension values.
/// Strings are capped by encoded bytes, a conservative bound on decoded UTF-8.
pub(crate) fn preflight(bytes: &[u8], limits: EventLimits) -> Result<(), EventError> {
    preflight_with_node_bytes(bytes, limits, 96)
}

/// A selected typed representation can require a larger per-node working charge.
/// Keep the same lexical checks rather than maintaining a second JSON scanner.
pub(crate) fn preflight_with_node_bytes(
    bytes: &[u8],
    limits: EventLimits,
    node_bytes: usize,
) -> Result<(), EventError> {
    limits.validate()?;
    if bytes.len() > limits.wire_bytes {
        return Err(EventError::WireLimit);
    }
    let mut nodes = 0usize;
    let mut depth = 0usize;
    let mut string_start = None;
    let mut escaped = false;
    let mut primitive = false;
    // Strings plus Serde's geometrically grown escape scratch; Vec headers and
    // minimum capacity/growth use the selected per-node charge on 64-bit targets.
    // This conservative working bound is separate from the caller's wire buffer
    // and from allocator/native/process overhead in the future runtime grant.
    let strings = bytes.len().checked_mul(3).ok_or(EventError::ArenaLimit)?;
    if strings > limits.parse_bytes {
        return Err(EventError::ArenaLimit);
    }
    for (position, &byte) in bytes.iter().enumerate() {
        if let Some(start) = string_start {
            if position - start > limits.string_bytes {
                return Err(EventError::StringLimit);
            }
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                string_start = None;
            }
            continue;
        }
        let node = match byte {
            b'"' => {
                string_start = Some(position + 1);
                primitive = false;
                true
            }
            b'[' | b'{' => {
                depth = depth.checked_add(1).ok_or(EventError::DepthLimit)?;
                if depth > limits.depth {
                    return Err(EventError::DepthLimit);
                }
                primitive = false;
                true
            }
            b']' | b'}' => {
                depth = depth.saturating_sub(1);
                primitive = false;
                false
            }
            b',' | b':' | b' ' | b'\t' | b'\n' | b'\r' => {
                primitive = false;
                false
            }
            _ => {
                let first = !primitive;
                primitive = true;
                first
            }
        };
        if node {
            nodes = nodes.checked_add(1).ok_or(EventError::NodeLimit)?;
            if nodes > limits.nodes {
                return Err(EventError::NodeLimit);
            }
            let estimate = nodes
                .checked_mul(node_bytes)
                .and_then(|n| n.checked_add(strings))
                .ok_or(EventError::ArenaLimit)?;
            if estimate > limits.parse_bytes {
                return Err(EventError::ArenaLimit);
            }
        }
    }
    Ok(())
}
