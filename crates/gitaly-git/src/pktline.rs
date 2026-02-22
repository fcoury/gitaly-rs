//! packet-line framing primitives.

use thiserror::Error;

pub const MAX_PACKET_LENGTH: usize = 65_520;
pub const HEADER_LENGTH: usize = 4;
pub const MAX_DATA_LENGTH: usize = MAX_PACKET_LENGTH - HEADER_LENGTH;

pub const FLUSH_FRAME: [u8; HEADER_LENGTH] = *b"0000";
pub const DELIM_FRAME: [u8; HEADER_LENGTH] = *b"0001";
pub const RESPONSE_END_FRAME: [u8; HEADER_LENGTH] = *b"0002";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameRef<'a> {
    Flush,
    Delim,
    ResponseEnd,
    Data(&'a [u8]),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EncodeError {
    #[error("payload too large: {payload_len} bytes (max {max_payload_len})")]
    PayloadTooLarge {
        payload_len: usize,
        max_payload_len: usize,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DecodeError {
    #[error("input too short for packet-line header: got {actual} bytes")]
    IncompleteHeader { actual: usize },
    #[error("invalid packet-line header `{header}`")]
    InvalidHeader { header: String },
    #[error("reserved control packet `{value:04x}` is not supported")]
    ReservedControlPacket { value: u16 },
    #[error("control packet must be exactly 4 bytes, got {actual}")]
    TrailingControlData { actual: usize },
    #[error("incomplete packet-line frame: expected {expected} bytes, got {actual}")]
    IncompleteFrame { expected: usize, actual: usize },
    #[error("packet-line frame has trailing bytes: expected {expected}, got {actual}")]
    TrailingBytes { expected: usize, actual: usize },
}

pub fn encode_data(payload: &[u8]) -> Result<Vec<u8>, EncodeError> {
    if payload.len() > MAX_DATA_LENGTH {
        return Err(EncodeError::PayloadTooLarge {
            payload_len: payload.len(),
            max_payload_len: MAX_DATA_LENGTH,
        });
    }

    let total_length = HEADER_LENGTH + payload.len();
    let mut frame = Vec::with_capacity(total_length);
    frame.extend_from_slice(&encode_length(total_length));
    frame.extend_from_slice(payload);

    Ok(frame)
}

#[must_use]
pub const fn encode_flush() -> [u8; HEADER_LENGTH] {
    FLUSH_FRAME
}

#[must_use]
pub const fn encode_delim() -> [u8; HEADER_LENGTH] {
    DELIM_FRAME
}

#[must_use]
pub const fn encode_response_end() -> [u8; HEADER_LENGTH] {
    RESPONSE_END_FRAME
}

pub fn decode_frame(frame: &[u8]) -> Result<FrameRef<'_>, DecodeError> {
    let Some((decoded, consumed)) = decode_next(frame)? else {
        return Err(DecodeError::IncompleteHeader { actual: 0 });
    };

    if consumed == frame.len() {
        return Ok(decoded);
    }

    if matches!(
        decoded,
        FrameRef::Flush | FrameRef::Delim | FrameRef::ResponseEnd
    ) {
        return Err(DecodeError::TrailingControlData {
            actual: frame.len(),
        });
    }

    Err(DecodeError::TrailingBytes {
        expected: consumed,
        actual: frame.len(),
    })
}

pub fn decode_next(input: &[u8]) -> Result<Option<(FrameRef<'_>, usize)>, DecodeError> {
    if input.is_empty() {
        return Ok(None);
    }

    if input.len() < HEADER_LENGTH {
        return Err(DecodeError::IncompleteHeader {
            actual: input.len(),
        });
    }

    let length = parse_length(&input[..HEADER_LENGTH])?;
    let length = usize::from(length);

    if length <= 3 {
        let frame = match length {
            0 => FrameRef::Flush,
            1 => FrameRef::Delim,
            2 => FrameRef::ResponseEnd,
            value => {
                return Err(DecodeError::ReservedControlPacket {
                    value: value as u16,
                })
            }
        };

        return Ok(Some((frame, HEADER_LENGTH)));
    }

    if input.len() < length {
        return Err(DecodeError::IncompleteFrame {
            expected: length,
            actual: input.len(),
        });
    }

    Ok(Some((
        FrameRef::Data(&input[HEADER_LENGTH..length]),
        length,
    )))
}

fn parse_length(header: &[u8]) -> Result<u16, DecodeError> {
    let mut value = 0_u16;

    for byte in header {
        value <<= 4;
        value |= u16::from(hex_nibble(*byte).ok_or_else(|| DecodeError::InvalidHeader {
            header: String::from_utf8_lossy(header).into_owned(),
        })?);
    }

    Ok(value)
}

const fn encode_length(value: usize) -> [u8; HEADER_LENGTH] {
    let value = value as u16;

    [
        hex_digit((value >> 12) as u8),
        hex_digit((value >> 8) as u8),
        hex_digit((value >> 4) as u8),
        hex_digit(value as u8),
    ]
}

const fn hex_digit(value: u8) -> u8 {
    match value & 0x0F {
        0..=9 => b'0' + (value & 0x0F),
        10..=15 => b'a' + ((value & 0x0F) - 10),
        _ => unreachable!(),
    }
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_data_round_trips_with_decode_frame() -> Result<(), Box<dyn std::error::Error>> {
        let encoded = encode_data(b"want refs/heads/main\n")?;
        let decoded = decode_frame(&encoded)?;

        assert!(matches!(decoded, FrameRef::Data(b"want refs/heads/main\n")));
        Ok(())
    }

    #[test]
    fn encode_data_allows_empty_payload() -> Result<(), Box<dyn std::error::Error>> {
        let encoded = encode_data(b"")?;

        assert_eq!(&encoded, b"0004");
        assert!(matches!(decode_frame(&encoded)?, FrameRef::Data(b"")));
        Ok(())
    }

    #[test]
    fn encode_data_rejects_payload_over_maximum() {
        let payload = vec![b'x'; MAX_DATA_LENGTH + 1];
        let err = encode_data(&payload).expect_err("must fail");

        assert!(matches!(
            err,
            EncodeError::PayloadTooLarge {
                payload_len,
                max_payload_len
            } if payload_len == MAX_DATA_LENGTH + 1 && max_payload_len == MAX_DATA_LENGTH
        ));
    }

    #[test]
    fn encodes_magic_control_frames() {
        assert_eq!(encode_flush(), *b"0000");
        assert_eq!(encode_delim(), *b"0001");
        assert_eq!(encode_response_end(), *b"0002");
    }

    #[test]
    fn decode_frame_handles_control_packets() -> Result<(), DecodeError> {
        assert_eq!(decode_frame(b"0000")?, FrameRef::Flush);
        assert_eq!(decode_frame(b"0001")?, FrameRef::Delim);
        assert_eq!(decode_frame(b"0002")?, FrameRef::ResponseEnd);
        Ok(())
    }

    #[test]
    fn decode_frame_rejects_reserved_control_packet() {
        let err = decode_frame(b"0003").expect_err("must fail");

        assert!(matches!(
            err,
            DecodeError::ReservedControlPacket { value: 3 }
        ));
    }

    #[test]
    fn decode_frame_rejects_control_packets_with_trailing_data() {
        let err = decode_frame(b"0000abcd").expect_err("must fail");

        assert!(matches!(
            err,
            DecodeError::TrailingControlData { actual: 8 }
        ));
    }

    #[test]
    fn decode_frame_rejects_incomplete_header() {
        let err = decode_frame(b"00").expect_err("must fail");

        assert!(matches!(err, DecodeError::IncompleteHeader { actual: 2 }));
    }

    #[test]
    fn decode_frame_rejects_non_hex_header() {
        let err = decode_frame(b"zzzz").expect_err("must fail");

        assert!(matches!(err, DecodeError::InvalidHeader { .. }));
    }

    #[test]
    fn decode_frame_rejects_incomplete_frame_data() {
        let err = decode_frame(b"0008ab").expect_err("must fail");

        assert!(matches!(
            err,
            DecodeError::IncompleteFrame {
                expected: 8,
                actual: 6
            }
        ));
    }

    #[test]
    fn decode_frame_rejects_trailing_bytes() {
        let err = decode_frame(b"0008abcd00").expect_err("must fail");

        assert!(matches!(
            err,
            DecodeError::TrailingBytes {
                expected: 8,
                actual: 10
            }
        ));
    }

    #[test]
    fn decode_next_reads_one_frame_and_returns_consumed_length() -> Result<(), DecodeError> {
        let stream = b"0007abc00006zz0000";
        let first = decode_next(stream)?;

        assert!(matches!(first, Some((FrameRef::Data(b"abc"), 7))));
        Ok(())
    }

    #[test]
    fn decode_next_returns_none_for_empty_input() -> Result<(), DecodeError> {
        assert_eq!(decode_next(b"")?, None);
        Ok(())
    }

    #[test]
    fn decode_next_rejects_incomplete_header() {
        let err = decode_next(b"00").expect_err("must fail");
        assert!(matches!(err, DecodeError::IncompleteHeader { actual: 2 }));
    }

    #[test]
    fn decode_next_rejects_incomplete_frame() {
        let err = decode_next(b"0008ab").expect_err("must fail");
        assert!(matches!(
            err,
            DecodeError::IncompleteFrame {
                expected: 8,
                actual: 6
            }
        ));
    }
}
