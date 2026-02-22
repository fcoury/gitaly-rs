//! side-band helpers for packfile protocol streams.

use thiserror::Error;

use crate::pktline::{self, FrameRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SidebandChannel {
    Data,
    Progress,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SidebandFrame<'a> {
    pub channel: SidebandChannel,
    pub payload: &'a [u8],
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SidebandError {
    #[error("sideband payload is empty")]
    EmptyPayload,
    #[error("unknown sideband channel `{channel}`")]
    UnknownChannel { channel: u8 },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SidebandPacketError {
    #[error("invalid packet-line frame: {source}")]
    PacketLine { source: pktline::DecodeError },
    #[error("packet-line control frame `{frame:?}` does not carry sideband payload")]
    UnexpectedControlFrame { frame: &'static str },
    #[error(transparent)]
    Sideband { source: SidebandError },
}

pub fn parse_sideband_payload(payload: &[u8]) -> Result<SidebandFrame<'_>, SidebandError> {
    let Some((&channel, payload)) = payload.split_first() else {
        return Err(SidebandError::EmptyPayload);
    };

    let channel = match channel {
        1 => SidebandChannel::Data,
        2 => SidebandChannel::Progress,
        3 => SidebandChannel::Error,
        channel => return Err(SidebandError::UnknownChannel { channel }),
    };

    Ok(SidebandFrame { channel, payload })
}

pub fn parse_sideband_packet(frame: &[u8]) -> Result<SidebandFrame<'_>, SidebandPacketError> {
    let pktline = pktline::decode_frame(frame)
        .map_err(|source| SidebandPacketError::PacketLine { source })?;

    match pktline {
        FrameRef::Data(payload) => parse_sideband_payload(payload)
            .map_err(|source| SidebandPacketError::Sideband { source }),
        FrameRef::Flush => Err(SidebandPacketError::UnexpectedControlFrame { frame: "flush" }),
        FrameRef::Delim => Err(SidebandPacketError::UnexpectedControlFrame { frame: "delim" }),
        FrameRef::ResponseEnd => Err(SidebandPacketError::UnexpectedControlFrame {
            frame: "response-end",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pktline;

    #[test]
    fn parse_sideband_payload_classifies_data_channel() -> Result<(), SidebandError> {
        let frame = parse_sideband_payload(b"\x01PACK...")?;

        assert_eq!(frame.channel, SidebandChannel::Data);
        assert_eq!(frame.payload, b"PACK...");
        Ok(())
    }

    #[test]
    fn parse_sideband_payload_classifies_progress_channel() -> Result<(), SidebandError> {
        let frame = parse_sideband_payload(b"\x02Counting objects: 10, done.")?;

        assert_eq!(frame.channel, SidebandChannel::Progress);
        assert_eq!(frame.payload, b"Counting objects: 10, done.");
        Ok(())
    }

    #[test]
    fn parse_sideband_payload_classifies_error_channel() -> Result<(), SidebandError> {
        let frame = parse_sideband_payload(b"\x03fatal: early EOF")?;

        assert_eq!(frame.channel, SidebandChannel::Error);
        assert_eq!(frame.payload, b"fatal: early EOF");
        Ok(())
    }

    #[test]
    fn parse_sideband_payload_rejects_empty_payload() {
        let err = parse_sideband_payload(b"").expect_err("must fail");
        assert_eq!(err, SidebandError::EmptyPayload);
    }

    #[test]
    fn parse_sideband_payload_rejects_unknown_channel() {
        let err = parse_sideband_payload(b"\x09oops").expect_err("must fail");
        assert!(matches!(err, SidebandError::UnknownChannel { channel: 9 }));
    }

    #[test]
    fn parse_sideband_packet_parses_pktline_and_classifies_channel(
    ) -> Result<(), SidebandPacketError> {
        let pkt = pktline::encode_data(b"\x02Compressing objects: 100%")
            .expect("pktline data encoding should work");
        let frame = parse_sideband_packet(&pkt)?;

        assert_eq!(frame.channel, SidebandChannel::Progress);
        assert_eq!(frame.payload, b"Compressing objects: 100%");
        Ok(())
    }

    #[test]
    fn parse_sideband_packet_rejects_pktline_control_packets() {
        let err = parse_sideband_packet(b"0000").expect_err("must fail");
        assert!(matches!(
            err,
            SidebandPacketError::UnexpectedControlFrame { frame: "flush" }
        ));
    }

    #[test]
    fn parse_sideband_packet_wraps_pktline_errors() {
        let err = parse_sideband_packet(b"zzzz").expect_err("must fail");
        assert!(matches!(err, SidebandPacketError::PacketLine { .. }));
    }

    #[test]
    fn parse_sideband_packet_rejects_data_frames_without_sideband_byte() {
        let pkt = pktline::encode_data(b"").expect("encode");
        let err = parse_sideband_packet(&pkt).expect_err("must fail");
        assert!(matches!(
            err,
            SidebandPacketError::Sideband {
                source: SidebandError::EmptyPayload
            }
        ));
    }
}
