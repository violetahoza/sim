"""
Centralised protocol and link-layer constants.

Every number is either a spec-derived value (with a one-line citation) or a
modelling assumption (marked as such and citable).
"""
from __future__ import annotations

import math

# ── LoRaWAN (sensor → edge) ────────────────────────────────────────────────
# EU868 Class A uplink: MHDR(1) + FHDR(7) + FPort(1) + MIC(4) = 13 B
LORAWAN_OVERHEAD_BYTES: int = 13  # LoRaWAN 1.0.3 §4.3

# Default PHY parameters (EU868 DR5 – SF7/125 kHz)
LORA_SF: int = 7
LORA_BW_HZ: int = 125_000
LORA_CR: int = 1                 # coding rate 4/(4+CR)
LORA_PREAMBLE_SYMBOLS: int = 8
LORA_DUTY_CYCLE: float = 0.01   # 1 % duty cycle (ETSI EN 300.220)

# ── IP / TCP / UDP (backhaul edge → cloud) ─────────────────────────────────
IPV4_HEADER_BYTES: int = 20      # RFC 791
TCP_HEADER_BYTES: int = 20       # RFC 793 (no options)
UDP_HEADER_BYTES: int = 8        # RFC 768
ETHERNET_HEADER_BYTES: int = 14  # IEEE 802.3
TCP_TRANSPORT_OVERHEAD: int = IPV4_HEADER_BYTES + TCP_HEADER_BYTES   # 40 B
UDP_TRANSPORT_OVERHEAD: int = IPV4_HEADER_BYTES + UDP_HEADER_BYTES   # 28 B

# ── MQTT v3.1.1 (OASIS Standard, 29 October 2014) ─────────────────────────
MQTT_CONTROL_BYTE: int = 1
MQTT_PACKET_ID_BYTES: int = 2
MQTT_TOPIC_LEN_BYTES: int = 2
MQTT_ACK_BYTES: int = 4         # PUBACK / PUBREC / PUBREL / PUBCOMP

# ── CoAP (RFC 7252) ───────────────────────────────────────────────────────
COAP_HEADER_BYTES: int = 4
COAP_TOKEN_BYTES: int = 4       # typical 4-byte token
COAP_PAYLOAD_MARKER: int = 1    # 0xFF separator
COAP_URI_PATH_OPTION_EST: int = 15  # "parking/update" delta-encoded
COAP_ACK_BYTES: int = COAP_HEADER_BYTES + COAP_TOKEN_BYTES  # 8 B empty ACK

# ── AMQP 0-9-1 (RabbitMQ) ─────────────────────────────────────────────────
AMQP_FRAME_ENVELOPE: int = 8    # type(1) + channel(2) + size(4) + end(1)
AMQP_PUBLISH_METHOD_FIXED: int = 9
AMQP_CONTENT_HEADER_FIXED: int = 14
AMQP_PROPERTY_TABLE_EST: int = 40  # timestamp, message_id, content_type
AMQP_DURABLE_PROPERTY: int = 1
AMQP_ACK_FRAME: int = 21        # basic.ack method body + frame envelope

# ── Arrival rates (events/s/spot) ──────────────────────────────────────────
# Derived from urban parking turnover studies (Shoup, "The High Cost of
# Free Parking", 2005) scaled to per-spot binary-sensor event rate.
ARRIVAL_RATES: dict[str, float] = {
    "low": 0.0028,
    "medium": 0.0102,
    "peak": 0.0182,
}

# ── Dwell-time mixture ─────────────────────────────────────────────────────
# 90 % short-stay (µ = 1500 s ≈ 25 min, CV = 0.9)
# 10 % long-stay  (µ = 14400 s = 4 h,   CV = 0.5)
# Based on typical urban metered-parking distributions.
DWELL_SHORT_MU_S: float = 1500.0
DWELL_SHORT_CV: float = 0.9
DWELL_LONG_MU_S: float = 14400.0
DWELL_LONG_CV: float = 0.5
DWELL_SHORT_PROB: float = 0.90


def compute_lora_airtime_s(
    payload_bytes: int,
    sf: int = LORA_SF,
    bw: int = LORA_BW_HZ,
    cr: int = LORA_CR,
    preamble: int = LORA_PREAMBLE_SYMBOLS,
    crc: bool = True,
    explicit_hdr: bool = True,
) -> float:
    """LoRa airtime per Semtech AN1200.13 / SX1276 datasheet §4.1.1."""
    t_sym = (2 ** sf) / bw
    t_preamble = (preamble + 4.25) * t_sym
    de = 1 if sf >= 11 else 0
    h = 0 if explicit_hdr else 1
    crc_bits = 16 if crc else 0
    numerator = 8 * payload_bytes - 4 * sf + 28 + crc_bits - 20 * h
    n_payload = 8 + max(
        0, math.ceil(numerator / (4 * (sf - 2 * de))) * (cr + 4)
    )
    t_payload = n_payload * t_sym
    return t_preamble + t_payload


def lora_duty_cycle_rate(
    max_payload_bytes: int = 51,
    duty_cycle: float = LORA_DUTY_CYCLE,
    **airtime_kw,
) -> float:
    """Max messages/s respecting duty cycle, given max payload size."""
    airtime = compute_lora_airtime_s(max_payload_bytes, **airtime_kw)
    return duty_cycle / airtime if airtime > 0 else 1.0
