from __future__ import annotations

import math

LORAWAN_OVERHEAD_BYTES: int = 13  

LORA_SF: int = 7
LORA_BW_HZ: int = 125_000
LORA_CR: int = 1    
LORA_PREAMBLE_SYMBOLS: int = 8
LORA_DUTY_CYCLE: float = 0.01  

IPV4_HEADER_BYTES: int = 20  
TCP_HEADER_BYTES: int = 20 
UDP_HEADER_BYTES: int = 8 
ETHERNET_HEADER_BYTES: int = 14 
TCP_TRANSPORT_OVERHEAD: int = IPV4_HEADER_BYTES + TCP_HEADER_BYTES
UDP_TRANSPORT_OVERHEAD: int = IPV4_HEADER_BYTES + UDP_HEADER_BYTES  

MQTT_CONTROL_BYTE: int = 1
MQTT_PACKET_ID_BYTES: int = 2
MQTT_TOPIC_LEN_BYTES: int = 2
MQTT_ACK_BYTES: int = 4       

COAP_HEADER_BYTES: int = 4
COAP_TOKEN_BYTES: int = 4   
COAP_PAYLOAD_MARKER: int = 1 
COAP_URI_PATH_OPTION_EST: int = 15 
COAP_ACK_BYTES: int = COAP_HEADER_BYTES + COAP_TOKEN_BYTES  

AMQP_FRAME_ENVELOPE: int = 8   
AMQP_PUBLISH_METHOD_FIXED: int = 9
AMQP_CONTENT_HEADER_FIXED: int = 14
AMQP_PROPERTY_TABLE_EST: int = 40  
AMQP_DURABLE_PROPERTY: int = 1
AMQP_ACK_FRAME: int = 21      

ARRIVAL_RATES: dict[str, float] = {"low": 0.0028, "medium": 0.0102, "peak": 0.0182}

DWELL_SHORT_MU_S: float = 1500.0
DWELL_SHORT_CV: float = 0.9
DWELL_LONG_MU_S: float = 14400.0
DWELL_LONG_CV: float = 0.5
DWELL_SHORT_PROB: float = 0.90


def compute_lora_airtime_s(payload_bytes: int, sf: int = LORA_SF, bw: int = LORA_BW_HZ, cr: int = LORA_CR, preamble: int = LORA_PREAMBLE_SYMBOLS,
    crc: bool = True, explicit_hdr: bool = True) -> float:
    t_sym = (2 ** sf) / bw
    t_preamble = (preamble + 4.25) * t_sym
    de = 1 if sf >= 11 else 0
    h = 0 if explicit_hdr else 1
    crc_bits = 16 if crc else 0
    numerator = 8 * payload_bytes - 4 * sf + 28 + crc_bits - 20 * h
    n_payload = 8 + max(0, math.ceil(numerator / (4 * (sf - 2 * de))) * (cr + 4))
    t_payload = n_payload * t_sym
    return t_preamble + t_payload


def lora_duty_cycle_rate(max_payload_bytes: int = 51, duty_cycle: float = LORA_DUTY_CYCLE, **airtime_kw) -> float:
    airtime = compute_lora_airtime_s(max_payload_bytes, **airtime_kw)
    return duty_cycle / airtime if airtime > 0 else 1.0
