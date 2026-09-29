from __future__ import annotations

from typing import Any
from xml.etree import ElementTree

PACKET_TYPE_MAP = {
    110: "FERMI_GBM_ALERT",
    115: "FERMI_GBM_FIN_POS",
    111: "FERMI_GBM_FLT_POS",
    112: "FERMI_GBM_GND_POS",
    119: "FERMI_GBM_POS_TEST",
    132: "FERMI_GBM_SUBTHRESH",
}


def extract_voevent_summary(xml_payload: str | bytes) -> dict[str, Any]:
    if isinstance(xml_payload, bytes):
        policy_text = xml_payload.decode("utf-8", errors="strict")
    else:
        policy_text = xml_payload
    upper = policy_text.upper()
    if "<!DOCTYPE" in upper or "<!ENTITY" in upper:
        raise ValueError("VOEvent XML DTD and entity declarations are forbidden")
    root = ElementTree.fromstring(policy_text)
    packet_type = _safe_int(_get_param(root, "Packet_Type"))
    sequence_num = _safe_int(_get_param(root, "Sequence_Num"))
    trig_id = _get_param(root, "TrigID")
    isotime = _get_text(root, "ISOTime")
    ra, dec, error_radius = _get_position(root)

    return {
        "packet_type": packet_type,
        "packet_type_name": PACKET_TYPE_MAP.get(packet_type),
        "sequence_num": sequence_num,
        "trig_id": trig_id,
        "isotime": isotime,
        "ra_deg": ra,
        "dec_deg": dec,
        "ra_dec_error_json": error_radius,
    }


def _tag_name(element: Any) -> str:
    tag = element.tag
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _get_param(element: Any, name: str) -> str | None:
    for child in element.iter():
        if _tag_name(child) == "Param" and child.attrib.get("name") == name:
            return child.attrib.get("value")
    return None


def _get_text(element: Any, name: str) -> str | None:
    for child in element.iter():
        if _tag_name(child) == name and child.text:
            return child.text.strip()
    return None


def _get_position(element: Any) -> tuple[float | None, float | None, float | None]:
    for child in element.iter():
        if _tag_name(child) != "Position2D":
            continue
        ra = dec = error_radius = None
        for sub in child.iter():
            tag = _tag_name(sub)
            if tag == "C1" and sub.text:
                ra = float(sub.text.strip())
            elif tag == "C2" and sub.text:
                dec = float(sub.text.strip())
            elif tag == "Error2Radius" and sub.text:
                error_radius = float(sub.text.strip())
        return ra, dec, error_radius
    return None, None, None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
