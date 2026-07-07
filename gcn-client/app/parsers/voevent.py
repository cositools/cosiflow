from __future__ import annotations

from typing import Any


PACKET_TYPE_MAP = {
    110: "FERMI_GBM_ALERT",
    115: "FERMI_GBM_FIN_POS",
    111: "FERMI_GBM_FLT_POS",
    112: "FERMI_GBM_GND_POS",
    119: "FERMI_GBM_POS_TEST",
    132: "FERMI_GBM_SUBTHRESH",
}


def extract_voevent_summary(xml_text: str) -> dict[str, Any]:
    import voeventparse as vp

    v = vp.fromstring(xml_text)
    packet_type = _safe_int(_get_param(v.What, "Packet_Type"))
    sequence_num = _safe_int(_get_param(v.What, "Sequence_Num"))
    trig_id = _get_param(v.What, "TrigID")
    isotime = _get_isotime(v)
    ra, dec, error_radius = _get_position(v)

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
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _get_param(element: Any, name: str) -> str | None:
    for child in element.iter():
        if _tag_name(child) == "Param" and child.attrib.get("name") == name:
            return child.attrib.get("value")
    return None


def _get_isotime(v: Any) -> str | None:
    for child in v.iter():
        if _tag_name(child) == "ISOTime" and child.text:
            return child.text.strip()
    return None


def _get_position(v: Any) -> tuple[float | None, float | None, float | None]:
    for child in v.iter():
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
