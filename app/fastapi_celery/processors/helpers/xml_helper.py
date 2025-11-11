from utils import log_helpers


# === Set up logging ===
logger = log_helpers.get_logger("xml_helper")


def build_processor_setting_xml(processor_args: list[dict[str, str]]) -> str | None:
    """
    Convert processorArgumentDtos into XML format like:
    <PROCESSORSETTINGXML>
      <param_name>param_value</param_name>
      ...
    </PROCESSORSETTINGXML>

    Args:
        processor_args (List[Dict[str, str]]): List of processorArgumentDtos containing 'processorArgumentName' and 'value'.

    Returns:
        str | None: XML string or None if list is empty or invalid.
    """
    if not processor_args:
        logger.warning("[build_processor_setting_xml] No processor arguments provided.")
        return None

    xml_lines = ["<PROCESSORSETTINGXML>"]
    for arg in processor_args:
        name = arg.get("name")
        value = arg.get("value", "")
        if name:
            # escape special XML chars
            safe_value = (
                str(value)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&apos;")
            )
            xml_lines.append(f"  <{name}>{safe_value}</{name}>")
        else:
            logger.warning(
                f"[build_processor_setting_xml] Missing processorArgumentName in {arg}"
            )

    xml_lines.append("</PROCESSORSETTINGXML>")
    xml_string = "\n".join(xml_lines)

    logger.info(f"[build_processor_setting_xml] Generated XML:\n{xml_string}")
    return xml_string


def get_data_output_for_rule_mapping(response_api):

    data_output = {
        "processorArgs": [],
        "processorConfigXml": "<PROCESSORSETTINGXML></PROCESSORSETTINGXML>",
        "fileLogLink": "",
    }
    try:
        processor_args = []
        if "processorArgumentDtos" in response_api:
            raw_args = response_api["processorArgumentDtos"] or []
            processor_args = [
                {"name": arg["processorArgumentName"], "value": arg["value"]}
                for arg in raw_args
            ]
        xml_data = build_processor_setting_xml(processor_args)

        data_output["processorArgs"] = processor_args
        data_output["processorConfigXml"] = xml_data

    except Exception as e:
        logger.error(
            f"[get_data_output_for_rule_mapping] An error occurred: {e}",
            exc_info=True,
        )
    finally:
        return data_output
