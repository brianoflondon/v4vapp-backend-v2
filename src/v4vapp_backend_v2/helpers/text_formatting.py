import re


def text_to_rtf(
    input_text: str,
    output_file: str = "output.rtf",
    max_lines_per_page: int = 60,
    font_name: str = "ArnoldMono",
    font_size: int = 8,
):
    """
    Converts plain text to RTF format with monospace font, landscape orientation,
    and optional page breaks after separator lines if the section exceeds max_lines_per_page.

    :param input_text: The input text as a string.
    :param output_file: The path to save the RTF file.
    :param max_lines_per_page: Maximum lines before considering a page break.
    :param font_name: The monospace font to use.
    :param font_size: Font size in half-points (RTF uses twips, so size*2).
    :return: None, writes to file.
    """
    # RTF header for landscape, margins, etc.
    rtf_header = r"{\rtf1\ansi\deff0 {\fonttbl {\f0 " + font_name + ";}}" + "\n"
    rtf_header += (
        r"\landscape \paperw15840 \paperh12240 \margl227 \margr227 \margt720 \margb720" + "\n"
    )
    rtf_header += r"\f0 \fs" + str(font_size * 2) + "\n"  # Font size in half-points

    # RTF footer
    rtf_footer = r"}"

    # Process the text
    lines = input_text.splitlines()
    current_section_lines = 0
    rtf_body = []

    for line in lines:
        # Escape special RTF characters
        escaped_line = line.replace("\\", r"\\").replace("{", r"\{").replace("}", r"\}")
        rtf_body.append(escaped_line + r"\par")  # Each line as a paragraph
        current_section_lines += 1

        # Check if this is a separator line (full of =, -, or similar repeated chars)
        if (
            re.fullmatch(r"([=~]+)", line.strip()) and len(line.strip()) > 40
        ):  # Arbitrary min length for separator
            # If the previous section (including this separator) is too long, insert page break before next section
            if current_section_lines > max_lines_per_page:
                rtf_body.append(r"\page")
                current_section_lines = 0  # Reset for next section

    # Combine everything
    rtf_content = rtf_header + "\n".join(rtf_body) + "\n" + rtf_footer

    # Write to file
    with open(output_file, "w", encoding="utf-8") as file:
        file.write(rtf_content)


# Example usage:
# Assuming the input text is provided as a multi-line string
if __name__ == "__main__":
    sample_text = """Paste your entire input text here as a multi-line string."""
    text_to_rtf(sample_text, "balance_sheet.rtf", max_lines_per_page=50)
