def text_to_rtf(
    input_text: str,
    output_file: str = "output.rtf",
    max_lines_per_page: int = 60,
    font_name: str = "ArnoldMono",
    font_size: int = 8,
):
    """
    Converts plain text to RTF format with monospace font, landscape orientation,
    and keeps sections between separator lines on the same page unless they exceed 45 lines.

    :param input_text: The input text as a string.
    :param output_file: The path to save the RTF file.
    :param max_lines_per_page: Maximum lines before considering a page break.
    :param font_name: The monospace font to use.
    :param font_size: Font size in half-points (RTF uses twips, so size*2).
    :return: None, writes to file.
    """
    # 1.27cm in twips (1cm ≈ 567 twips)
    header_footer_height = 720

    # RTF header with improved formatting for Pages compatibility
    rtf_header = r"{\rtf1\ansi\ansicpg1252\cocoartf2709" + "\n"
    rtf_header += (
        r"\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 " + font_name + ";}" + "\n"
    )
    rtf_header += r"{\colortbl;\red255\green255\blue255;\red0\green0\blue0;}" + "\n"
    rtf_header += r"{\*\expandedcolortbl;;\cssrgb\c0\c0\c0;}" + "\n"

    # Important: Set viewkind to 1 (page layout view) which helps with orientation
    rtf_header += r"\viewkind1\viewscale100" + "\n"

    # Standard A4 dimensions (11906 x 16838 twips) - proper landscape for Pages
    rtf_header += r"\paperw11906\paperh16838" + "\n"
    rtf_header += (
        r"\margl283\margr283\margb"
        + str(header_footer_height)
        + r"\margt"
        + str(header_footer_height)
        + "\n"
    )
    rtf_header += (
        r"\headery" + str(header_footer_height) + r"\footery" + str(header_footer_height) + "\n"
    )

    # This is the key fix: landscape section command that Pages recognizes
    rtf_header += (
        r"\sectd\lndscpsxn\pgnx"
        + str(header_footer_height)
        + r"\pgny"
        + str(header_footer_height)
        + "\n"
    )

    # Additional landscape indicator
    rtf_header += r"\landscape" + "\n"

    rtf_header += r"\deftab720" + "\n"
    rtf_header += r"\pard\pardeftab720\partightenfactor0" + "\n"

    # Font settings
    rtf_header += r"\f0\fs" + str(font_size * 2) + r" \cf2 \up0 \nosupersub \ulnone" + "\n"

    # Footer with page numbers
    footer = (
        r"{\footer \pard\pardeftab720\qc\partightenfactor0"  # Centered alignment
        r"\f0\fs16 Page "
        r"{\field{\*\fldinst PAGE}}"  # Current page number
        r" of "
        r"{\field{\*\fldinst NUMPAGES}}"  # Total pages
        r"\par}"
    )

    # RTF footer
    rtf_footer = r"}"

    # Process the text - improved section handling
    lines = input_text.splitlines()
    rtf_body = []

    # First, identify all separator lines and their positions
    separator_indices = []
    for i, line in enumerate(lines):
        if is_separator_line(line):
            separator_indices.append(i)

    # Add the end of the document as a "virtual" separator
    separator_indices.append(len(lines))

    # Process each section
    current_page_lines = 0
    max_section_lines = 45  # Maximum lines in a section before forcing a break

    # Start with section 0 (before the first separator)
    for i in range(len(separator_indices)):
        start_idx = 0 if i == 0 else separator_indices[i - 1] + 1
        end_idx = separator_indices[i]

        # Calculate section length (including the separator)
        section_length = end_idx - start_idx + 1

        # Check if we need to insert a page break before this section
        if current_page_lines + section_length > max_lines_per_page and i > 0:
            rtf_body.append(r"\page")
            current_page_lines = 0

        # Add all lines in this section
        for j in range(start_idx, end_idx + 1):
            if j < len(lines):  # Safety check
                # Escape special RTF characters
                escaped_line = (
                    lines[j].replace("\\", r"\\").replace("{", r"\{").replace("}", r"\}")
                )
                rtf_body.append(escaped_line + r"\par")
                current_page_lines += 1

        # If this section was too long, force a page break after it
        if section_length > max_section_lines:
            rtf_body.append(r"\page")
            current_page_lines = 0

    # Combine everything - INCLUDE THE FOOTER this time!
    rtf_content = rtf_header + footer + "\n".join(rtf_body) + "\n" + rtf_footer

    # Write to file
    with open(output_file, "w", encoding="utf-8") as file:
        file.write(rtf_content)


def is_separator_line(line: str) -> bool:
    """
    Determines if a line is a separator line based on its content.

    :param line: The line to check.
    :return: True if it's a separator line, False otherwise.
    """
    stripped_line = line.strip()
    return len(stripped_line) > 0 and all(c in "=~-" for c in stripped_line)


# Example usage:
# Assuming the input text is provided as a multi-line string
if __name__ == "__main__":
    sample_text = """Paste your entire input text here as a multi-line string."""
    text_to_rtf(sample_text, "balance_sheet.rtf", max_lines_per_page=50)
