import calendar
import datetime
import math
import os
import random
import re
from pprint import pprint
from typing import Union, Iterable, Optional

from PIL import Image
from reportlab.lib.pagesizes import letter, landscape
from psycopg2.extras import DictCursor
import psycopg2
import logging
from reportlab.lib.colors import black, white
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph

SPREAD_WIDTH, SPREAD_HEIGHT = landscape(letter)

width, height = landscape(letter)
x_margin = 0.5 * inch
y_margin = 0.95 * inch
line_height = 0.22 * inch
col_width = ((width - 1 * x_margin) / 4) - .5 * inch  # 4 columns
col_x_positions = [
    x_margin + 0 * col_width,
    x_margin + 1.2 * col_width,
    x_margin + 2.6 * col_width,
    x_margin + 3.8 * col_width,
]

y_start = height - y_margin
used_ads = set()


def get_sorted_ads():
    ads = build_ad_candidates('advertising', 70, 'fall', 'monday')
    ads_1 = [a for a in ads if "_1_" in a]
    ads_2 = [a for a in ads if "_2_" in a]
    ads_full = [a for a in ads if "_full_" in a]

    random.shuffle(ads_1)
    random.shuffle(ads_2)
    random.shuffle(ads_full)

    return ads_1, ads_2, ads_full


def build_ad_candidates(ad_source: Union[str, Iterable[str]], year: str, season: str, day: str) -> list:
    """
    Return a list of candidate ad file paths that match either:
      <year>_<season>_<day>_...  OR  <year>_<season>_gen_...
    ad_source may be a directory path (str) or an iterable of file paths.
    """
    exts = {'.png', '.jpg', '.jpeg', '.tif', '.tiff', '.gif'}

    day_prefix = f"{year}_{season}_{day}_".lower()
    gen_prefix = f"{year}_{season}_gen_".lower()
    candidates = []

    if isinstance(ad_source, str):
        # treat as directory
        try:
            for fname in os.listdir(ad_source):
                lower = fname.lower()
                if not any(lower.endswith(ext) for ext in exts):
                    continue
                if lower.startswith(day_prefix) or lower.startswith(gen_prefix):
                    candidates.append(os.path.join(ad_source, fname))
        except FileNotFoundError:
            return []
    else:
        # treat as iterable of paths
        for p in ad_source:
            fname = os.path.basename(p).lower()
            if not any(fname.endswith(ext) for ext in exts):
                continue
            if fname.startswith(day_prefix) or fname.startswith(gen_prefix):
                candidates.append(p)

    return candidates


def select_ad_for_column(ads, space_left, col_width, used_ads, min_height=50):
    """
    Returns the path of the largest ad that fits in the available vertical space.
    Picks the best fit instead of the first match.
    """
    if space_left < min_height:
        return None  # not enough space

    best_fit = None
    best_height = 0

    for ad_path in ads:
        if ad_path in used_ads:
            continue

        scaled = get_scaled_image_size(ad_path, col_width, space_left)
        if not scaled:
            continue

        _, ad_h = scaled
        if space_left >= ad_h > best_height:
            best_fit = ad_path
            best_height = ad_h

    if best_fit:
        used_ads.add(best_fit)
    return best_fit


def get_scaled_image_size(image_path, col_width, available_height=None):
    """
    Scale image to exactly fit col_width, maintaining aspect ratio.
    Returns (width, height).
    If available_height is set and the scaled height > available_height,
    returns None to signal 'does not fit'.
    """
    with Image.open(image_path) as img:
        orig_width, orig_height = img.size

        # Always scale to column width
        scale_w = col_width
        scale_h = (orig_height / orig_width) * scale_w

        # If height doesn’t fit, bail out
        if available_height is not None and scale_h > available_height:
            return None

    return scale_w, scale_h


def chunk_list_into_logical_pages(data, page_height, col_width, top_margin=72, bottom_margin=0, canvas=None):
    # Step 1: Estimate how many shows fit on a page
    max_per_page = fit_shows_on_logical_page(
        data, page_height, col_width, top_margin, bottom_margin, canvas
    )

    # Step 2: Calculate how many sides we need
    number_of_sides = math.ceil(len(data) / max_per_page)

    # Step 3: Round up to nearest multiple of 4
    total_pages = math.ceil(number_of_sides / 4) * 4

    # Step 4: Evenly distribute shows across total_pages
    shows_per_page = len(data) // total_pages
    remainder = len(data) % total_pages

    result = []
    idx = 0
    for i in range(total_pages):
        count = shows_per_page + (1 if i < remainder else 0)
        result.append(data[idx:idx + count])
        idx += count

    return result


def fit_shows_on_logical_page(shows, page_height, col_width, top_margin=72, bottom_margin=0, canvas=None):
    y_remaining = page_height - top_margin
    col_index = 0
    shows_per_page = 0

    col_heights = [y_remaining, y_remaining]

    for show in shows:
        h = estimate_show_height(show, desc_max_width=col_width, canvas=canvas)

        if h > col_heights[col_index] - bottom_margin:
            col_index += 1
            if col_index >= 2:
                break
        col_heights[col_index] -= h
        shows_per_page += 1

    return shows_per_page


def truncate_paragraph(paragraph, n_sentences):
    # Split on punctuation followed by space
    sentences = re.split(r'(?<=[.!?])\s+', paragraph)
    truncated = ' '.join(sentences[:n_sentences])
    return truncated


def estimate_show_height(show, line_height=12, desc_line_height=8, desc_max_width: float = 250, canvas=None):
    """
    Estimate vertical space used by a show entry.
    - show['show']: title
    - show['description']: description
    - line_height: title line height
    - desc_line_height: base description line height
    - desc_max_width: max width of column in points
    - canvas: optional reportlab canvas for measuring text width
    """
    styles = getSampleStyleSheet()

    # Title style
    title_style = styles["Heading4"].clone("TitleStyle")
    title_style.fontName = "Helvetica-Bold"
    title_style.fontSize = 10
    title_style.leading = line_height

    # Description style
    desc_style = styles["Normal"].clone("DescStyle")
    desc_style.fontName = "Helvetica"
    desc_style.fontSize = 8
    desc_style.leading = desc_line_height

    # Measure wrapped title height
    title_para = Paragraph(show['show'].upper(), title_style)
    _, title_height = title_para.wrap(desc_max_width, 10000)

    # Measure wrapped description height
    desc_para = Paragraph(show['description'], desc_style)
    _, desc_height = desc_para.wrap(desc_max_width, 10000)

    # Add padding
    padding = 0

    total_height = title_height + desc_height + padding

    return total_height


def get_page_numbers(num_pages: int):
    if num_pages % 4 != 0:
        raise ValueError("Number of pages must be a multiple of 4")

    sheets = []
    left, right = 1, num_pages

    while left < right:
        # Front side of the sheet (outer)
        sheets.extend([right, left, left + 1, right - 1])

        left += 2
        right -= 2

    return sheets


def imposition_order(shows: list):
    """
    Returns a list of tuples (page_left, page_right) where each value is
    the actual data chunk from the shows list.
    The shows list length must be a multiple of 4.
    """
    num_pages = len(shows)
    if num_pages % 4 != 0:
        raise ValueError("Number of pages must be a multiple of 4")

    sheets = []
    left, right = 0, num_pages - 1  # use 0-based indexing for Python lists

    while left < right:
        # Front side (outer)
        sheets.append((shows[right], shows[left]))
        # Back side (inner)
        sheets.append((shows[left + 1], shows[right - 1]))

        left += 2
        right -= 2

    return sheets


def seconds_to_human(seconds: int) -> str:
    if seconds < 3600:  # less than an hour → just minutes
        minutes = round(seconds / 60)
        return f"{minutes} min."
    elif seconds <= 5400:  # up to 1.5 hours → still minutes
        minutes = round(seconds / 60)
        return f"{minutes} min."
    else:  # more than 1.5 hours → hours + minutes
        hours = seconds // 3600
        minutes = round((seconds % 3600) / 60)
        if minutes == 0:
            return f"{hours} hr." if hours == 1 else f"{hours} hrs."
        return f"{hours} hr., {minutes} min." if hours == 1 else f"{hours} hrs, {minutes} min."


def weekday_name_from_int(n: int):
    # 1 = Monday, 7 = Sunday
    if 0 <= int(n) <= 6:
        return calendar.day_name[n]
    else:
        raise ValueError("Weekday integer must be 0-6")


def convert_to_12hr(time_24):
    # Split hours and minutes
    hour, minute = map(int, time_24.split(":"))

    # Determine AM/PM
    if hour == 0:
        hour_12 = 12
    elif hour == 12:
        hour_12 = 12
    elif hour > 12:
        hour_12 = hour - 12
    else:
        hour_12 = hour

    return f"{hour_12}:{minute:02d}"


def get_db_connection() -> psycopg2.extensions.connection:
    """Establish and return a database connection."""
    try:
        conn = psycopg2.connect(
            database="time_traveler",
            user="postgres",
            password="m06Ar14u",
            host="192.168.1.201",
            port=5432
        )
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise


def get_current_schedule(dow):
    with get_db_connection() as db:
        cur = db.cursor(cursor_factory=DictCursor)
        cur.execute("""SELECT DISTINCT ON (st.channel_id, st.show_id, st.time_slot)
                            split_part(ch.channel_name, '-', 3) AS channel,
                            st.time_slot,
                            CEIL((e.end_point - e.start_point) / 1800) * 1800 AS show_duration,
                            sh.show_name AS show,
                            sh.show_genre,
                            e.episode_title AS title,
                            e.episode_description AS description,
                            bl.date_played
                        FROM schedule_template st
                        LEFT JOIN broadcast_log bl 
                        ON st.channel_id = bl.channel_id AND st.show_id = bl.show_id
                        LEFT JOIN episodes e ON bl.episode_id = e.episode_id
                        LEFT JOIN shows sh ON st.show_id = sh.show_id
                        LEFT JOIN channels ch ON ch.channel_id = bl.channel_id
                        WHERE %s = ANY(st.days_of_week)
                        ORDER BY st.channel_id,st.time_slot, st.show_id, bl.date_played DESC;""", (dow,))
    records = cur.fetchall()
    formatted_records = [{
        **dict(record),
        'time_slot': record['time_slot'].strftime("%H:%M")
    } for record in records]
    return formatted_records


def draw_clear_number_box(c, x, y, width, height, number, radius=2, stroke_width=1):
    """
    Draws a rounded rectangle number box in ReportLab.
    - If stroke_width > 0 and fill=0 → outlined box
    - If fill=1 → solid box
    """

    # Adjust for stroke width so the visual size matches filled boxes
    adj = stroke_width / 2.0
    c.setLineWidth(stroke_width)

    # Draw rounded rectangle outline only (no fill)
    c.setStrokeColor(black)
    c.roundRect(x + adj, y + adj, width - stroke_width, height - stroke_width,
                radius, stroke=1, fill=0)

    # Draw number in black, centered
    c.setFillColor(black)
    font_size = height * 0.8
    c.setFont("Helvetica-Bold", font_size)

    text = str(number)
    text_width = c.stringWidth(text, "Helvetica-Bold", font_size)
    text_x = x + (width - text_width) / 2
    text_y = y + (height - font_size) / 2 + font_size * 0.15

    c.drawString(text_x, text_y, text)


def draw_number_box(c, x, y, width, height, number, radius=2):
    """
    Draws a black rounded rectangle with a white number centered in a ReportLab canvas.

    Args:
        c: reportlab.pdfgen.canvas.Canvas object
        x, y: bottom-left corner of the box
        width, height: size of the box
        number: integer or string to display
        radius: corner radius
    """
    # Draw black rounded rectangle
    c.setFillColor(black)
    c.roundRect(x, y, width, height, radius, stroke=0, fill=1)

    # Draw number in white, centered
    c.setFillColor(white)

    # Choose a font size that fits the box height
    font_size = height * 0.8
    c.setFont("Helvetica-Bold", font_size)

    # Center the number
    text_width = c.stringWidth(str(number), "Helvetica-Bold", font_size)
    text_x = x + (width - text_width) / 2
    text_y = y + (height - font_size) / 2 + font_size * 0.15  # adjustment for vertical centering

    c.drawString(text_x, text_y, str(number))
    c.setFillColor(black)


def wrap_text_to_width(c, text, max_width, font_name="Helvetica", font_size=9):
    """
    Wrap text so that no line exceeds max_width in points.
    Returns a list of lines.
    """
    words = text.split()
    lines = []
    current_line = ""

    for word in words:
        test_line = f"{current_line} {word}".strip()
        if c.stringWidth(test_line, font_name, font_size) <= max_width:
            current_line = test_line
        else:
            if current_line:
                lines.append(current_line)
            current_line = word

    if current_line:
        lines.append(current_line)

    return lines

def get_needed_draw_space(shows: list, max_line_width: float) -> float:
    required_space = 0
    for show in shows:
        show_height = estimate_show_height(show, line_height=12, desc_line_height=8, desc_max_width=max_line_width, canvas=None)
        required_space += show_height

    return required_space


def get_layout_options(total_length, page_height, col_width, ads, used_ads, min_height=50):
    """
    Given the total length needed for shows, return viable layout options.

    total_length : total vertical space required by all shows
    page_height  : available vertical space per column
    col_width    : width of one column
    ads          : list of ads (file paths)
    used_ads     : set of already used ads
    min_height   : minimum ad height
    """
    layouts = []
    ads_1, ads_2, ads_full = get_sorted_ads()

    # Option 1: No ads, just shows
    if total_length <= 2 * page_height:
        layouts.append({"type": "no_ads", "capacity": 2 * page_height})

    # Option 2: Full column ad (outside column only, e.g. col 1 or 4)
    if total_length <= page_height:
        ad = select_ad_for_column(ads_full, page_height, col_width, used_ads, min_height)
        if ad:
            layouts.append({
                "type": "full_column_ad",
                "ad": ad,
                "capacity": page_height
            })

    # Option 3: Two-column ad at bottom
    # leaves reduced vertical space in both columns
    ad = select_ad_for_column(ads_2, page_height, col_width * 2, used_ads, min_height)
    if ad:
        _, ad_h = get_scaled_image_size(image_path=ad, col_width=col_width * 2, available_height=page_height)
        remaining_height = page_height - ad_h
        if total_length <= 2 * remaining_height:
            layouts.append({
                "type": "two_column_ad",
                "ad": ad,
                "capacity": 2 * remaining_height
            })

    # Option 4: Smaller stacked ads in a column
    # Try to fill one column with ads, leave the other for shows
    best_ad = select_ad_for_column(ads_1, page_height, col_width, used_ads, min_height)
    if best_ad:
        _, ad_h = get_scaled_image_size(image_path=best_ad, col_width=col_width, available_height=page_height)
        remaining_height = page_height - ad_h
        if total_length <= page_height + remaining_height:
            layouts.append({
                "type": "stacked_ads",
                "ad": best_ad,
                "capacity": page_height + remaining_height
            })

    return layouts


def pick_best_layout(layouts, total_length):
    """
    From a list of layout options, pick the one that wastes the least space.
    """
    if not layouts:
        return None

    # sort by wasted space (capacity - total_length)
    sorted_opts = sorted(
        layouts,
        key=lambda opt: (opt["capacity"] - total_length, opt["capacity"])
    )
    return sorted_opts[0]

def generate_4col_booklet(imposed_sheets, dow, col_w, filename="tv_schedule_4col_booklet.pdf"):
    c = canvas.Canvas(filename, pagesize=landscape(letter))
    c.setFont("Helvetica", 10)
    page_numbers = get_page_numbers(len(imposed_sheets) * 2)

    def draw_page_number(side, page_number):
        c.setFont("Helvetica", 8)
        y = 0.25 * inch

        if side == "left":
            # Outside = sheet left margin
            x = 0.25 * inch
            c.drawString(x, y, f"A-{page_number} TV GUIDE")

        elif side == "right":
            # Outside = sheet right margin
            x = SPREAD_WIDTH - 0.25 * inch
            c.drawRightString(x, y, f"A-{page_number} TV GUIDE")

    for sheet_index, sheet in enumerate(imposed_sheets):
        column_space = [y_start - y_margin] * 4
        for side_index, page_data in enumerate(sheet):

            if not page_data:
                continue

            # Left side (cols 0,1) vs right side (cols 2,3)
            start_col = 0 if side_index == 0 else 2
            end_col = start_col + 2

            col_index = start_col
            y = y_start
            time_slot = None

            # DOW
            c.setFont("Helvetica-Bold", 12)
            c.drawString(col_x_positions[start_col], SPREAD_HEIGHT - 0.4 * inch, weekday_name_from_int(dow))
            c.setFont("Helvetica", 8)
            c.drawString(col_x_positions[start_col], SPREAD_HEIGHT - 0.6 * inch, "EVENING")

            current_date = datetime.datetime.now().strftime("%B %d")+" 1970"  # e.g., September 17, 2025
            c.setFont("Helvetica", 9)
            c.drawRightString(col_x_positions[col_index] + (col_w*1.7), SPREAD_HEIGHT - 0.5 * inch, current_date)

            needed_space = get_needed_draw_space(page_data, col_width)
            ads = build_ad_candidates('advertising', 70, 'fall', 'monday')
            options = get_layout_options(needed_space, y_start, col_width, ads, used_ads, min_height=50)

            best_layout = pick_best_layout(options, needed_space)

            print("Best fit:", best_layout)

            for item in page_data:
                x = col_x_positions[col_index]

                # Wrap description
                column_left_margin = 0.1 * inch
                max_line_width = col_w - column_left_margin

                if item['show_duration'] > 1800:
                    item['description'] += f" ({seconds_to_human(item['show_duration'])})"

                description_lines = wrap_text_to_width(
                    c, item['description'], max_line_width, font_name="Helvetica", font_size=8
                )

                title_height = line_height / 1.5
                desc_line_height = 8 * 1.2
                desc_height = len(description_lines) * desc_line_height
                block_height = title_height + desc_height + (desc_line_height / 2)

                # Move to next column if needed
                if y - block_height < 20:
                #if y - block_height < needed_space / 2:
                    col_index += 1
                    y = y_start

                    if col_index >= end_col:  # both columns on this side are full
                        break  # stop writing more shows on this side

                    x = col_x_positions[col_index]

                # Draw boxes and text
                box_size = 0.14 * inch
                box_width = box_size + 0.01 * inch
                box_padding = 0.1 * inch
                font_size = 7
                text_height = font_size * 0.7
                box_y = y - (box_size - text_height) / 2

                if time_slot != item['time_slot']:
                    c.setFont("Helvetica", 8)
                    c.drawString(x - 0.3 * inch, y, convert_to_12hr(item['time_slot']))
                    time_slot = item['time_slot']

                draw_number_box(c, x, box_y, box_width, box_size, item['channel'])
                draw_clear_number_box(c, x + box_size + 0.05 * inch, box_y, box_width, box_size, item['channel'])

                text_x = x + box_size * 2 + box_padding
                max_text_width = col_w - (text_x - x)

                full_title = (
                    f"{item['show'].upper()}--{item['show_genre'].capitalize()}"
                    if item['show_duration'] > 1800 else
                    item['show'].upper()
                )

                # Wrap the title
                title_lines = wrap_text_to_width(
                    c, full_title, max_text_width, font_name="Helvetica-Bold", font_size=10
                )

                # Draw title
                c.setFont("Helvetica-Bold", font_size)
                for line in title_lines:
                    c.drawString(text_x, y, line)
                    y -= line_height / 1.5

                # Draw description
                c.setFont("Helvetica", 8)
                for line in description_lines:
                    c.drawString(x, y, line)
                    y -= desc_line_height
                y -= desc_line_height / 2

                column_space[col_index] = y - y_margin

                # Done with this logical page
            if side_index == 0:  # left page of sheet
                draw_page_number("left", page_numbers.pop(0))
            else:  # right page of sheet
                draw_page_number("right", page_numbers.pop(0))

        PADDING = 10  # vertical space in points between ads

        ads_1, ads_2, ads_full = get_sorted_ads()

        for col_index in range(4):
            remaining_space = column_space[col_index]
            y_ad_top = y_margin + remaining_space  # top of available space

            while remaining_space > 10 + PADDING:  # include padding in min threshold
                ad_path = select_ad_for_column([*ads_1, *ads_full], remaining_space, col_width, used_ads=used_ads, min_height=50)
                if not ad_path or not get_scaled_image_size(ad_path, col_width, remaining_space - PADDING):
                    break  # no ad fits

                ad_w, ad_h = get_scaled_image_size(ad_path, col_width, remaining_space - PADDING)
                if ad_h + PADDING > remaining_space:
                    break  # too tall to fit

                x = col_x_positions[col_index]
                y = y_ad_top - ad_h  # bottom-left corner for drawImage
                c.drawImage(ad_path, x - 4, y, width=ad_w, height=ad_h)

                # Update trackers (subtract ad height + padding)
                remaining_space -= (ad_h + PADDING)
                y_ad_top -= (ad_h + PADDING)
                column_space[col_index] = remaining_space

        if sheet_index < len(imposed_sheets) - 1:
            c.showPage()

    c.save()
    print(f"4-Column booklet PDF saved to {filename}")



if __name__ == "__main__":
    shows = get_current_schedule(0)
    [show.update({"description": truncate_paragraph(show["description"], 1)}) for show in shows]
    sorted_list = sorted(shows,key=lambda x: (x['time_slot'], int(x['channel'])))
    chunks = list(chunk_list_into_logical_pages(sorted_list, 912, col_width=col_width))
    generate_4col_booklet(imposition_order(chunks), 0, col_width)


