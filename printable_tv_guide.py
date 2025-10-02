"""
tv_schedule_booklet.py

Generate a 4-column printable TV schedule booklet in PDF format, using
ReportLab for layout and PostgreSQL as the schedule data source.

Features:
- Retrieves show schedule from a PostgreSQL database
- Randomizes and fits shows into 4-column pages
- Dynamically inserts advertisements (1-column, 2-column, or full-column)
- Generates booklet imposition order for duplex printing
"""

import calendar
import datetime
import io
import logging
import os
import random
from operator import itemgetter

import nltk
from pprint import pprint
from typing import Union, Iterable, Any

from PIL import Image, ImageDraw, ImageFont
from psycopg2.extras import DictCursor
import psycopg2
from reportlab.lib.colors import black, white
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from nltk.tokenize import sent_tokenize
from sympy import ceiling

# nltk.download("punkt")
# nltk.download('punkt_tab')

# --- Global layout constants ---
width, height = (711, 531)
x_margin: float = 0.5 * inch
y_margin: float = 0.85 * inch
y_bottom: float = 0.75 * inch
line_height: float = 0.22 * inch

# Column setup (4 columns evenly spaced)
col_width: float = ((width - 1 * x_margin) / 4) - 0.47 * inch
col_x_positions: list[float] = [
    x_margin + 0 * col_width,
    x_margin + 1.2 * col_width,
    x_margin + 2.7 * col_width,
    x_margin + 3.9 * col_width,
]

# Other constants
y_start: float = height - y_margin
used_ads: set[str] = set()
real_channels = [2, 3, 4, 5, 7, 9, 11, 13]
non_channels: list[int] = [22, 28, 34, 40, 52, 24, 36, 42]
clear_channels: list[int] = [3, 24, 36, 42]
advert_keys = ['ad_image', 'ad_text', 'network_img', 'channel_img', 'font', 'font_size', 'time_slot', 'show_id']

column_left_margin = 0.1 * inch
max_line_width = col_width - column_left_margin

box_size = 0.12 * inch
box_width = box_size + 0.04 * inch
box_padding = 0.06 * inch

SHOW_IMAGES = "advertising/shows/"
SHOW_LOGOS = "advertising/logos/"
FONT_DIR = "advertising/fonts/"
AD_IMAGES = "advertising/images/"
THUMB_HEIGHT = 16


def build_ad_candidates(ad_source: Union[str, Iterable[str]], year: str, season: str, day: str) -> list[str]:
    """
    Collect candidate advertisement file paths that match:
      <year>_<season>_<day>_...  OR  <year>_<season>_gen_...

    Args:
        ad_source: Directory path (str) or iterable of file paths
        year: Year string (e.g., "1970")
        season: Season string (e.g., "fall")
        day: Day string (e.g., "monday")

    Returns:
        List of valid advertisement file paths
    """

    exts = {'.png', '.jpg', '.jpeg', '.tif', '.tiff', '.gif'}

    day_prefix: str = f"{year}_{season}_{day}_".lower()
    gen_prefix: str = f"{year}_{season}_gen_".lower()
    candidates: list[str] = []

    for fname in os.listdir(ad_source):
        lower = fname.lower()
        if not any(lower.endswith(ext) for ext in exts):
            continue

        candidates.append(os.path.join(ad_source, fname))
    return candidates

    """

    if isinstance(ad_source, str):
        # Treat as directory
        try:
            for fname in os.listdir(ad_source):
                lower = fname.lower()
                if not any(lower.endswith(ext) for ext in exts):
                    continue
                if lower.startswith(day_prefix) or lower.startswith(gen_prefix):
                    #candidates.append(os.path.join(ad_source, fname))
        except FileNotFoundError:
            logging.warning("Ad source directory not found: %s", ad_source)
            return []
        except Exception as e:
            logging.error("Error scanning ad directory %s: %s", ad_source, e)
            return []
    else:
        # Treat as iterable of paths
        for p in ad_source:
            fname = os.path.basename(p).lower()
            if not any(fname.endswith(ext) for ext in exts):
                continue
            if fname.startswith(day_prefix) or fname.startswith(gen_prefix):
                candidates.append(p)

    return candidates
    """


def get_sorted_ads():
    ads = build_ad_candidates(AD_IMAGES, 70, 'fall', 'monday')
    ads_1 = [a for a in ads if "_1_" in a]
    ads_2 = [a for a in ads if "_2_" in a]
    ads_full = [a for a in ads if "_full_" in a]

    random.shuffle(ads_1)
    random.shuffle(ads_2)
    random.shuffle(ads_full)

    return ads_1, ads_2, ads_full


def select_ad_for_column(ads, space_left, col_w, used_ads, is_bottom_ad, min_height: float = 50):
    """
    Returns the path of the largest ad that fits in the available vertical space.

    Rules:
    - If is_bottom_ad is True → only ads with "_B_" in filename are considered.
    - If is_bottom_ad is False → exclude any ads with "_B_" in filename.
    - Picks the best fit (largest that fits), not the first match.
    """

    if space_left < min_height:
        return None  # not enough space

    best_fit = None
    best_height = 0

    for ad_path in ads:
        # skip already used
        if ad_path in used_ads:
            continue

        # enforce "_B_" rules
        # if is_bottom_ad and "_B_" not in ad_path:
        # continue
        if not is_bottom_ad and "_B_" in ad_path:
            continue

        scaled = get_scaled_image_size(ad_path, col_w, space_left)
        if not scaled:
            continue

        _, ad_h = scaled
        if space_left >= ad_h > best_height:
            best_fit = ad_path
            best_height = ad_h

    if best_fit or not is_bottom_ad or col_w > col_width:
        used_ads.add(best_fit)
    return best_fit


def get_scaled_image_size(image_path: str, col_width: float, is_full_column: bool = False, available_height=None):
    """
    Scale image to fit within col_width and available_height (if given),
    maintaining aspect ratio.
    - Prefer scaling to col_width.
    - If that makes the image too tall for available_height,
      scale to available_height instead.
    Returns (width, height).
    """
    with Image.open(image_path) as img:
        orig_width, orig_height = img.size

        # scale based on width
        scale_w = col_width
        scale_h = (orig_height / orig_width) * scale_w

        if available_height is not None and scale_h > available_height and is_full_column:
            # Instead, scale based on available height
            scale_h = available_height
            scale_w = (orig_width / orig_height) * scale_h

        return scale_w, scale_h


def chunk_shows_random(shows, min_size, max_size):
    """
    Chunk shows into variable random sizes, but ensure the total number
    of chunks is a multiple of 4 (no padding).

    shows     : list of shows
    min_size  : minimum number of shows per chunk
    max_size  : maximum number of shows per chunk
    """
    list_length = len(shows)
    pages_needed = ceiling(int(list_length) / int(max_size))
    pages_needed = pages_needed if pages_needed > 3 else 4
    logical_page_count = pages_needed if pages_needed % 4 == 0 else ((pages_needed + 3) // 4) * 4

    sizes = chunk_sizes(logical_page_count, list_length)
    chunks = chunk_by_sizes(shows, sizes)
    return chunks


def chunk_sizes(n: int, list_length: int) -> list[int]:
    """
    Split list_length items into n chunks, distributing remainder evenly.

    Example:
        chunk_sizes(3, 10) -> [4, 3, 3]
        chunk_sizes(4, 10) -> [3, 3, 2, 2]
    """
    base = list_length // n  # minimum size of each chunk
    remainder = list_length % n  # how many need +1

    sizes = [base + 1 if i < remainder else base for i in range(n)]
    return sizes


def chunk_by_sizes(data, sizes):
    """
    Split a list into chunks according to a list of sizes.

    Example:
        data = list(range(45))
        sizes = [6,6,6,6,6,6,5,5]
        -> returns a list of sublists with those sizes
    """
    chunks = []
    start = 0
    for size in sizes:
        end = start + size
        chunks.append(data[start:end])
        start = end
    return chunks


def fit_shows_on_logical_page(shows: list, page_height: float, top_margin: float = 72, bottom_margin: float = 50):
    y_remaining = page_height - top_margin
    col_index = 0
    shows_per_page = 0
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(width, height))

    col_heights = [y_remaining, y_remaining]

    total_h = sum(h for h, _, _ in (get_wrapped_title_des(c, show, max_line_width) for show in shows))
    columns_needed = ceiling(total_h / (col_heights[col_index] - bottom_margin))
    shows_per_page = (len(shows) // columns_needed) * 2
    return shows_per_page


def truncate_paragraph(paragraph, n_sentences):
    sentences = sent_tokenize(paragraph)
    return sentences[0] if sentences else ""


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


def convert_to_12hr(time_24: str, ampm: bool = False) -> str:
    """
    Convert a 24-hour time string (HH:MM) into 12-hour format.

    Args:
        time_24 (str): Time string in 24-hour format (e.g., "23:15").
        ampm (bool): If True, return "H AM/PM" (e.g., "11 PM").
                     If False, return "H:MM" (e.g., "11:15").

    Returns:
        str: Converted time string.
    """
    # Split hours and minutes
    hour, minute = map(int, time_24.split(":"))

    # Determine AM/PM
    if hour == 0:
        hour_12 = 12
        suffix = "AM"
    elif hour == 12:
        hour_12 = 12
        suffix = "PM"
    elif hour > 12:
        hour_12 = hour - 12
        suffix = "PM"
    else:
        hour_12 = hour
        suffix = "AM"

    if ampm:
        return f"{hour_12}{suffix}"
    else:
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


def get_current_schedule(dow, year: int = 1974):
    with get_db_connection() as db:
        cur = db.cursor(cursor_factory=DictCursor)
        cur.execute("""-- Normal shows
            (
            SELECT DISTINCT ON (st.channel_id, st.show_id, st.time_slot)
                   split_part(ch.channel_name, '-', 3) AS channel,
                   st.time_slot,
                   CEIL((e.end_point - e.start_point) / 1800) * 1800 AS show_duration,
                   sh.show_name AS show,
                   sh.show_genre,
                   e.episode_title AS title,
                   COALESCE(e.episode_description, sh.show_description) AS description,
                   bl.date_played,
                   sa.ad_image,
                   sa.ad_text,
                   sa.network_img,
                   sa.channel_img,
                   sa.font,
                   sa.font_size,
                   sh.show_id,
                   e.is_bw,
                   e.episode_co_stars as actors
            FROM schedule_template st
            LEFT JOIN broadcast_log bl 
                   ON st.channel_id = bl.channel_id AND st.show_id = bl.show_id
            LEFT JOIN episodes e 
                   ON bl.episode_id = e.episode_id
            LEFT JOIN shows sh 
                   ON st.show_id = sh.show_id
            LEFT JOIN channels ch 
                   ON ch.channel_id = bl.channel_id
            LEFT JOIN show_adverts sa 
                   ON sa.show_id = sh.show_id
            WHERE %s = ANY(st.days_of_week)
              AND st.replication_year = %s
              AND st.show_id <> 173
        )
        UNION ALL
        (
            -- Movies (special case show_id = 173)
            SELECT DISTINCT ON (st.channel_id, st.show_id, st.time_slot)
                   split_part(ch.channel_name, '-', 3) AS channel,
                   st.time_slot,
                   CEIL((m.end_point - m.start_point) / 1800) * 1800 AS show_duration,
                   'MOVIE' AS show,
                   m.movie_genre AS show_genre,
                   m.movie_name AS title,
                   m.movie_description AS description,
                   TO_DATE(m.movie_release_date::TEXT, 'YYYY') AS movie_release_date,
                   sa.ad_image,
                   sa.ad_text,
                   sa.network_img,
                   sa.channel_img,
                   sa.font,
                   sa.font_size,
                   173 as show_id,
                   m.is_bw,
                   m.movie_stars as actors
            FROM schedule_template st
            LEFT JOIN broadcast_log bl 
                   ON st.channel_id = bl.channel_id AND st.show_id = bl.show_id
            LEFT JOIN movies m 
                   ON bl.episode_id = m.movie_id
            LEFT JOIN channels ch 
                   ON ch.channel_id = bl.channel_id
            LEFT JOIN show_adverts sa 
                   ON sa.show_id = st.show_id
            WHERE %s = ANY(st.days_of_week)
              AND st.replication_year = %s
              AND st.show_id = 173
        )
        --ORDER BY channel, time_slot, sh.show_id, date_played DESC;
        """, (dow, year, dow, year))
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


def wrap_text_to_width(c: canvas.Canvas, text: str, max_width: float, font_name="Helvetica", font_size=9):
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
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(width, height))
    required_space = sum(h for h, _, _ in (get_wrapped_title_des(c, show, max_line_width) for show in shows))

    return required_space


def get_layout_options(
        total_length: float,
        page_height: float,
        col_width: float,
        used_ads: set[str],
        min_height: float = 50
) -> dict[str, Any]:
    """
    Given the total length needed for shows, return a random viable layout option.
    Always returns at least one option.

    Args:
        total_length: Vertical space needed for shows
        page_height: Height of one column
        col_width: Width of one column
        used_ads: Set of ads already used
        min_height: Minimum ad height

    Returns:
        A layout dict with keys: type, capacity, pages, (optionally) ad
    """
    layouts = []
    ads_1, ads_2, ads_full = get_sorted_ads()

    # --- Option 1: No ads ---
    if total_length <= 2 * page_height:
        layouts.append({
            "type": "no_ads",
            "capacity": 2 * page_height,
            "pages": 1
        })

    # --- Option 2: Full column ad ---
    if total_length <= page_height:
        ad = select_ad_for_column(ads_full, page_height, col_width, used_ads, False, min_height)
        if ad:
            layouts.append({
                "type": "full_column_ad",
                "ad": ad,
                "capacity": page_height,
                "pages": 1
            })

    # --- Option 3: Two-column ad at bottom ---
    ad = select_ad_for_column(ads_2, page_height, col_width * 2, used_ads, True, min_height)
    if ad:
        _, ad_h = get_scaled_image_size(ad, col_width * 2, page_height)
        remaining_height = page_height - ad_h
        if total_length <= 2 * remaining_height:
            layouts.append({
                "type": "two_column_ad",
                "ad": ad,
                "capacity": 2 * remaining_height,
                "pages": 1
            })

    # --- Option 4: Smaller stacked ads ---
    ad = select_ad_for_column(ads_1, page_height, col_width, used_ads, False, min_height)
    if ad:
        _, ad_h = get_scaled_image_size(ad, col_width, page_height)
        remaining_height = page_height - ad_h
        if total_length <= page_height + remaining_height:
            layouts.append({
                "type": "stacked_ads",
                "ad": ad,
                "capacity": page_height + remaining_height,
                "pages": 1
            })

    # --- Fallback ---
    if not layouts:
        capacity_per_page = 2 * page_height
        num_pages = (total_length + capacity_per_page - 1) // capacity_per_page
        return {
            "type": "multi_page_fallback",
            "capacity": capacity_per_page,
            "pages": num_pages
        }

    # Randomize viable choices
    return random.choice(layouts)


def render_ad_image(ad_image, ad_text, time_slot, font, col_width, thumb_height, font_size, is_cbs):
    """Load, resize, and prepare the ad image with text space reserved."""
    im = Image.open(f"{SHOW_IMAGES}{ad_image}")
    ad_text = f"{ad_text} {convert_to_12hr(time_slot, is_cbs)}"

    # Resize proportionally
    scale = col_width / im.size[0]
    new_w, new_h = int(col_width), int(im.size[1] * scale)
    im = im.resize((new_w, new_h), resample=Image.LANCZOS)

    # Measure text
    font_obj = ImageFont.truetype(f"{FONT_DIR}{font}", size=font_size)
    draw = ImageDraw.Draw(im)
    bbox = draw.textbbox((0, 0), ad_text, font=font_obj)
    text_w, text_h = bbox[2] - bbox[0], bbox[3] - bbox[1]

    # Create white background tall enough for image + text + logos
    total_h = round(new_h + text_h * 1.6 + thumb_height * 1.6)
    new_im = Image.new("L", (new_w, total_h), 255)
    new_im.paste(im, (0, 0))

    return new_im, ad_text, font_obj, text_w, text_h, new_w, new_h


def create_show_image(ad_data_list: list, used_ads: list, available_height: float = 0) -> tuple[tuple[float, float], ImageReader] | tuple[None, None]:
    best_fit = None
    best_height = 0

    # Pass 1: find best fitting ad
    for ad_i, ad_data in enumerate(ad_data_list):
        ad_image, ad_text, network_img, channel_img, font, font_size, time_slot = itemgetter(
            'ad_image', 'ad_text', 'network_img', 'channel_img', 'font', 'font_size', 'time_slot'
        )(ad_data)

        if ad_image in used_ads:
            continue

        new_im, _, _, _, _, _, _ = render_ad_image(
            ad_image, ad_text, time_slot, font, col_width, THUMB_HEIGHT, font_size, "cbs" in network_img
        )
        _, new_im_h = new_im.size

        if best_height < new_im_h < available_height:
            best_fit = ad_i
            best_height = new_im_h

    if best_fit is None:
        return None, None

    # Pass 2: actually build the chosen ad image
    ad_image, ad_text, network_img, channel_img, font, font_size, time_slot = itemgetter(
        'ad_image', 'ad_text', 'network_img', 'channel_img', 'font', 'font_size', 'time_slot'
    )(ad_data_list[best_fit])

    new_im, ad_text, font_obj, text_w, text_h, im_w, im_h = render_ad_image(
        ad_image, ad_text, time_slot, font, col_width, THUMB_HEIGHT, font_size, "cbs" in network_img
    )

    used_ads.add(ad_image)

    # Draw centered text
    x = (im_w - text_w) / 2
    y = im_h + 2
    draw = ImageDraw.Draw(new_im)
    draw.text((x, y), ad_text, font=font_obj, fill=0)

    logos = [network_img, channel_img]
    scaled_logos = []

    # Scale all logos by THUMB_HEIGHT while preserving aspect ratio
    for logo in logos:
        logo_im = Image.open(f"{SHOW_LOGOS}{logo}")
        w, h = logo_im.size
        new_w = round((w / h) * THUMB_HEIGHT)
        new_h = THUMB_HEIGHT
        logo_im = logo_im.resize((new_w, new_h), resample=Image.LANCZOS)
        scaled_logos.append(logo_im)

    # total width of both logos + spacing
    spacing = 4
    total_w = sum(im.size[0] for im in scaled_logos) + spacing

    # starting x so the block is centered
    start_x = round((im_w - total_w) / 2)

    # paste both logos side by side
    x = start_x
    for logo_im in scaled_logos:
        new_im.paste(logo_im, (x, y + THUMB_HEIGHT + 2), logo_im if logo_im.mode == "RGBA" else None)
        x += logo_im.size[0] + spacing
    # Save to memory buffer
    buf = io.BytesIO()
    new_im.save(buf, format="PNG")
    buf.seek(0)

    return new_im.size, ImageReader(buf)


def draw_ad_box(c: canvas.Canvas , x: float, y: float, box_w: float, box_h: float, file_path: str) -> None:
    """
    Draw an ad in a column at (x, y) with given width and height.
    If label_or_path is an image path, draw it scaled.
    Otherwise, draw a gray box with a text label.
    """
    try:
        img = ImageReader(file_path)
        iw, ih = img.getSize()
        scale = min(box_w / iw, box_h / ih)
        new_w, new_h = iw * scale, ih * scale
        offset_x = x + (box_w - new_w) / 2
        offset_y = y + (box_h - new_h) / 2
        c.drawImage(img, offset_x, offset_y, new_w, new_h)
    except Exception:
       return


def fill_column_ads(c, ads: list, col_i: int, col_data: dict) -> None:
    ad_y = col_data['current_y']
    ad_bottom = col_data['y_bottom']
    ad_list = []
    ad_space = col_data['current_y'] - col_data['y_bottom']
    x = col_x_positions[col_i]

    while ad_bottom < ad_y:
        is_bottom_ad = True if ad_bottom == y_bottom else False
        filler_scaled_img = None

        filler_ad = select_ad_for_column(ads, ad_y - ad_bottom, col_width, used_ads, is_bottom_ad, min_height=50)

        if filler_ad:
            filler_scaled_img = get_scaled_image_size(filler_ad, col_width, available_height=(ad_y - ad_bottom))

        if filler_scaled_img:
            img_width, img_height = filler_scaled_img
            ad_list.append((filler_ad, img_width, img_height, ad_bottom))
            ad_bottom += (img_height + 10)

        else:
            ad_space = col_data['current_y'] - col_data['y_bottom']
            break

    for advert in ad_list:
        ad, img_width, img_height, bottom = advert
        draw_ad_box(c, x - 4, ad_y - img_height, img_width, img_height, ad)


def draw_day_date_header(c, dow: int, start_col: int, col_index: int, col_w: float) -> None:
    # DOW
    c.setFont("Helvetica-Bold", 12)
    c.drawString(col_x_positions[start_col], height - 0.35 * inch, weekday_name_from_int(dow))
    c.setFont("Helvetica", 8)
    c.drawString(col_x_positions[start_col], height - 0.5 * inch, "EVENING")

    current_date = datetime.datetime.now().strftime("%B %d") + " 1970"  # e.g., September 17, 2025
    c.setFont("Helvetica", 9)
    c.drawRightString(col_x_positions[col_index] + (col_w * 1.7), height - 0.4 * inch, current_date)


def draw_channel_boxes(c: canvas.Canvas, box_x: float, y: float, channel: int | str, title: str) -> float:
    font_size = 8
    text_height = font_size * 0.7
    box_y = y - (box_size - text_height) / 2

    channel_groups = [(4, 36), (3, 7, 42), (2,)]
    title_length = box_size + box_padding + c.stringWidth(title, "Helvetica-Bold", 10)
    channels = next((t for t in channel_groups if int(channel) in t), (channel,))

    for fake_channel in channels:

        if fake_channel in clear_channels:
            draw_clear_number_box(c, box_x, box_y, box_width, box_size, fake_channel)
        else:
            draw_number_box(c, box_x, box_y, box_width, box_size, fake_channel)
        title_length += (box_size + box_padding)
        box_x = box_x + box_size + box_padding

    return box_x


def get_wrapped_title_des(c, item, max_line_width, font_size: int = 8):
    string_width = c.stringWidth(item['show'].upper(), "Helvetica-Bold", font_size)

    full_title = (
        f"{item['show'].upper()} -{item['show_genre'].split(',')[0].strip().capitalize()}"
        if item['show_duration'] > 1800 and string_width < max_line_width else
        item['show'].upper()
    )

    temp_description = item['description']

    if item['show_duration'] > 1800:
        temp_description += f" ({seconds_to_human(item['show_duration'])})"

    if item['show_id'] == 173:
        temp_description = f'"{item["title"]} {temp_description} {item["actors"]} ({item["date_played"].year})'

    description_lines = wrap_text_to_width(
        c, temp_description, max_line_width, font_name="Helvetica", font_size=8
    )
    # Wrap the title
    title_lines = wrap_text_to_width(
        c, full_title, max_line_width - 10, font_name="Helvetica-Bold", font_size=10
    )

    title_line_height = line_height / 1.5
    title_height = len(description_lines) * title_line_height
    desc_line_height = 8 * 1.2
    desc_height = len(description_lines) * desc_line_height
    block_height = title_height + desc_height + (desc_line_height / 2)

    return block_height, description_lines, title_lines


def draw_title_des(c, x: float, y: float, item: dict, max_line_width: float):
    _, description_lines, title_lines = get_wrapped_title_des(c, item, max_line_width)
    text_x = draw_channel_boxes(c, x, y, item['channel'], title_lines[0])

    if (col_width - (text_x - x)) + c.stringWidth(title_lines[0], "Helvetica-Bold", 8) > col_width:
        _, _, title_lines = get_wrapped_title_des(c, item, col_width - (text_x - x) + 20)

    # Draw title
    c.setFont("Helvetica-Bold", 8)
    for cnt, line in enumerate(title_lines):
        c.drawString(text_x, y, line)

        # Only on the last line
        if cnt == len(title_lines) - 1 and not item['is_bw'] and item['show_id'] != 173:
            # Measure the width of the text we just drew
            text_width = c.stringWidth(line, "Helvetica-Bold", 8)  # <-- match your font + size
            # Position box just after text
            box_x = text_x + text_width + 2  # 5pt padding
            draw_clear_number_box(c, box_x, y - 1, box_width, box_size, "C")

        y -= line_height / 1.5
        text_x = x

    # Draw description
    c.setFont("Helvetica", 8)
    for line in description_lines:
        c.drawString(x, y, line)
        y -= 8 * 1.2
    y -= 8 * 1.2 / 2
    return y


def draw_timeslot(c, x: float, y: float, item, time_slot) -> str:
    if time_slot != item['time_slot']:
        c.setFont("Helvetica", 8)
        c.drawString(x - 0.3 * inch, y, convert_to_12hr(item['time_slot']))
        return item['time_slot']
    return time_slot


def draw_show_ad(c, show_ads: list, current_x: float, current_y: float, bottom_y: float) -> tuple | None:
    dems, image = create_show_image(show_ads, used_ads, available_height=int(current_y - bottom_y))
    if dems:
        draw_ad_box(c, current_x, current_y - dems[1] - 5, dems[0], dems[1], image)
        return dems

    return None


def generate_tv_guide(imposed_sheets: list, dow: int, col_w: float, filename="tv_guide.pdf") -> None:
    c = canvas.Canvas(filename, pagesize=(width, height))
    c.setFont("Helvetica", 10)
    page_numbers = get_page_numbers(len(imposed_sheets) * 2)

    ads_1, _, ads_full = get_sorted_ads()

    def draw_page_number(side: str, page_number: int) -> None:
        c.setFont("Helvetica", 8)
        y = 0.35 * inch

        if side == "left":
            x = 0.25 * inch
            c.drawString(x, y, f"A-{page_number} TV GUIDE")

        elif side == "right":
            x = width - 0.25 * inch
            c.drawRightString(x, y, f"A-{page_number} TV GUIDE")

    def get_show_id_time_slots(timeslot_shows: list) -> list:
        # Only include items where show_id is not None
        return [
            show["show_id"]
            for show in timeslot_shows
            if show.get("ad_image") is not None
        ]

    for sheet_index, sheet in enumerate(imposed_sheets):
        # Create 4 independent dicts (not 4 references to the same dict)
        column_space = [
            {"current_y": y_start, "y_bottom": y_bottom, "type": None, "idx": i}
            for i in range(4)
        ]

        for side_index, page_data in enumerate(sheet):

            if not page_data:
                continue

            start_col = 0 if side_index == 0 else 2
            end_col = start_col + 2  # WAS +1 — that was incorrect

            col_index = start_col
            y = y_start
            time_slot = None

            shows_ad_list = get_show_id_time_slots(page_data)
            matching_ads = [show for show in page_data if show.get("show_id") in shows_ad_list]
            column_space[col_index]["has_ads"] = True if matching_ads else False

            draw_day_date_header(c, dow, start_col, col_index, col_w)

            needed_space = get_needed_draw_space(page_data, col_width)
            best_layout = get_layout_options(needed_space, y_start, col_width, used_ads, min_height=50)
            layout_type = best_layout.get('type')

            column_space[col_index]['type'] = layout_type
            column_space[col_index]['idx'] = col_index

            if layout_type == 'full_column_ad':
                img_x = col_x_positions[start_col] if side_index == 0 else col_x_positions[end_col - 1]
                available_space = height - (y_bottom + y_margin)
                img_width, img_height = get_scaled_image_size(best_layout['ad'], col_width, True, available_height=available_space)
                adjusted_img_x = .4 * inch if side_index == 0 else img_x
                draw_ad_box(c, adjusted_img_x , y - img_height + 4, img_width, img_height, best_layout['ad'])
                if side_index == 0:
                    x1 = x2 = adjusted_img_x + img_width + 5
                    y1 = y + 5
                    y2 = y - img_height - 5
                else:
                    x1 = x2 = adjusted_img_x - 5
                    y1 = y + 5
                    y2 = y - img_height - 5

                c.line(x1, y1, x2, y2)

                if side_index == 0:
                    col_index = start_col + 1

            elif layout_type == 'two_column_ad':
                img_x = col_x_positions[start_col]
                img_width, img_height = get_scaled_image_size(best_layout['ad'], (max_line_width * 2) + x_margin,
                                                              available_height=None)

                c.line(img_x, y_bottom + img_height + 10, img_x + img_width, y_bottom + img_height + 10)
                draw_ad_box(c, img_x, y_bottom, img_width, img_height, best_layout['ad'])
                y_bottom_adjusted = img_height + y_bottom
                column_space[start_col]['y_bottom'] = y_bottom_adjusted
                column_space[start_col + 1]['y_bottom'] = y_bottom_adjusted

            elif layout_type == 'stacked_ads':
                x = col_x_positions[col_index]
                if matching_ads:
                    img_dimensions = draw_show_ad(c, matching_ads, x, y, y_bottom)
                    if img_dimensions:
                        y -= (img_dimensions + 10)

                img_width, img_height = get_scaled_image_size(best_layout['ad'], col_width,
                                                              available_height=int(y - y_bottom))
                draw_ad_box(c, x - 10, y - img_height + 10, img_width, img_height, best_layout['ad'])
                y -= (img_height + 10)

            for cnt, item in enumerate(page_data):
                x = col_x_positions[col_index]
                block_height, _, _ = get_wrapped_title_des(c, item, max_line_width)

                if layout_type == "two_column_ad":
                    _, img_height = get_scaled_image_size(best_layout['ad'], col_width * 2, available_height=None)
                    compare_to = y - img_height - 20 - block_height
                else:
                    compare_to = y - block_height

                if compare_to < y_bottom:
                    col_index += 1
                    y = y_start

                    if col_index >= end_col:  # both columns on this side are full
                        break  # stop writing more shows on this side
                    x = col_x_positions[col_index]

                time_slot = draw_timeslot(c, x, y, item, time_slot)
                y = draw_title_des(c, x, y, item, max_line_width)

                if layout_type == 'full_column_ad':
                    col_i: int = 0 if side_index == 0 else 3
                    col_2 = 1 if col_i == 0 else 2

                    column_space[col_i]["current_y"] = y_bottom
                    column_space[col_i]["y_bottom"] = y_bottom
                    column_space[col_2]["current_y"] = y
                    column_space[col_2]["y_bottom"] = y_bottom

                elif layout_type == 'two_column_ad':
                    _, img_height = get_scaled_image_size(best_layout['ad'], (max_line_width * 2) + x_margin,
                                                          available_height=None)
                    y_bottom_adjusted = img_height + y_bottom

                    column_space[col_index]["current_y"] = y
                    column_space[start_col]["y_bottom"] = y_bottom_adjusted
                    column_space[end_col - 1]["y_bottom"] = y_bottom_adjusted

                else:
                    column_space[col_index]["current_y"] = y
                    column_space[col_index]["y_bottom"] = y_bottom

                if matching_ads and cnt == len(page_data) - 1:
                    dems, image = create_show_image(matching_ads, used_ads, available_height=(
                            column_space[col_index]["current_y"] - column_space[col_index]["y_bottom"]))
                    if dems:
                        draw_ad_box(c, x, y - dems[1] - 5, dems[0], dems[1], image)
                        column_space[col_index]["current_y"] = y - dems[1] - 10

            if side_index == 0:
                draw_page_number("left", page_numbers.pop(0))
            else:
                draw_page_number("right", page_numbers.pop(0))

        left_has_ads = any(col.get("has_ads", False) for col in column_space if col.get("idx") in (0, 1))
        right_has_ads = any(col.get("has_ads", False) for col in column_space if col.get("idx") in (2, 3))

        for col_i, col_data in enumerate(column_space):
            idx = col_data.get("idx", col_i)
            x = col_x_positions[col_i]

            if idx in (0, 1) and left_has_ads:
                img_dimensions = draw_show_ad(c, matching_ads, x, col_data["current_y"], col_data["y_bottom"])
                if img_dimensions:
                    col_data["current_y"] = col_data["current_y"] - img_dimensions[1] - 10

            if idx in (2, 3) and right_has_ads:
                img_dimensions = draw_show_ad(c, matching_ads, x, col_data["current_y"], col_data["y_bottom"])
                if img_dimensions:
                    col_data["current_y"] = col_data["current_y"] - img_dimensions[1] - 10

            fill_column_ads(c, [*ads_1, *ads_full], col_i, col_data)

        if sheet_index < len(imposed_sheets) - 1:
            c.showPage()

    c.save()
    print(f"PDF saved to {filename}")


if __name__ == "__main__":
    DOW = 5
    YEAR = 1970

    shows = get_current_schedule(DOW, YEAR)

    if not shows:
        print("No shows found")
        exit(0)

    [show.update({"description": truncate_paragraph(show["description"], 1)}) for show in shows]
    sorted_list = sorted(shows, key=lambda x: (x['time_slot'], int(x['channel'])))
    max_shows = fit_shows_on_logical_page(shows, height, top_margin=y_margin, bottom_margin=y_bottom)
    chunks = list(chunk_shows_random(sorted_list, 4, max_shows))
    generate_tv_guide(imposition_order(chunks), DOW, col_width)
