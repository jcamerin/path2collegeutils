#!/usr/bin/env python3
# How to use: historical_price_scraper.py --start-date 2025-01-17 --out prices.csv --clean-price --headful

import argparse
import csv
import sys
import time
import re
from typing import Optional, Tuple
from datetime import datetime, timedelta, date
from dateutil import tz
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Page, Frame

# -------------------- SITE & SELECTORS --------------------
DEFAULT_URL = "https://www.gapath2college.com/gadtpl/ao/overview.cs"

# Be flexible: pages change; try several selectors and across frames.
DATE_INPUT_SELECTORS = [
    "input#asofDate",
    "input[id='asofDate' i]",            # case-insensitive exact
    "input[type='text'][id*='asof' i]",  # id contains asof (case-insens)
    "input[type='text'][name*='asof' i]"
]
CONTENTEDITABLE_SELECTORS = [
    "div[contenteditable='plaintext-only']",
    "div[contenteditable='true']",
    "[contenteditable][role='textbox']",
]
SUBMIT_BUTTON_SELECTORS = [
    "#customAsOfBal",
]
PRICE_CELL_SELECTORS = [
    "#caoBalDiv > table > tbody > tr > td.unite-table-cell.unite-table-cell-2.unite-table-column-unit"
]

WAIT_AFTER_SUBMIT_SEC = 0.25
MAX_WAIT_PRICE_MS = 10000
PER_DAY_RETRIES = 2
POLITE_DELAY_BETWEEN_DAYS_SEC = 0.35

PRICE_REGEX = re.compile(r"-?\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})|[0-9]+(?:\.[0-9]{2}))")

# -------------------- UTILITIES --------------------
def to_mmddyyyy(d: date) -> str:
    return d.strftime("%m/%d/%Y")

def daterange(start: date, end_inclusive: date):
    cur = start
    while cur <= end_inclusive:
        yield cur
        cur += timedelta(days=1)

def first_visible_in_frame(frame: Frame, selectors) -> Optional[str]:
    for sel in selectors:
        try:
            el = frame.query_selector(sel)
            if el and el.is_visible():
                return sel
        except Exception:
            pass
    return None

def find_across_frames(page: Page, selectors) -> Tuple[Optional[Frame], Optional[str]]:
    # Try main frame first
    sel = first_visible_in_frame(page.main_frame, selectors)
    if sel:
        return page.main_frame, sel
    # Then any child frames
    for fr in page.frames:
        if fr is page.main_frame:
            continue
        sel = first_visible_in_frame(fr, selectors)
        if sel:
            return fr, sel
    return None, None

def set_date_anywhere(page: Page, target_str_mmddyyyy: str) -> bool:
    # Prefer input element
    fr, sel = find_across_frames(page, DATE_INPUT_SELECTORS)
    if fr and sel:
        try:
            el = fr.query_selector(sel)
            el.click()
            page.keyboard.press("Control+A" if sys.platform != "darwin" else "Meta+A")
            page.keyboard.press("Backspace")
            el.type(target_str_mmddyyyy, delay=20)
            return True
        except Exception:
            pass
    # Fallback to contenteditable
    fr, sel = find_across_frames(page, CONTENTEDITABLE_SELECTORS)
    if fr and sel:
        try:
            el = fr.query_selector(sel)
            el.click()
            page.keyboard.press("Control+A" if sys.platform != "darwin" else "Meta+A")
            page.keyboard.press("Backspace")
            el.type(target_str_mmddyyyy, delay=20)
            return True
        except Exception:
            pass
    return False

def click_submit_anywhere(page: Page) -> bool:
    fr, sel = find_across_frames(page, SUBMIT_BUTTON_SELECTORS)
    if fr and sel:
        try:
            fr.click(sel)
            return True
        except Exception:
            return False
    return False

def locate_price_locator(page: Page):
    # Return the first locator that exists/visible across frames
    # (We use locators later for waiting/reading text)
    # Try main frame first
    for sel in PRICE_CELL_SELECTORS:
        try:
            loc = page.locator(sel).first
            if loc and loc.count() > 0:
                # not always visible immediately; we'll still try to use it
                return loc
        except Exception:
            pass
    # Fallback: any frame-specific locator
    for fr in page.frames:
        for sel in PRICE_CELL_SELECTORS:
            try:
                loc = fr.locator(sel).first
                if loc and loc.count() > 0:
                    return loc
            except Exception:
                pass
    return None

def extract_price_text(page: Page) -> str:
    try:
        loc = locate_price_locator(page)
        if not loc:
            return ""
        return (loc.inner_text() or "").strip()
    except Exception:
        return ""

def wait_for_price_update(page: Page, prev_text: Optional[str]) -> str:
    # We search a locator repeatedly; use a function that compares its text.
    deadline = time.time() + (MAX_WAIT_PRICE_MS / 1000.0)
    last = None
    while time.time() < deadline:
        txt = extract_price_text(page)
        last = txt
        if prev_text:
            if txt and txt != prev_text:
                return txt.strip()
        else:
            if txt:
                return txt.strip()
        time.sleep(0.15)
    # Timeout: return whatever we last saw (may be empty)
    return (last or "").strip()

def clean_price(raw: str) -> str:
    if not raw:
        return ""
    m = PRICE_REGEX.search(raw.replace("\u00A0", " ").strip())
    if not m:
        return raw.strip()
    return m.group(1).replace(",", "")

# -------------------- MAIN --------------------
def main():
    ap = argparse.ArgumentParser(description="Download historical prices to CSV by iterating dates.")
    ap.add_argument("--url", default=DEFAULT_URL, help="Target page URL.")
    ap.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD (inclusive).")
    ap.add_argument("--out", default="prices.csv", help="Output CSV path.")
    ap.add_argument("--headful", action="store_true", help="Run with a visible browser (recommended for login/MFA).")
    ap.add_argument("--slowmo", type=int, default=0, help="Slowdown in ms (e.g., 200).")
    ap.add_argument("--clean-price", action="store_true", help="Normalize price text to decimal.")
    return run(ap.parse_args())

def run(args):
    try:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    except ValueError:
        print("Error: --start-date must be YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    today_local = datetime.now(tz=tz.tzlocal()).date()
    if start > today_local:
        print("Error: --start-date cannot be in the future.", file=sys.stderr)
        sys.exit(1)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful, slow_mo=args.slowmo)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()

        # 1) Go to overview; if redirected to login, log in manually (since MFA)
        page.goto(args.url, wait_until="domcontentloaded")

        if args.headful:
            print("\nIf you see a login page or MFA, complete it manually. "
                  "Once you reach Account Overview, leave the tab open; the script will proceed.\n")

        # Give you time to log in if needed
        # (We’ll also proceed immediately if the elements are already present.)
        t0 = time.time()
        while time.time() - t0 < 120:  # up to 2 minutes to log in
            # Check if our target elements are present across frames
            fr_input, sel_input = find_across_frames(page, DATE_INPUT_SELECTORS)
            fr_ce, sel_ce = find_across_frames(page, CONTENTEDITABLE_SELECTORS)
            fr_btn, sel_btn = find_across_frames(page, SUBMIT_BUTTON_SELECTORS)
            if (fr_input and sel_input) or (fr_ce and sel_ce):
                if fr_btn and sel_btn:
                    break
            time.sleep(0.5)

        # CSV
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "close"])

            cur = start
            while cur <= today_local:
                mmddyyyy = to_mmddyyyy(cur)
                last_error = None

                for attempt in range(1, PER_DAY_RETRIES + 2):
                    try:
                        prev_text = extract_price_text(page)

                        if not set_date_anywhere(page, mmddyyyy):
                            raise RuntimeError(
                                "Could not locate a date field (tried multiple selectors and frames). "
                                "If you just logged in, refresh or navigate to Account Overview."
                            )

                        # Some widgets need Enter or blur
                        try:
                            page.keyboard.press("Enter")
                        except Exception:
                            pass

                        if not click_submit_anywhere(page):
                            raise RuntimeError("Could not find/press the submit/update button.")

                        time.sleep(WAIT_AFTER_SUBMIT_SEC)

                        new_text = wait_for_price_update(page, prev_text if prev_text else None)
                        price_out = clean_price(new_text) if args.clean_price else new_text

                        writer.writerow([cur.isoformat(), price_out])
                        time.sleep(POLITE_DELAY_BETWEEN_DAYS_SEC)
                        break
                    except Exception as e:
                        last_error = e
                        if attempt <= PER_DAY_RETRIES:
                            time.sleep(0.8 * attempt)
                            try:
                                page.evaluate("window.scrollTo(0, 0);")
                            except Exception:
                                pass
                            continue
                        sys.stderr.write("[WARN] {}: {}\n".format(cur.isoformat(), last_error))
                        writer.writerow([cur.isoformat(), ""])
                        time.sleep(POLITE_DELAY_BETWEEN_DAYS_SEC)

                cur += timedelta(days=1)

        browser.close()
    print("Done. Wrote CSV to: {}".format(args.out))

if __name__ == "__main__":
    main()

