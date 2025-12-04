import asyncio
from playwright.async_api import async_playwright
import json
import pandas as pd
import logging

async def parse_item(item):
    descripton = await item.eval_on_selector("a.entity-card_name", "el => el.textContent.trim()") #extracting description
    name = descripton.split(',')[0].strip()
    memory = (' ').join(descripton.split(',')[1].strip().split(' ')[0:2])
    year = descripton.split(',')[1].strip().split(' ')[-1]
    color = descripton.split(',')[2].strip()
    price = await item.eval_on_selector("span.carousel-product_price-value", "el => el.textContent.trim()")
    return {
        "name": name,
        "memory": memory,
        "year": year,
        "color": color,
        "price": price
    }


async def scrape(browser, url: str, item_selector: str, next_selector: str):
    page = await browser.new_page()
    await page.goto(url, timeout=20000)
    collected = []

    while True:
        try:
            await page.wait_for_selector(item_selector, timeout=5000)
        except:
            print("No more items found, ending pagination.")
            break

        items = await page.query_selector_all(item_selector)
        for item in items:
            try:
                data = await parse_item(item)
                print(data)
                collected.append(data)
            except Exception as e:
                print(f"Error parsing item: {e}")
            
        nxt = await page.query_selector(next_selector)#finding next button
        if not nxt:
            break
        try:
            await nxt.click()
            await page.wait_for_selector(item_selector, timeout=6000)
            await page.wait_for_timeout(500)
        except Exception as e:
            print(f"Pagination ended: {e}")
            break

    await page.close()
    return collected

async def main():
    URL = "https://ispace.kz/category/ipad"
    ITEM_SELECTOR = "div.carousel-product"
    NEXT_SELECTOR = ".pagination_button__next"

    async with async_playwright() as playwright: # Launching Playwright
        browser = await playwright.chromium.launch(headless=True)
        result = await scrape(browser, URL, ITEM_SELECTOR, NEXT_SELECTOR)
        await browser.close()
        print(f"Collected {len(result)} items")
    
    if result:
        df = pd.DataFrame(result)
        df.to_parquet("/workspaces/SIS2/data/raw_data.parquet", engine="pyarrow", index=False)# Saving data to Parquet
        print(f"Saved {len(result)} items to data/output.parquet")
        logging.info(f"Scraping completed, {len(result)} items saved.")
    else:
        print("No data collected")

if __name__ == "__main__":
    asyncio.run(main())
