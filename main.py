import asyncio
import os
from dotenv import load_dotenv
import pandas as pd
import gspread
from playwright.async_api import async_playwright

load_dotenv()

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        scraped_data = [] 
        
        cities = ["vinnitsa", "poltava", "chernigov", "sumy", "cherkassy", "zhytomyr", "rovno", "ternopol"]
        pages_per_city = 3
        
        for city in cities:
            for page_num in range(1, pages_per_city + 1):
                url = f"https://www.olx.ua/uk/elektronika/kompyutery-i-komplektuyuschie/komplektuyuschie-i-aksesuary/{city}/?page={page_num}"
                
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_selector('[data-cy="l-card"]', timeout=15000)
                    
                    cards = await page.locator('[data-cy="l-card"]').all()
                    
                    for card in cards:
                        title_element = card.locator('[data-cy="ad-card-title"] h4')
                        title = await title_element.inner_text() if await title_element.count() > 0 else "Без назви"
                        
                        price_element = card.locator('[data-testid="ad-price"]')
                        price = await price_element.inner_text() if await price_element.count() > 0 else "Без ціни"
                        
                        location_element = card.locator('[data-testid="location-date"]')
                        location_date = await location_element.inner_text() if await location_element.count() > 0 else "Без локації"
                        
                        link_element = card.locator('[data-cy="ad-card-title"] a')
                        link = await link_element.get_attribute('href') if await link_element.count() > 0 else ""
                        full_link = f"https://www.olx.ua{link}" if link else "Немає посилання"
                        
                        scraped_data.append({
                            "Назва": title,
                            "Ціна": price,
                            "Локація та Дата": location_date,
                            "Посилання": full_link
                        })
                        
                    await asyncio.sleep(2) 
                    
                except Exception as e:
                    print(f"Помилка {city} сторінка {page_num}: {e}")
                    continue

        await browser.close()

        df = pd.DataFrame(scraped_data)

        df['Ціна_грн'] = df['Ціна'].str.replace(r'\D', '', regex=True)
        df['Ціна_грн'] = pd.to_numeric(df['Ціна_грн'], errors='coerce') 
        df[['Місто', 'Дата']] = df['Локація та Дата'].str.split(' - ', n=1, expand=True)
        df = df.drop(columns=['Ціна', 'Локація та Дата'])
        
        df = df.dropna(subset=['Назва']) 
        df = df[['Назва', 'Ціна_грн', 'Місто', 'Дата', 'Посилання']] 
        
        try:
            gc = gspread.service_account(filename="credentials.json")
            sheet_url = os.getenv("GOOGLE_SHEET_URL")
            sh = gc.open_by_url(sheet_url)
            worksheet = sh.sheet1
            
            existing_data = worksheet.get_all_records()
            if existing_data:
                existing_df = pd.DataFrame(existing_data)
                new_data_df = df[~df['Посилання'].isin(existing_df['Посилання'])]
            else:
                new_data_df = df
            
            if not new_data_df.empty:
                if not existing_data:
                    data_to_upload = [new_data_df.columns.values.tolist()] + new_data_df.fillna("").values.tolist()
                    worksheet.update(data_to_upload)
                else:
                    worksheet.append_rows(new_data_df.fillna("").values.tolist(), value_input_option='USER_ENTERED')
                
                print("Успіх")
            else:
                print("Усе завантажено")
                
        except Exception as e:
            print(f"Помилка: {e}")

if __name__ == "__main__":
    asyncio.run(main())