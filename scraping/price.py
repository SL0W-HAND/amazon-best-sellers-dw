import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Set up Chrome options
chrome_options = Options()
#chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

# Initialize the driver
driver = webdriver.Chrome(options=chrome_options)

url = 'https://camelcamelcamel.com/product/B0FQ637L54'
driver.get(url)

# Esperar un poco más para que la página cargue completamente
time.sleep(5)

# Extraer contenido final
soup = BeautifulSoup(driver.page_source, 'lxml')

# Extract product fields table
product_fields = soup.find('table', class_='product_fields')
if product_fields:
    print("Product Information:")
    print("-" * 40)
    rows = product_fields.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True)
            value = cells[1].get_text(strip=True)
            print(f"{label}: {value}")
else:
    print("Product fields table not found")

# Download price history chart using Selenium screenshot
try:
    # Wait for the chart image to be visible
    chart_element = WebDriverWait(driver, 15).until(
        EC.visibility_of_element_located((By.ID, 'summary_chart'))
    )
    
    # Scroll to the chart to ensure it's in view
    driver.execute_script("arguments[0].scrollIntoView(true);", chart_element)
    time.sleep(2)  # Wait for image to fully render
    
    # Wait until the image has loaded (naturalWidth > 0)
    WebDriverWait(driver, 10).until(
        lambda d: d.execute_script(
            "return document.getElementById('summary_chart').naturalWidth > 0"
        )
    )
    
    chart_element.screenshot('price_history_chart2.png')
    print("\nChart saved as 'price_history_chart.png'")
except Exception as e:
    print(f"Failed to download chart: {e}")

driver.quit()
