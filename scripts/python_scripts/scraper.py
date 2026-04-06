from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

start_url = "https://www.expansion.com/mercados/cotizaciones/indices/ibex35_I.IB.html"

with webdriver.Firefox() as driver:
    driver.get(start_url)
    wait = WebDriverWait(driver, 15)

    # 1. Aceptar Cookies (usando el texto del botón por seguridad)
    try:
        cookie_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Aceptar')]")))
        cookie_btn.click()
    except:
        pass

    try:
        # Esperamos a que la tabla cargue
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
        
        # Buscamos todas las filas
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")

        for row in rows:
            # IMPORTANTE: Buscamos tanto 'td' como 'th' para no saltarnos el nombre
            cells = row.find_elements(By.CSS_SELECTOR, "td, th")
            
            # Según tu imagen, la fila tiene 10 columnas
            if len(cells) >= 10:
                # --- MAPEO SEGÚN TU IMAGEN ---
                # 0: Valor (Nombre)
                # 1: Último
                # 5: Máximo
                # 6: Mínimo
                # 9: Hora (o fecha 02/04)

                nombre = cells[0].text.strip()
                
                def limpiar(val):
                    # Quitamos puntos de miles y cambiamos coma por punto
                    return val.replace('.', '').replace(',', '.')

                ultimo = limpiar(cells[1].text)
                maximo = limpiar(cells[5].text)
                minimo = limpiar(cells[6].text)
                hora   = cells[9].text.strip()

                # Formato final solicitado
                if nombre:
                    print(f"{nombre},{ultimo},{maximo},{minimo},{hora}")

    except Exception as e:
        print(f"Error: {e}")