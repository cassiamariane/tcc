import requests

def get_coordinates(city):
    url = f"https://nominatim.openstreetmap.org/search?city={city}&country=Brazil&format=json"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com",
        "Connection": "keep-alive"
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        try:
            data = response.json()
            if data:
                latitude = data[0]['lat']
                longitude = data[0]['lon']
                return latitude, longitude
        except ValueError:
            print(f"Error decoding JSON for city: {city}")
    else:
        print(f"Error fetching data for city: {city}, Status code: {response.status_code}")
    
    return None, None

def formatar_valor(valor_str):
    
    if 'BRL' in valor_str:
        valor_str = valor_str.replace(' BRL', '').strip()
    if 'K' in valor_str:
        fator = 1_000
        valor_str = valor_str.replace('K', '').strip()
    if 'M' in valor_str:
        fator = 1_000_000
        valor_str = valor_str.replace('M', '').strip()
    elif 'B' in valor_str:
        fator = 1_000_000_000
        valor_str = valor_str.replace('B', '').strip()
    else:
        fator = 1 
    
    valor_num = float(valor_str.replace('.', '').replace(',', '.')) * fator
    
    return valor_num