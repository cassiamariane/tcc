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

def format_value(value):
    
    if 'BRL' in value:
        value = value.replace(' BRL', '').strip()
    if 'K' in value:
        factor = 1_000
        value = value.replace('K', '').strip()
    if 'M' in value:
        factor = 1_000_000
        value = value.replace('M', '').strip()
    elif 'B' in value:
        factor = 1_000_000_000
        value = value.replace('B', '').strip()
    else:
        factor = 1 
    
    final_value = float(value.replace('.', '').replace(',', '.')) * factor
    
    return final_value

def highlight_sentiment(val):
    if val == "Neutro":
        return "color: #4A90E2"
    elif val in ["Positivo", "Muito positivo"]:
        return "color: #2FBF42"
    else:
        return "color: #CB3131"