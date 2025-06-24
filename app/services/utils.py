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
    elif val in ["Positivo", "Muito Positivo"]:
        return "color: #2FBF42"
    else:
        return "color: #CB3131"