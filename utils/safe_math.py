# utils/safe_math.py
def safe_float(val):
    try:
        return float(val)
    except:
        return 0.0
