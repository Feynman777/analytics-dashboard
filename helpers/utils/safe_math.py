def safe_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def safe_int(val, default=0):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def safe_round(val, digits=2, default=0.0):
    try:
        return round(float(val), digits)
    except (TypeError, ValueError):
        return default
