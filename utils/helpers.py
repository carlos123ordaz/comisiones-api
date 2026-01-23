import pandas as pd
import numpy as np
import bcrypt


def clean_nan_values(data):
    if isinstance(data, list):
        return [clean_nan_values(item) for item in data]
    elif isinstance(data, dict):
        cleaned = {}
        for key, value in data.items():
            if isinstance(value, float):
                if pd.isna(value) or np.isinf(value):
                    cleaned[key] = 0
                else:
                    cleaned[key] = value
            elif isinstance(value, dict):
                cleaned[key] = clean_nan_values(value)
            elif isinstance(value, list):
                cleaned[key] = clean_nan_values(value)
            else:
                cleaned[key] = value
        return cleaned
    else:
        if isinstance(data, float) and (pd.isna(data) or np.isinf(data)):
            return 0
        return data


def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return bcrypt.checkpw(
            plain_password.encode('utf-8'),
            hashed_password.encode('utf-8')
        )
    except Exception as e:
        print(f"Error verificando password: {e}")
        return False
