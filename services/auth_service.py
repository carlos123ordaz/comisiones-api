from typing import Optional, Dict
from config.database import vendedores_collection
from utils.helpers import verify_password


def authenticate_user(username: str, password: str) -> tuple[bool, Optional[Dict], Optional[str]]:
    lista = vendedores_collection.find({}, {'username': 1, '_id': 0})
    print(list(lista))
    vendedor = vendedores_collection.find_one({'username': username})
    print('vendedor: ', vendedor)
    if not vendedor:
        return False, None, "Usuario no encontrado"

    if not verify_password(password, vendedor.get('password', '')):
        return False, None, "Contraseña incorrecta"

    user_data = {
        'nombre': vendedor['nombre'],
        'esLider': vendedor.get('esLider', False),
        'unidad_negocio': vendedor.get('unidad_negocio', 'N/A')
    }

    return True, user_data, "Login exitoso"
