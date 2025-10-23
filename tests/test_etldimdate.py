import importlib.util
import os

def test_etldimdate_integrity():
    """
    Test unitario básico de integridad:
    - Verifica que el archivo exista
    - Que sea importable sin errores
    - Y que contenga las variables clave de configuración
    """

    script_path = "src/etldimdate.py"
    assert os.path.exists(script_path), "No se encontró el archivo etldimdate.py"

    # Cargar dinámicamente el módulo sin ejecutarlo completamente
    spec = importlib.util.spec_from_file_location("etldimdate", script_path)
    etl_module = importlib.util.module_from_spec(spec)

    try:
        spec.loader.exec_module(etl_module)
    except Exception as e:
        assert False, f"Error al importar el módulo: {e}"

    required_vars = ["bucket", "prefix", "key"]
    for var in required_vars:
        assert hasattr(etl_module, var), f"Falta variable requerida: {var}"

    print("Test de integridad del script ETL pasado correctamente.")
