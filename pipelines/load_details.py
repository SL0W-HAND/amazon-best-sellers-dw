from load.load_products_details import (
    run_etl_batch,
)

if __name__ == "__main__":
    # Ejemplo: Ejecutar el ETL en modo batch para 10 productos
    max_retries = 5
    retries = 0
    while retries < max_retries:
        stats = run_etl_batch(limit=None, days_threshold=1, headless=True)
        print(stats)
        print(f"Total procesados: {stats['total']}")
        print(f"Éxitos: {stats['success']}")
        errores = stats['server_errors'] + stats['network_errors'] + stats['other_errors']
        print(f"Errores: {errores}")
        if errores == 0:
            break
        retries += 1
        print(f"Retrying... ({retries}/{max_retries})")
    if errores != 0:
        print(f"Finalizado con errores después de {max_retries} intentos.")

    