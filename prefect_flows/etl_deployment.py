from prefect import flow
import asyncio

#source = "file:///app"
source="file://."


async def main():
    # --- 0) STORAGE WATCHER ---
    # Despliega un flujo que monitorea el almacenamiento (MinIO u otro origen)
    monitor_storage = await flow.from_source(
        source=source,
        entrypoint="prefect_flows/monitor_storage.py:monitor_storage",
    )
    await monitor_storage.deploy(
        name="monitor_storage",
        work_pool_name="default",
        interval=60,  # Se ejecuta cada 60 segundos
        tags=["monitor_storage"],
    )

    # --- 1) STATE WATCHER ---
    # Despliega un flujo que revisa periódicamente el estado de los archivos
    watcher = await flow.from_source(
        source=source,
        entrypoint="prefect_flows/watcher_flow.py:watcher_flow",
    )
    await watcher.deploy(
        name="watcher",
        work_pool_name="default",
        cron="*/5 * * * *",  # Se ejecuta cada 5 minutos
        tags=["watcher"],
    )

    # --- 2) ETL PIPELINE ---
    # Despliega el flujo principal de extracción, transformación y carga de datos
    etl = await flow.from_source(
        source=source,
        entrypoint="prefect_flows/etl_flow.py:etl_flow",
    )
    await etl.deploy(
        name="etl_api_trigger",
        work_pool_name="default",
        tags=["etl"],
    )

# Punto de entrada principal: ejecuta la función asíncrona principal
if __name__ == "__main__":
    asyncio.run(main())