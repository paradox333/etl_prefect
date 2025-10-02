from prefect import flow

if __name__ == "__main__":
    # 1) WATCHER
    watcher = flow.from_source(
        source="file:///app",
        entrypoint="prefect_flows/watcher_flow.py:watcher_flow",
    )
    watcher.deploy(
        name="watcher",
        work_pool_name="default",
        # cron="*/5 * * * *",
        interval=60,
        tags=["watcher"],
    )

    # 2) ETL
    etl = flow.from_source(
        source="file:///app",
        entrypoint="prefect_flows/etl_flow.py:etl_flow",
    )
    etl.deploy(
        name="etl_api_trigger",
        work_pool_name="default",
        tags=["etl"],
    )
