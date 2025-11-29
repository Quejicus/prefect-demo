import os

import dlt
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_github import GitHubCredentials


def set_github_pat_env():
    pat = GitHubCredentials.load("github-pat").token.get_secret_value()
    os.environ["SOURCES__ACCESS_TOKEN"] = pat


def make_bq_destination():
    gcp_credentials = GcpCredentials.load("gcp-creds")
    creds = gcp_credentials.service_account_info.get_secret_value() or {}
    project = creds.get("project_id")
    return dlt.destinations.bigquery(project_id=project, credentials=creds)


@task(log_prints=True)
def run_resource(
    resource_name: str,
    bq_destination: dlt.destinations.bigquery,
    incremental_date: str | None = None,
):
    import github_pipeline

    base_source = github_pipeline.github_source
    if incremental_date and resource_name == "issues":
        base_source.issues.apply_hints(
            incremental=dlt.sources.incremental(
                "created_at", initial_value=incremental_date
            )
        )

    selected_source = base_source.with_resources(resource_name)

    pipeline = dlt.pipeline(
        pipeline_name=f"github_incremental_{resource_name}",
        destination=bq_destination,
        dataset_name="demo_incremental_github",
        progress="log",
    )

    load_info = pipeline.run(selected_source)
    print(f"Load info for resource {resource_name}: {load_info}")
    return load_info


@flow(log_prints=True)
def main(incremental_date: str | None = None):
    set_github_pat_env()
    bq_destination = make_bq_destination()
    a = run_resource("repos", bq_destination)
    b = run_resource("contributors", bq_destination)
    c = run_resource("releases", bq_destination)
    d = run_resource("issues", bq_destination, incremental_date)
    return a, b, c


if __name__ == "__main__":
    main()
