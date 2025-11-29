import os

import dlt
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner
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
def run_resource(resource_name: str, bq_destination: dlt.destinations.bigquery):
    import github_pipeline

    source = github_pipeline.github_source.with_resources(resource_name)

    pipeline = dlt.pipeline(
        pipeline_name=f"github_remote_{resource_name}",
        destination=bq_destination,
        dataset_name="demo_remote_github",
        progress="log",
    )

    load_info = pipeline.run(source)
    print(f"Load info for resource {resource_name}: {load_info}")
    return load_info


@flow(task_runner=ThreadPoolTaskRunner(max_workers=5), log_prints=True)
def main():
    set_github_pat_env()
    bq_destination = make_bq_destination()
    a = run_resource.submit("repos", bq_destination)
    b = run_resource.submit("contributors", bq_destination)
    c = run_resource.submit("releases", bq_destination)
    d = run_resource.submit("issues", bq_destination)
    e = run_resource.submit("forks", bq_destination)
    return a.result(), b.result(), c.result(), d.result(), e.result()


if __name__ == "__main__":
    main()
