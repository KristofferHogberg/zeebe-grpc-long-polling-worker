import asyncio
import json
import logging
from zeebe_grpc import gateway_pb2, gateway_pb2_grpc
from main import get_gateway_stub


async def start_and_complete_task(worker_type):
    zeebe_client = get_gateway_stub()

    while True:
        try:
            activate_jobs_response = zeebe_client.ActivateJobs(
                gateway_pb2.ActivateJobsRequest(
                    type=worker_type,
                    worker=f"Start {worker_type} worker",
                    fetchVariable=['instanceId'],
                    timeout=60000,
                    maxJobsToActivate=32
                )
            )

            for job_response in activate_jobs_response:
                for job in job_response.jobs:
                    job_data = {
                        "jobKey": job.key,
                        "variables": job.variables,
                    }

                    try:
                        zeebe_client.CompleteJob(
                            gateway_pb2.CompleteJobRequest(jobKey=job.key, variables=json.dumps({})))
                    except Exception as complete_error:
                        print(f"Error completing job {job.key}: {type(complete_error).__name__}")

                    print(f"{worker_type} data:", job_data)

        except Exception as error:
            print(f"An error occurred for {worker_type} worker:", type(error).__name__)

        await asyncio.sleep(5)  # Sleep for 5 seconds before polling again


async def run_workers():
    await asyncio.gather(
        start_and_complete_task("foo"),
        start_and_complete_task("bar")
    )


asyncio.run(run_workers())
