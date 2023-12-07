import asyncio
import json
import logging
import grpc
from zeebe_grpc import gateway_pb2, gateway_pb2_grpc


def get_gateway_stub():
    channel = grpc.insecure_channel("localhost:26500")
    stub = gateway_pb2_grpc.GatewayStub(channel)
    return stub


async def get_topology():
    client = get_gateway_stub()

    topology = client.Topology(gateway_pb2.TopologyRequest())
    print(topology)
    return topology


async def deploy_process_definition():
    client = get_gateway_stub()

    with open("bpmn/demoProcess.bpmn", "rb") as process_definition_file:
        process_definition = process_definition_file.read()
        process = gateway_pb2.ProcessRequestObject(
            name="demoProcess.bpmn",
            definition=process_definition
        )
    client.DeployProcess(
        gateway_pb2.DeployProcessRequest(
            processes=[process]
        )
    )


async def create_process_instance():
    client = get_gateway_stub()
    variables = {"instanceId": "id-12345"}

    process_instance = client.CreateProcessInstance(
        gateway_pb2.CreateProcessInstanceRequest(
            bpmnProcessId="demoProcess",
            version=-1,
            variables=json.dumps(variables)
        )
    )
    return process_instance


async def main():
    topology_result = await get_topology()
    print("Topology:", topology_result)

    await deploy_process_definition()
    print("Process definition deployed successfully.")

    process_instance_result = await create_process_instance()
    print("Process instance created:", process_instance_result)


if __name__ == "__main__":
    asyncio.run(main())
