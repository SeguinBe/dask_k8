from kubernetes import client as kube_client, config
import yaml
from dask.distributed import Client
from time import sleep
from . import default_configs


class DaskCluster:
    def __init__(self, namespace: str, cluster_id: str, scheduler_pod_spec=None, worker_pod_spec=None):
        """
        A Dask cluster in kubernetes.
        :param namespace: Kubernetes namespace to be used
        :param cluster_id: unique string used to distinguish between the clusters of different users inside the same
        namespace. Something based on a username is a good idea.
        :param scheduler_pod_spec: a YAML pod specification for the scheduler (see default_configs.py)
        :param worker_pod_spec: a YAML pod specification for the worker (see default_configs.py)
        """
        self.namespace = namespace
        self.cluster_id = cluster_id
        self.scheduler_pod_spec = scheduler_pod_spec or default_configs.scheduler_pod_spec_yaml
        self.worker_pod_spec = worker_pod_spec or default_configs.worker_pod_spec_yaml
        # Configs can be set in Configuration class directly or using helper utility
        config.load_kube_config()
        self._initialized = False
        self._scheduler = None
        self._dashboard = None
        self._client = None

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def create(self):
        """
        Creates the services, the scheduler deployment and the workers deployment.
        WARNING: does not currently check if the cluster already exists...
        """
        # Apis handlers
        v1 = kube_client.CoreV1Api()
        apps_v1 = kube_client.AppsV1Api()

        # Services to be accessed outside of the cluster
        service_scheduler = kube_client.V1Service(api_version="v1", kind="Service",
                                                  metadata=kube_client.V1ObjectMeta(name=self.name_scheduler_service),
                                                  spec=kube_client.V1ServiceSpec(type="NodePort",
                                                                                 ports=[kube_client.V1ServicePort(
                                                                                     port=8786)],
                                                                                 selector={"app": "dask-scheduler",
                                                                                           "user": self.cluster_id}
                                                                                 ))
        service_scheduler_dashboard = kube_client.V1Service(api_version="v1", kind="Service",
                                                            metadata=kube_client.V1ObjectMeta(
                                                                name=self.name_scheduler_dashboard_service),
                                                            spec=kube_client.V1ServiceSpec(type="NodePort", ports=[
                                                                kube_client.V1ServicePort(port=8787)],
                                                                                           selector={
                                                                                               "app": "dask-scheduler",
                                                                                               "user": self.cluster_id}
                                                                                           ))
        # Start the services
        service_scheduler_created = v1.create_namespaced_service(self.namespace, service_scheduler, pretty=True)
        service_scheduler_dashboard_created = v1.create_namespaced_service(self.namespace, service_scheduler_dashboard,
                                                                           pretty=True)
        dask_scheduler_ip_port_internal = f"{service_scheduler_created.spec.cluster_ip}:{service_scheduler_created.spec.ports[0].port}"
        dask_scheduler_external_port = service_scheduler_created.spec.ports[0].node_port
        dask_scheduler_dashboard_external_port = service_scheduler_dashboard_created.spec.ports[0].node_port

        # Deployments #
        scheduler_pod_spec = yaml.safe_load(self.scheduler_pod_spec)
        worker_pod_spec = yaml.safe_load(self.worker_pod_spec)

        # scheduler deployment
        labels = {"app": "dask-scheduler", "user": self.cluster_id}
        template = kube_client.V1PodTemplateSpec(
            metadata=kube_client.V1ObjectMeta(labels=labels),
            spec=kube_client.V1PodSpec(**scheduler_pod_spec))

        scheduler_deployment = kube_client.V1Deployment(api_version="apps/v1",
                                                        metadata=kube_client.V1ObjectMeta(
                                                            name=self.name_scheduler_deployment),
                                                        spec=kube_client.V1DeploymentSpec(replicas=1,
                                                                                          selector={
                                                                                              "matchLabels": labels},
                                                                                          template=template))

        # worker deployment
        labels = {"app": "dask-workers", "user": self.cluster_id}
        worker_pod_spec['containers'][0]['env'].append(kube_client.V1EnvVar(name="DASK_SCHEDULER_ADDRESS",
                                                                            value=dask_scheduler_ip_port_internal))
        template = kube_client.V1PodTemplateSpec(
            metadata=kube_client.V1ObjectMeta(labels=labels),
            spec=kube_client.V1PodSpec(**worker_pod_spec))

        worker_deployment = kube_client.V1Deployment(api_version="apps/v1",
                                                     metadata=kube_client.V1ObjectMeta(
                                                         name=self.name_workers_deployment),
                                                     spec=kube_client.V1DeploymentSpec(replicas=0,
                                                                                       selector={"matchLabels": labels},
                                                                                       template=template))

        # Starts the deployments
        apps_v1.create_namespaced_deployment(self.namespace, scheduler_deployment, pretty=True)
        apps_v1.create_namespaced_deployment(self.namespace, worker_deployment, pretty=True)

        # Get the host IP of the scheduler
        v1 = kube_client.CoreV1Api()
        while True:
            dask_scheduler_external_ip = v1.list_namespaced_pod(self.namespace,
                                                                label_selector=f"user={self.cluster_id},app=dask-scheduler"
                                                                ).items[0].status.host_ip
            if dask_scheduler_external_ip is not None:
                break
            sleep(2)

        self._initialized = True
        self._scheduler = f"tcp://{dask_scheduler_external_ip}:{dask_scheduler_external_port}"
        self._dashboard = f"http://{dask_scheduler_external_ip}:{dask_scheduler_dashboard_external_port}"

        print(f"Scheduler: {self._scheduler}")
        print(f"Dashboard: {self._dashboard}")

    def close(self):
        """
        Release the kubernetes ressources (services, deployments, pods, etc...) associated with the
        `namespace` and `cluster_id`.
        """
        # Delete services
        v1 = kube_client.CoreV1Api()
        try:
            v1.delete_namespaced_service(self.name_scheduler_service, self.namespace)
        except Exception as e:
            pass
        try:
            v1.delete_namespaced_service(self.name_scheduler_dashboard_service, self.namespace)
        except Exception as e:
            pass
        # Delete deployments
        apps_v1 = kube_client.AppsV1Api()
        try:
            apps_v1.delete_namespaced_deployment(self.name_scheduler_service, self.namespace)
        except Exception as e:
            pass
        try:
            apps_v1.delete_namespaced_deployment(self.name_workers_deployment, self.namespace)
        except Exception as e:
            pass

        self._initialized = False
        self._client = None

    def make_dask_client(self, timeout=10, retries=20) -> Client:
        """
        Creates the Dask client connected to the scheduler running in kubernetes. Will try several times in case the
        scheduler takes time to boot up.
        :param timeout:
        :param retries:
        :return: The Dask client
        """
        if not self._initialized:
            raise ValueError("Cluster is not initalized")
        if self._client is None:
            for _ in range(retries):
                try:
                    self._client = Client(self._scheduler, timeout=timeout)
                except (OSError, TimeoutError):
                    print("Could not connect to scheduler, retrying...")
        return self._client

    @property
    def name_scheduler_service(self) -> str:
        return f"dask-scheduler-{self.cluster_id}"

    @property
    def name_scheduler_dashboard_service(self) -> str:
        return f"dask-scheduler-{self.cluster_id}-dashboard"

    @property
    def name_scheduler_deployment(self) -> str:
        return self.name_scheduler_service

    @property
    def name_workers_deployment(self) -> str:
        return f"dask-workers-{self.cluster_id}"

    def scale(self, n: int, blocking=True):
        """
        Scales the number of workers to a desired number
        :param n: number of workers to reach
        :param blocking: Blocks until the desired number of workers is reached and connected with the client (default: True)
        """
        if not self._initialized:
            raise ValueError("Cluster is not initalized")
        body = {'spec': {'replicas': n}}
        apps_v1 = kube_client.AppsV1Api()
        apps_v1.patch_namespaced_deployment(self.name_workers_deployment, self.namespace, body)
        if blocking:
            client = self.make_dask_client()
            while True:
                sleep(5)
                n_workers = len(client.scheduler_info()["workers"])
                if n_workers == n:
                    print(f"Reached the desired {n_workers} workers!")
                    break
                else:
                    print(f"Currently {n_workers} workers out of the {n} required, waiting...")
