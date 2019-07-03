
scheduler_pod_spec_yaml = """
  containers:
    - image: daskdev/dask:1.2.0
      command: ["sh", "-c"]
      args:
        - dask-scheduler --port 8786 --bokeh-port 8787
      imagePullPolicy: Always
      name: dask-scheduler
      ports:
        - containerPort: 8787
        - containerPort: 8786
      resources:
        requests:
          cpu: 1
          memory: 4G
"""

worker_pod_spec_yaml = """
  containers:
    - image: daskdev/dask:1.2.0
      args: [dask-worker, $(DASK_SCHEDULER_ADDRESS), --nthreads, '1', --no-bokeh, --memory-limit, 4GB, --death-timeout, '60']
      imagePullPolicy: Always
      name: dask-worker
      env:
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: EXTRA_PIP_PACKAGES
          value: s3fs
        - name: EXTRA_CONDA_PACKAGES
          value: 
      resources:
        requests:
          cpu: 1
          memory: "4G"
        limits:
          cpu: 1
          memory: "4G"
"""
