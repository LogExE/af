
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from kubernetes.client import models as k8s

def test():
  import os
  return os.system("nvidia-smi")

with DAG(
   dag_id="dagger",
   start_date=datetime.datetime(2024, 11, 5),
   schedule="@once",
):
  task = PythonOperator(
          task_id="t_g",
          python_callable=test,
          provide_context=True,
          executor_config = {
            "pod_override": k8s.V1Pod(
              spec=k8s.V1PodSpec(
                containers=[
                  k8s.V1Container(
                    name="base",
                    image="ubuntu:24.04",
                    resources=k8s.V1ResourceRequirements(
                      limits={
                        "nvidia.com/gpu": "1"
                      }
                    )
                  )
                ],
                node_selector={
                  "nvidia.com/gpu.product": "NVIDIA-GeForce-GTX-1060-6GB"
                }
              ),
            )
        }
  )
