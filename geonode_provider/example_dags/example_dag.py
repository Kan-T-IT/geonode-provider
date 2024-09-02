from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from geonode_provider.hooks.geonode import GeoNodeHook


default_args = {
    'owner': 'KAN',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': False,
    'email_on_retry': False,
}

geonode_hook = GeoNodeHook(geonode_conn_id="geonode_connection_id_airflow") # Instance GeoNodeHook

with DAG("GeonodeHook-ExampleDAG", default_args=default_args, schedule_interval=None, tags=['Airflow', 'GeoNode']) as dag:

    start = EmptyOperator(
        task_id='start',
        dag=dag
    )

    import_layer_task = PythonOperator(
        task_id='import_layer_task',
        python_callable=geonode_hook.import_layer,
        op_args=[
            "path/to/you/datasets/directory"
        ],
        provide_context=True,
        dag=dag
    )

    publish_layer_task = PythonOperator(
    task_id='publish_layer_task',
    python_callable=geonode_hook.publish_layer,
    op_args=[
        "your_dataset_name"
    ],
    provide_context=True,
    dag=dag
    )

    update_layer_task = PythonOperator(
        task_id='update_layer_task',
        python_callable=geonode_hook.update_layer,
        op_args=[
            "your_dataset_name"
        ],
        provide_context=True,
        dag=dag
    )

    upload_dataset_api = PythonOperator(
        task_id='upload_dataset_api',
        python_callable=geonode_hook.upload_dataset,
        op_args={
            "path/to/you/datasets/directory/or/file"
        },
        provide_context=True,
        dag=dag
    )

    upload_style_task = PythonOperator(
    task_id='upload_style_task',
    python_callable=geonode_hook.upload_style,
    op_args=[
        "path/to/you/sld/style",
        "style_name" # Optional
    ],
    provide_context=True,
    dag=dag
    )

    update_style_task = PythonOperator(
        task_id='update_style_task',
        python_callable=geonode_hook.update_style,
        op_args=[
            "path/to/you/sld/style",
            "style_name" # Optional
        ],
        provide_context=True,
        dag=dag
    )

    update_layer_style_task = PythonOperator(
    task_id='update_layer_style_task',
    python_callable=geonode_hook.update_layer_style,
    op_args=[
        "your_dataset_name",
        "your_style_name.sld"
    ],
    provide_context=True,
    dag=dag
    )

    upload_map = PythonOperator(
        task_id='upload_map',
        python_callable=geonode_hook.upload_map,
        op_args=[
            [dataset_1_id, dataset_2_id, dataset_3_id, etc],
            "Map Title"
        ],
        provide_context=True,
        dag=dag
    )

    upload_document_task = PythonOperator(
        task_id='upload_document_task',
        python_callable=geonode_hook.upload_document,
        op_args=[
            "path/to/your/document"
        ],
        provide_context=True,
        dag=dag
    )

    upload_metadata_task = PythonOperator(
    task_id='upload_metadata_task',
    python_callable=geonode_hook.upload_metadata,
    op_args=[
        "your_resource_type",
        your_resource_id,
        "Resource Title",
        "Resource supplemental information"
    ],
    provide_context=True,
    dag=dag
    )

    set_permiss_task = PythonOperator(
        task_id='set_permiss_task',
        python_callable=geonode_hook.set_permiss,
        op_args=[
            your_resource_id,
            {
                "users": [
                    {
                        "id": <user_id>,
                        "permissions": "none"
                    },
                    {
                        "id": <user_id>,
                        "permissions": "view"
                    },
                    {
                        "id": <user_id>,
                        "permissions": "download"
                    },
                    {
                        "id": <user_id>,
                        "permissions": "edit"
                    },
                    {
                        "id": <user_id>,
                        "permissions": "manage"
                    },
                    {
                        "id": <user_id>,
                        "permissions": "owner"
                    }
                ],
                "groups": [
                    {
                        "id": <group_id>,
                        "permissions": "none"
                    },
                    {
                        "id": <group_id>,
                        "permissions": "none"
                    }
                ],
                "organizations": [
                    {
                    "id": <organization_id>,
                    "permissions": "edit"
                    }
                ]
            }
        ],
        provide_context=True,
        dag=dag
    )

    get_execution_information_task = PythonOperator(
    task_id='get_execution_information_task',
    python_callable=geonode_hook._get_execution_information,
    op_args=[
        "execution_id"
    ],
    provide_context=True,
    dag=dag
    )

    end = EmptyOperator(
        task_id='end',
        dag=dag
    )


(
start
>> import_layer_task
>> publish_layer_task
>> update_layer_task
>> upload_dataset_api
>> upload_style_task
>> update_layer_style_task
>> upload_map
>> upload_document_task
>> upload_metadata_task
>> set_permiss_task
>> get_execution_information_task
>> end
)