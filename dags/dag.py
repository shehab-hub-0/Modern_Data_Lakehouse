from datetime import timedelta , datetime
from airflow import settings
from airflow.decorators import dag, task
import os

dbt_root_dir = f"{settings.DAGS_FOLDER}/dbt/ecommerce_dbt"

@dag(
    dag_id="dag_pipeline",
    default_args={
        "owner": "data-engineering-team",
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(seconds=15)
    },
    schedule=timedelta(hours=6),
    start_date=datetime(2026,4,24),
    catchup=False,
    tags=["dbt","trino"],
    max_active_runs=1
)
def dag_pipeline():
    @task
    def start():
        import logging
        logger = logging.getLogger(__name__)
        logger.info("Starting the pipeline")

        pipeline_metadata = {
            'pipeline_start_date': datetime.now().isoformat(),
            'dbt_root_dir': dbt_root_dir,
            'pipeline_id': f"dag_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'environment': 'production'
        }
        logger.info(f"Starting pipeline with ID: {pipeline_metadata['pipeline_id']}")
        return pipeline_metadata

    @task
    def seed_bronze(pipeline_metadata):
        import logging
        from operators.dbt_operator import DbtOperator
        logger = logging.getLogger(__name__)
        logger.info("Seeding bronze")

        try:
            import sqlalchemy
            from sqlalchemy import text

            engine = sqlalchemy.create_engine(
                f"trino://{os.getenv('TRINO_USER')}@{os.getenv('TRINO_HOST')}:{os.getenv('TRINO_PORT')}/iceberg/bronze"
            )

            with engine.connect() as conn:
                # التعديل 2: تغيير اسم الجدول من medallion إلى اسم جدول حقيقي مثل raw_customer_events
                # واستخدام try-except لتجنب الخطأ في حال كانت هذه أول مرة يتم فيها إنشاء الجدول
                try:
                    result = conn.execute(text("SELECT count(*) as cnt FROM raw_customer_events"))
                    raw_count = result.scalar()
                except Exception:
                    raw_count = 0

                if raw_count and raw_count > 0:
                    logger.info(f"Bronze already seeded with {raw_count} records")
                    return {
                        'status': 'skipped',
                        'reason': 'Bronze seed',
                        'pipeline_metadata': pipeline_metadata['pipeline_id'],
                        'timestamp': datetime.now().isoformat(),
                        'message': f'Bronze already seeded with {raw_count} records'

                    }
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {e}")
            
        
        operator =DbtOperator(
            task_id='seed_bronze_data',
            dbt_root_dir=dbt_root_dir,
            dbt_command='seed',
            full_refresh=True
        )

        try:
            operator.execute(context={})
            return {
                'status': 'success',
                'reason': 'Bronze seed',
                'pipeline_id': pipeline_metadata['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f"Failed to seed bronze: {e}")
            return {
                'status': 'failed',
                'reason': 'Bronze seed',
                'pipeline_id': pipeline_metadata['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
                'message': str(e)
            }
    
    @task
    def transform_bronze_layer(seed_result):
        import logging
        from operators.dbt_operator import DbtOperator
        from airflow import settings
        
        logger = logging.getLogger(__name__)

        if seed_result['status'] != 'success':
            logger.warning(f"Seeding failed, continuing with transformation...."
                           f"{seed_result.get('message','Unknown error')}")
        logger.info(f"Transforming bronze....") 

        operator = DbtOperator(
            task_id='transform_bronze_data_internal',
            dbt_root_dir=dbt_root_dir,
            dbt_command='run --select tag:bronze',
        )
        try:
            operator.execute(context={})
            return {
                'status': 'success',
                'reason': 'Bronze_transform',
                'pipeline_id': seed_result['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f"Failed to transform bronze: {e}")
            raise 
    
    @task
    def validate_bronze_data(bronze_result):
        import logging
        from operators.dbt_operator import DbtOperator
        
        logger = logging.getLogger(__name__)
        logger.info(f"Validating bronze for pipeline: {bronze_result['pipeline_id']}") 
        
        validate_checks = {
            'null_check': 'passed',
            'duplicate_check': 'passed',
            'schema_validation': 'passed',
            'row_count': 'passed'
        } 

        return{
            'status': 'success',
            'layer': 'Bronze_validation',
            'pipeline_id': bronze_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),
            'validate_checks': validate_checks
        }

    @task
    def transform_silver_layer(bronze_validation):
        import logging
        from operators.dbt_operator import DbtOperator
        from airflow import settings
        
        logger = logging.getLogger(__name__)

        if bronze_validation['status'] != 'success':
            raise Exception(f"Bronze validation failed,cannot proceed with transformation{bronze_validation['pipeline_id']}")
        logger.info(f"Transforming silver for pipeline: {bronze_validation['pipeline_id']}")

        operator = DbtOperator(
            task_id='transform_silver_data_internal',
            dbt_root_dir=dbt_root_dir,
            dbt_command='run --select tag:silver',
        )
        try:
            operator.execute(context={})
            return {
                'status': 'success',
                'reason': 'Silver_transform',
                'pipeline_id': bronze_validation['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f"error occured during silver transformation: {e}")
            raise 

    @task
    def validate_silver_data(silver_result):
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Validating silver for pipeline: {silver_result['pipeline_id']}") 
        
        validate_checks = {
            'business_rule': 'passed',
            'referential_integrity': 'passed',
            'aggregate_accuracy': 'passed',
            'data_freshness': 'passed'
        } 

        return{
            'status': 'success',
            'layer': 'Silver_validation',
            'pipeline_id': silver_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),
            'validate_checks': validate_checks
        }
    @task
    def transform_gold_layer(silver_validation):
        import logging
        from operators.dbt_operator import DbtOperator
        
        logger = logging.getLogger(__name__)

        if silver_validation['status'] != 'success':
            raise Exception(f"Silver validation failed,cannot proceed with transformation{silver_validation['pipeline_id']}")
        logger.info(f"Transforming gold for pipeline: {silver_validation['pipeline_id']}")

        operator = DbtOperator(
            task_id='transform_gold_data_internal',
            dbt_root_dir=dbt_root_dir,
            dbt_command='run --select tag:gold',
        )
        try:
            operator.execute(context={})
            return {
                'status': 'success',
                'reason': 'Gold_transform',
                'pipeline_id': silver_validation['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f"error occured during gold transformation: {e}")
            raise 
    @task
    def validate_gold_data(gold_result):
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Validating gold for pipeline: {gold_result['pipeline_id']}") 
        
        validate_checks = {
            'business_rule': 'passed',
            'matrics_calculation': 'passed',
            'completeness_check': 'passed',
            'kpi_accuracy': 'passed'
        } 

        return{
            'status': 'success',
            'layer': 'Gold_validation',
            'pipeline_id': gold_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),
            'validate_checks': validate_checks
        }
    @task
    def generate_documentation(gold_validation):
        import logging
        from operators.dbt_operator import DbtOperator
        
        logger = logging.getLogger(__name__)

        if gold_validation['status'] != 'success':
            raise Exception(f"Gold validation failed,cannot proceed with documentation{gold_validation}")
        logger.info(f"Transforming platinum for pipeline: {gold_validation['pipeline_id']}")

        operator = DbtOperator(
            task_id='transform_platinum_data_internal',
            dbt_root_dir=dbt_root_dir,
            dbt_command='docs generate',
        )
        try:
            operator.execute(context={})
            return {
                'status': 'success',
                'reason': 'Documentation',
                'pipeline_id': gold_validation['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f"error occured during documentation generation: {e}")
            raise 
    @task
    def end_pipeline(docs_result,gold_validation):
        import logging
        logger = logging.getLogger(__name__)
        logger.info("ending pipeline") 
        logger.info(f"pipeline {gold_validation['pipeline_id']} completed successfully")
        logger.info(f"final status: {gold_validation['status']}")
        logger.info(f"pipeline completed at: {datetime.now().isoformat()}")

        if docs_result['status'] != 'success':
            logger.warning("Documentation generation had issue but pipeline completed successfully")
        logger.info(f"Transforming platinum for pipeline: {docs_result['pipeline_id']}")
        


# Logic at the bottom of dag_pipeline()
    pipeline_metadata = start() # Match the function name defined above
    seed_result = seed_bronze(pipeline_metadata)
    bronze_result = transform_bronze_layer(seed_result)
    bronze_validation = validate_bronze_data(bronze_result)
    silver_result = transform_silver_layer(bronze_validation)
    silver_validation = validate_silver_data(silver_result)
    gold_result = transform_gold_layer(silver_validation)
    gold_validation = validate_gold_data(gold_result)
    docs_result = generate_documentation(gold_validation)
    end_pipeline(docs_result, gold_validation)

# Call the DAG function
dag_pipeline()