from airflow.sdk import BaseOperator
from airflow.exceptions import AirflowException
from dbt.cli.main import dbtRunner, dbtRunnerResult
import os
from airflow.utils.context import Context
from typing import Any
import json
from datetime import datetime

class DbtOperator(BaseOperator):
    def __init__(
        self,
        dbt_root_dir: str = None, 
        dbt_command: str = None,
        target: str = None,
        dbt_vars: dict = None,
        select: str = None,
        full_refresh: bool = None,    
        **kwargs
    ):
        super().__init__(**kwargs)
        self.dbt_root_dir = dbt_root_dir
        self.dbt_command = dbt_command
        self.target = target
        self.dbt_vars = dbt_vars
        self.select = select
        self.full_refresh = full_refresh
        self.runner = dbtRunner()

    def execute(self, context: Context) -> Any:
        # 1. التحقق من وجود المجلدات وصلاحيات الوصول
        if not os.path.exists(self.dbt_root_dir):
            raise AirflowException(f"DBT root directory not found: {self.dbt_root_dir}")
        
        logs_dir = os.path.join(self.dbt_root_dir, "logs")
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir, mode=0o777, exist_ok=True)
            self.log.info(f"Created logs directory: {logs_dir}")

        # 2. بناء أمر dbt بدقة
        command_parts = self.dbt_command.split() if isinstance(self.dbt_command, str) else [self.dbt_command]
        
        command_args = command_parts + [
            "--project-dir", self.dbt_root_dir,
            "--profiles-dir", self.dbt_root_dir,
        ]

        if self.target:
            command_args.extend(["--target", self.target])
        if self.select:
            command_args.extend(["--select", self.select])
        if self.full_refresh:
            command_args.extend(["--full-refresh"])
        if self.dbt_vars:
            command_args.extend(["--vars", json.dumps(self.dbt_vars)])
        
        self.log.info(f"Executing dbt command: {' '.join(command_args)}")
        
        # 3. تنفيذ الأمر باستخدام dbtRunner
        res: dbtRunnerResult = self.runner.invoke(command_args)

        # 4. معالجة النتائج وإرجاع مخرجات "قوية" للـ DAG
        if res.success:
            self.log.info("DBT command completed successfully")
            
            successful_models = []
            if hasattr(res, 'result') and res.result and hasattr(res.result, 'results'):
                try:
                    for result in res.result.results:
                        if hasattr(result, "status") and hasattr(result, "node"):
                            self.log.info(f"Model {result.node.name}: {result.status}")
                            if result.status == "success":
                                successful_models.append(result.node.name)
                except Exception as e:
                    self.log.warning(f"Could not parse detailed results: {e}")

            # الحل القوي: إرجاع قاموس (Dictionary) بدلاً من True
            return {
                'status': 'success',
                'pipeline_id': f"dbt_{self.dbt_command}_{self.task_id}",
                'execution_time': datetime.now().isoformat(),
                'models_count': len(successful_models),
                'successful_models': successful_models
            }
        else:
            self.log.error("DBT command failed")
            # في حالة الفشل نرفع استثناء لإيقاف Airflow
            raise AirflowException(f"DBT command failed: {' '.join(command_args)}")