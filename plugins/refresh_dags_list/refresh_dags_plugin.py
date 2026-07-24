import os
import subprocess
from flask import redirect, flash
from flask_appbuilder import BaseView, expose
from airflow.plugins_manager import AirflowPlugin
from airflow.models import DagBag
from airflow.utils.session import provide_session


class RefreshDagsView(BaseView):
    """View that executes 'airflow dags list' command and refreshes the DAG bag"""
    
    default_view = "refresh_dags"
    route_base = "/refresh_dags"

    @expose("/")
    def refresh_dags(self):
        """Execute 'airflow dags list' command and refresh DAG bag, then redirect to home"""
        try:
            # Execute the command
            result = self._execute_dags_list()
            
            # Force refresh of the DAG bag
            self._refresh_dagbag()
            
            # Show success message and redirect to home
            if result.get('success', False):
                flash("DAGs list refreshed successfully!", "success")
            else:
                flash(f"DAGs list refresh completed with warnings: {result.get('error', 'Unknown error')}", "warning")
            
            return redirect('/home')
        except Exception as e:
            flash(f"Error refreshing DAGs list: {str(e)}", "error")
            return redirect('/home')

    def _execute_dags_list(self):
        """Execute 'airflow dags list' command and return the output"""
        try:
            # Get AIRFLOW_HOME from environment
            airflow_home = os.environ.get('AIRFLOW_HOME', '/home/gamma/airflow')
            
            # Execute the command
            result = subprocess.run(
                ['airflow', 'dags', 'list'],
                capture_output=True,
                text=True,
                timeout=30,
                env={**os.environ, 'AIRFLOW_HOME': airflow_home}
            )
            
            return {
                'success': result.returncode == 0,
                'output': result.stdout,
                'error': result.stderr
            }
        except subprocess.TimeoutExpired:
            return {
                'success': False,
                'output': '',
                'error': 'Timeout: command took too long'
            }
        except Exception as e:
            return {
                'success': False,
                'output': '',
                'error': f'Error during execution: {str(e)}'
            }

    @provide_session
    def _refresh_dagbag(self, session=None):
        """Force refresh of the DAG bag to update the list in the UI"""
        try:
            # Get the DAGs folder path
            dag_folder = os.environ.get(
                'AIRFLOW__CORE__DAGS_FOLDER',
                os.path.join(os.environ.get('AIRFLOW_HOME', '/home/gamma/airflow'), 'dags')
            )
            
            # Create a new DagBag instance to force refresh
            dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
            
            # Force parsing of DAGs
            dagbag.collect_dags()
            
            # Sync with database
            dagbag.sync_to_db()
            
            return True
        except Exception as e:
            # Log the error but don't block execution
            print(f"Warning: Error during DAG bag refresh: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


class RefreshDagsPlugin(AirflowPlugin):
    name = "refresh_dags_plugin"
    
    appbuilder_views = [
        {
            "name": "Refresh DAGs List",
            "category": "Develop Tools",
            "view": RefreshDagsView()
        }
    ]
