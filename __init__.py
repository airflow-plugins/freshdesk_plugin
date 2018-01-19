from airflow.plugins_manager import AirflowPlugin
from freshdesk_plugin.operators.freshdesk_to_s3_operator import FreshdeskToS3Operator


class freshdesk_plugin(AirflowPlugin):
    name = "freskdesk_plugin"
    operators = [FreshdeskToS3Operator]
    hooks = []
    # Leave in for explicitness
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
