from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import os
import subprocess
import platform
import logging
from pathlib import Path
from airflow.utils.trigger_rule import TriggerRule


logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

def create_spark_directories():
    """Create required directories for Spark jobs"""
    #base_path = "/opt/airflow/data"  # Linux path for containers
    
    # For Windows development
    # Get the directory where this DAG file is located
    current_file = os.path.abspath(__file__)
    task_folder = os.path.dirname(current_file)

    # Go up one level to get airflow root
    dags_folder= os.path.dirname(os.path.dirname(task_folder)) # This assumes the DAG file is two levels deep in the airflow structure (folder daily-market-data-pipeline/tasks)
    airflow_root = os.path.dirname(dags_folder)
    data_path = os.path.join(airflow_root, "data")
    
    logger.info(f'Current task file: {current_file}')
    logger.info(f'Current task folder: {task_folder}')
    logger.info(f'Current dag folder: {dags_folder}')
    logger.info(f'Current airflow folder: {airflow_root}')
    logger.info(f'Data Path: {data_path}')


    directories = [
        os.path.join(data_path, "processed", "crypto"),
        os.path.join(data_path, "checkpoints", "crypto"),
        os.path.join(data_path, "processed", "stock"),
        os.path.join(data_path, "checkpoints", "stock")
    ]
    
# The following block is commented out and replaced by the code below:
# for directory in directories:
#     os.makedirs(directory, exist_ok=True)
#     logger.info(f"Created directory: {directory}")
#
# # Set permissions (platform-specific)
# if platform.system() == "Windows":
#     # Windows permissions
#     subprocess.run([
#         "icacls", data_path, 
#         "/grant", "Everyone:F", 
#         "/T"
#     ], check=True)
# else:
#     # Linux permissions
#     subprocess.run([
#         "chmod", "-R", "755", data_path
#     ], check=True)

    for directory in directories:
            path_obj = Path(directory)
            
            if path_obj.exists():
                if path_obj.is_dir():
                    print(f"✓ Directory exists: {directory}")
                    
                    # Check permissions
                    if os.access(directory, os.R_OK | os.W_OK):
                        print(f"✓ Directory has read/write access: {directory}")
                    else:
                        print(f"⚠ Directory exists but insufficient permissions: {directory}")
                        # Try to fix permissions
                        _fix_permissions(directory)
                        
                else:
                    print(f"✗ Path exists but is not a directory: {directory}")
                    raise ValueError(f"Path exists but is not a directory: {directory}")
            else:
                try:
                    path_obj.mkdir(parents=True, exist_ok=True)
                    print(f"✓ Created directory: {directory}")
                    
                    # Verify creation
                    if path_obj.exists():
                        print(f"✓ Verified directory creation: {directory}")
                    else:
                        raise Exception(f"Directory creation failed: {directory}")
                        
                except Exception as e:
                    print(f"✗ Failed to create directory {directory}: {e}")
                    raise
    
    # Set permissions on the entire data structure
    _fix_permissions(data_path)
    print("✅ Directory setup completed successfully")

def _fix_permissions(path):
    """Fix permissions for the given path"""
    try:
        if platform.system() == "Windows":
            result = subprocess.run([
                "icacls", path, 
                "/grant", "Everyone:F", 
                "/T"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✓ Windows permissions set for: {path}")
            else:
                print(f"⚠ Windows permission warning: {result.stderr}")
        else:
            result = subprocess.run([
                "chmod", "-R", "755", path
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✓ Unix permissions set for: {path}")
            else:
                print(f"⚠ Unix permission warning: {result.stderr}")
                
    except Exception as e:
        print(f"⚠ Could not set permissions for {path}: {e}")

# Bash-based directory creation (recommended for permission issues)
setup_directories_bash = BashOperator(
    task_id='setup_directories_bash',
    bash_command="""
    # Create directories in /opt/airflow/data
    DATA_PATH="/opt/airflow/data"
    
    echo "Creating directory structure in $DATA_PATH"
    
    # Create directories with verbose output
    mkdir -p -v "$DATA_PATH/processed/crypto" || { echo "Failed to create crypto processed dir"; exit 1; }
    mkdir -p -v "$DATA_PATH/checkpoints/crypto" || { echo "Failed to create crypto checkpoints dir"; exit 1; }
    mkdir -p -v "$DATA_PATH/processed/stocks" || { echo "Failed to create stocks processed dir"; exit 1; }
    mkdir -p -v "$DATA_PATH/checkpoints/stocks" || { echo "Failed to create stocks checkpoints dir"; exit 1; }
    
    echo "✓ All directories created successfully"
    
    # Set permissions (ignore errors if can't set permissions)
    chmod -R 755 "$DATA_PATH" 2>/dev/null && echo "✓ Permissions set successfully" || echo "⚠ Warning: Could not set permissions (continuing anyway)"
    
    # Verify directory structure
    echo "📁 Directory structure:"
    ls -la "$DATA_PATH" 2>/dev/null || echo "Cannot list $DATA_PATH contents"
    ls -la "$DATA_PATH/processed" 2>/dev/null || echo "Cannot list processed directory"
    ls -la "$DATA_PATH/checkpoints" 2>/dev/null || echo "Cannot list checkpoints directory"
    
    echo "✅ Directory setup completed"
    """,
    trigger_rule=TriggerRule.ONE_FAILED
)



setup_directories_python = PythonOperator(
    task_id='setup_directories_python',
    python_callable=create_spark_directories,
    retries=0,
)

setup_complete = DummyOperator(
    task_id='setup_complete',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  # Run if at least one setup task succeeded
)