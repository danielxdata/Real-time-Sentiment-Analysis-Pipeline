import concurrent.futures
import subprocess
from pathlib import Path


chemins_scripts = []
scripts = ['producer/producer.py', 'consumer/consumer_classifier.py', 'consumer/consumer_postgres.py']
for script in scripts :
     chemins_scripts.append(str(Path(script).resolve()))

def run_script(script_name):
    # Lance chaque script de manière indépendante
    result = subprocess.run(['python3', script_name])
    return result.stdout

# Exécute les scripts en parallèle
with concurrent.futures.ProcessPoolExecutor() as executor:
    results = executor.map(run_script, chemins_scripts)
    
