FROM prefecthq/prefect:2.7.7-python3.9

COPY requirements.txt .

RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY parameterized_flow.py /opt/prefect/flows/parameterized_flow.py
COPY data /opt/prefect/data




