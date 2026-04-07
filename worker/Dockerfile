FROM bitnamilegacy/spark:3.5

USER root

RUN install_packages python3-pip netcat-openbsd procps
RUN pip3 install --no-cache-dir psutil pandas matplotlib

WORKDIR /workspace

COPY worker.py /workspace/worker.py
COPY spark.py /workspace/spark.py
COPY plot_results.py /workspace/plot_results.py

CMD ["python3", "/workspace/worker.py"]