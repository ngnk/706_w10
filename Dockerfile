# Dockerfile
FROM apache/airflow:2.8.1-python3.10

# System deps (Java 17 for PySpark; curl for healthcheck)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

# Stable JAVA_HOME that works across archs
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/java-17-openjdk || true
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="$JAVA_HOME/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    matplotlib \
    scikit-learn \
    psycopg2-binary \
    SQLAlchemy \
    pyspark
