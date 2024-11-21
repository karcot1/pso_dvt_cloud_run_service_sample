FROM python:3.9-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /appgi
WORKDIR $APP_HOME

COPY execute_dvt_partitions/execute_dvt_partition_yamls.sh ./
COPY execute_dvt_partitions/partitions_main.py ./ 
COPY execute_dvt_partitions/partition_connections.sh ./

# Install production dependencies.
RUN apt-get update \
    && apt-get install gcc -y \
    && apt-get clean
RUN pip install --upgrade pip
RUN pip install --upgrade google-auth
RUN pip install oauth2client
RUN pip install Flask gunicorn
RUN pip install --upgrade google-pso-data-validator

# Install google cloud SDK
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && \
    apt-get install google-cloud-sdk -y

# Teradata Dependencies
RUN pip install teradatasql

# Hive/Impala Dependencies 
# RUN pip install hdfs
# RUN pip install thrift-sasl

# Oracle Dependencies
# if you are using Oracle you should add .rpm files
# under your license to a directory called oracle/
# and then uncomment the setup below.

# ENV ORACLE_SID oracle
# ENV ORACLE_ODBC_VERSION 12.2
# ENV ORACLE_HOME /usr/lib/oracle/${ORACLE_ODBC_VERSION}/client64

# RUN pip install cx_Oracle
# RUN apt-get -y install --fix-missing --upgrade vim alien unixodbc-dev wget libaio1 libaio-dev

# COPY oracle/*.rpm ./
# RUN alien -i *.rpm && rm *.rpm \
#     && echo "/usr/lib/oracle/${ORACLE_ODBC_VERSION}/client64/lib/" > /etc/ld.so.conf.d/oracle.conf \
#     && ln -s /usr/include/oracle/${ORACLE_ODBC_VERSION}/client64 $ORACLE_HOME/include \
#     && ldconfig -v

# UNCOMMENT TO RUN AS SERVICE
# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
# CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 dvt_main:app

# UNCOMMENT TO RUN AS JOB
CMD ["python", "partitions_main.py"]