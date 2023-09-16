FROM python:3.8.18

RUN pip install pandas==1.2.4 sqlalchemy==1.4.48 psycopg2==2.9.6 lxml==4.6.3 boto3==1.28.49

COPY ./read_and_process_messages.py ./

ENTRYPOINT [ "python", "read_and_process_messages.py" ]