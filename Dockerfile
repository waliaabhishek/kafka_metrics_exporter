FROM python:3.7

WORKDIR /usr/src/app

COPY Code/requirements.txt .
COPY Code/*.py ./
COPY ELK/jmx_dashboard.json ./scripts/dashboard/jmx_dashboard.json
RUN apt-get -y install gcc
RUN pip install --no-cache-dir -r ./requirements.txt

ENTRYPOINT [ "python", "main.py" ]