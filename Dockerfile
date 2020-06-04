FROM python:3.7-alpine

WORKDIR /usr/src/app

COPY Code/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY Code/*.py .

CMD [ "python", "./main.py" ]