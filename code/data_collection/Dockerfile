FROM python:3.8.0

COPY requirements.txt /opt/app/requirements.txt
WORKDIR /opt/app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
 
COPY . /app
WORKDIR /app

CMD ["python","-u","main.py"]

