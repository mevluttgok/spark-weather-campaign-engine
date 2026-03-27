FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    streamlit==1.28.0 \
    pandas==2.1.1 \
    plotly==5.17.0 \
    kafka-python==2.0.2 \
    pyarrow==14.0.0 \
    fastparquet==2023.10.1

COPY app.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
