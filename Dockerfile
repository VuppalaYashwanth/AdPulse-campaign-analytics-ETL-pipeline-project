FROM python:3.11-slim

WORKDIR /app

# psycopg2 needs libpq at build time
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Generate sample data on image build so a fresh `docker compose up`
# has something to run against immediately. Real usage would mount
# real source files into data/raw/ instead.
RUN python -m src.generate_sample_data

CMD ["python", "-m", "src.run_pipeline"]
