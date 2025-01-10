FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN if [ "${ENVIRONMENT}" = "development" ]; then pip install --no-cache-dir -r requirements-dev.txt; fi

# Copy application code
COPY src/ src/
COPY migrations/ migrations/

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the monitor
CMD ["python", "-m", "src.monitor"]
