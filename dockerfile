FROM python:3.12

WORKDIR /hello_world_api

# Install necessary dependencies
RUN apt-get update && \
    apt-get install -y \
    curl \
    apt-transport-https \
    gnupg \
    ca-certificates && \
    # Add Microsoft's package signing key
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    # Add the Microsoft repository
    curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    # Update package list and install ODBC driver
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc 

COPY requirements.txt .
COPY . .
RUN pip install -r requirements.txt

CMD ["python", "./extract_clips_data.py"]
