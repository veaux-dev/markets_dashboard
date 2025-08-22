# Línea 1 - Imagen base
FROM python:3.11-slim

# Línea 2 - Directorio de trabajo
WORKDIR /app

# Línea 3 - Copiar requirements primero (para cache de Docker)
COPY requirements.txt .

# Línea 4 - Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Línea 5 - Copiar todo el código fuente
COPY . .

# Línea 6 - Crear el directorio data dentro del container
RUN mkdir -p /app/data

# Línea 7 - Configurar timezone (importante para tus mercados MX/US)
ENV TZ=America/Mexico_City

# Línea 8 - Crear mount point para el dataset de TrueNAS
VOLUME ["/app/data"]

# Línea 9 - Comando por defecto
CMD ["python", "00_mkt_db.py"]