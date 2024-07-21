# Usa una imagen base de Python
FROM python:3.9-slim

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos necesarios
COPY requirements.txt ./
COPY etl_pipeline.py ./

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Ejecuta el script
CMD ["python", "etl_pipeline.py"]

