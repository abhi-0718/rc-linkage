FROM python:3.8.10

#CREATING working directory for application code 
WORKDIR /app

#COPYING all file from local machine to Docker Image 
COPY . /app

#UPGRADING pip
# RUN pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org --upgrade pip

#Installing all the dependencies from req.txt

RUN pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org -r requirement.txt

#Installing mysql in container
RUN su
RUN apt-get update \
    && apt-get install -y default-mysql-server
RUN service mysql start
# RUN mysqld_safe --skip-grant-tables
#This component listen on port 8000
EXPOSE 8000 8501 8502 3306

CMD ["python","database_docker.py"]
CMD ["streamlit","run","UI.py"]