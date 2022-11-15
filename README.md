# Comparar esquemas entre bases de datos

Cuando se tienen diferentes bases de datos de una misma aplicación, como por ejemplo la base de datos de desarrollo, pruebas, pre-producción y producción, se pueden presentar diferencias en los esquemas de las bases de datos, como por ejemplo tablas, columnas de tablas, vistas, procedimientos entre otros datos.

Este proyecto permite generar archivos .csv de la base de datos indicada en el código, para luego crear las tablas con una estructura predefinida donde se insertará dicha información, una vez con la información cargada en la base de datos, de preferencia local, se pueden construir las consultas necesarias para comparar la información y determinar que tan diferentes se encuentran los esquemas de las bases de datos.

## Instalación

Luego de crear tu propio entorno virtual puedes instalar las librerias necesarias con el comando:

```[bash]
pip install -r requirements.txt
```

De preferencia los archivos .csv se cargaran a una base de datos local, para lo cual se requiere tener instalado Microsoft SQL Server Express y SQL Server Managment Studio.

El usuario de dominio o con el que este autenticado el computador debe tener permisos de consulta en las bases de datos de donde se van a generar las consultas con la información de tablas, procedimiento, columnas entre otro, y permisos de crear, borrar datos y de "bulk admin" sobre la base de datos donde se cargará la información.

## Modo de uso

En el archivo **main.py** se encuentra la configuración necesaria para ejecutar el proyecto:

1. Se requiere conocer el nombre y servidor de las bases de datos que se van consultar los esquemas y generar los archivos .csv, adicionalmente el servidor donde se crearan las tablas para cargar los archivos,esta información se debe asignar a las variables de "database_name", "server_name", "local_database_name" y "local_server_name".

2. Por cada servidor y base de datos a la que se va a consultar información se requieren 3 funciones "for_create_csv", "for_create_or_truncate_tables" y "for_bulk_insert_csv".

3. Una vez con los datos necesarios ejecutar el archivo.
