#**TRABAJO FINAL SYNAPSE**

------------
**Autor:** Fausto Loja Mora
#### **CASO**
Dado el siguiente modelo analítico.
 
El departamento de Marketing, desea enviar una campaña personalizada a cada uno de los clientes, promocionando el producto que más veces han solicitado. Por lo tanto el requerimiento ingresa al departamento de Ingeniería Analítica solicitado que se genere una tabla, con las siguientes columnas:

- Codigo del cliente (rowidcliente)
- Nombre del producto mas comprado
- Fecha de la última compra
- Correo del cliente

Las tablas del modelo analítico proveen los datos transaccionales. Sin embargo los correos electrónicos han sido proporcionados por medio de un archivo CSV el cual se encuentra adjunto.

Lineamientos generales:

- El nombre de usuario debe ser la letra inicial del primer nombre y su apellido Ejm: Agustin Martinez (amartinez)
- Cada pipeline de ADF debe anteponer el nombre de usuario seguido de pipeline Ejm: amartinez_pipeline
- Los archivos ingestados en ADL2 deben colocarse en RAW en una carpeta con el nombre de usuario
- Los notebooks de Spark deben ser nombrados  {usuario}_notebook
- La tabla de resultados debe almacenarse en un POOL SQL, en el esquema default con el esquema "tbl_{usuario}"

Finalmente, cada participante deberá elaborar un archivo Markdown en Github con acceso publico. Por su puesto en el documento debe obviarse datos sensibles como claves etc.

El documento Markdown deberá contener el proceso técnico que el participante ha seguido con un orden lógico y el link del proyecto Githab deberá ser registrado como entregable de esta tarea.
#### **RESULTADO**
**PASO 1:** Ingestrar datos de mysql
![](https://raw.githubusercontent.com/faustol/synapse/main/mysql.png)

**PASO 1.1:** Obtener las tablas de mysql
```json
        "activities": [
            {
                "name": "getTablesName",
                "type": "Lookup",
                "dependsOn": [],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "MySqlSource",
                        "query": "select\ntable_name as tabla\nfrom information_schema. tables\nwhere\ntable_schema='dwh'"
                    },
                    "dataset": {
                        "referenceName": "floja_ds_mysql",
                        "type": "DatasetReference",
                        "parameters": {
                            "vTabla": "colocartablename"
                        }
                    },
                    "firstRowOnly": false
                }
            }]
```
**PASO 1.2:** Copiamos las tablas al storage account
```json
 {
                "name": "dataExtract",
                "type": "ForEach",
                "dependsOn": [
                    {
                        "activity": "getTablesName",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "userProperties": [],
                "typeProperties": {
                    "items": {
                        "value": "@activity('getTablesName').output.value",
                        "type": "Expression"
                    },
                    "activities": [
                        {
                            "name": "copyData",
                            "type": "Copy",
                            "dependsOn": [],
                            "policy": {
                                "timeout": "7.00:00:00",
                                "retry": 0,
                                "retryIntervalInSeconds": 30,
                                "secureOutput": false,
                                "secureInput": false
                            },
                            "userProperties": [],
                            "typeProperties": {
                                "source": {
                                    "type": "MySqlSource"
                                },
                                "sink": {
                                    "type": "ParquetSink",
                                    "storeSettings": {
                                        "type": "AzureBlobFSWriteSettings"
                                    },
                                    "formatSettings": {
                                        "type": "ParquetWriteSettings"
                                    }
                                },
                                "enableStaging": false,
                                "translator": {
                                    "type": "TabularTranslator",
                                    "typeConversion": true,
                                    "typeConversionSettings": {
                                        "allowDataTruncation": true,
                                        "treatBooleanAsNumber": false
                                    }
                                }
                            },
                            "inputs": [
                                {
                                    "referenceName": "floja_ds_mysql",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "vTabla": {
                                            "value": "@item().tabla",
                                            "type": "Expression"
                                        }
                                    }
                                }
                            ],
                            "outputs": [
                                {
                                    "referenceName": "floja_sd_dl",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "vTabla": {
                                            "value": "@item().tabla",
                                            "type": "Expression"
                                        }
                                    }
                                }
                            ]
                        }
                    ]
                }
 }
```
Con este proceso hemos completado la extracción de datos de mysql y almacenado el resultado en el storage account.
![](https://raw.githubusercontent.com/faustol/synapse/main/storage%20inicial.png)
