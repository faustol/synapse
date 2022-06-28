# **TRABAJO FINAL SYNAPSE**

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


**PASO 2:** Obtenermos los correos desde el archivo, el mismo que lo he almacenado en una URL y creamos una actividad de copia
**PASO 2.1:** Lectura del archivo, hay varias formas de hacerlo pero he utilizado un servicio vinculado http de Synapse como origen

![](https://raw.githubusercontent.com/faustol/synapse/main/vinculado.png)

```json
{
    "name": "floja_sd_mails",
    "properties": {
        "linkedServiceName": {
            "referenceName": "flojaGitHub",
            "type": "LinkedServiceReference"
        },
        "annotations": [],
        "type": "DelimitedText",
        "typeProperties": {
            "location": {
                "type": "HttpServerLocation"
            },
            "columnDelimiter": ",",
            "escapeChar": "\\",
            "firstRowAsHeader": true,
            "quoteChar": "\""
        },
        "schema": []
    },
    "type": "Microsoft.Synapse/workspaces/datasets"
}
```

**PASO 2.1:** Escritura del archivo en el destino storage account


![](https://raw.githubusercontent.com/faustol/synapse/main/destino.png)

```json
{
    "name": "floja_pipeline_file",
    "properties": {
        "activities": [
            {
                "name": "getFile",
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
                        "type": "DelimitedTextSource",
                        "storeSettings": {
                            "type": "HttpReadSettings",
                            "requestMethod": "GET"
                        },
                        "formatSettings": {
                            "type": "DelimitedTextReadSettings"
                        }
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
                        "referenceName": "floja_sd_mails",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "floja_sd_dl",
                        "type": "DatasetReference",
                        "parameters": {
                            "vTabla": "mails"
                        }
                    }
                ]
            }
        ],
        "folder": {
            "name": "floja"
        },
        "annotations": [],
        "lastPublishTime": "2022-06-23T00:41:46Z"
    },
    "type": "Microsoft.Synapse/workspaces/pipelines"
}
```

En este paso 2 hubiera podido usar spark pero para aprovechar las bondades de Synapse Data factory lo he realizado con HTTP.

**PASO 3:** Tranformación de datos mediante pyspark para llegar al resultado esperado
**PASO 3.1:** Lectura de los datos
```python
%%pyspark
##Ruta genérica
path='abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/floja/'
##Rutas
vPathCliente = path+'cliente.parquet'
vPathFactura = path+'factura.parquet'
vPathFacProducto = path+'facturaproducto.parquet'
vPathMail = path+'mails.parquet'
vPathProducto = path+'producto.parquet'

dfCliente = spark.read.load(vPathCliente, format='parquet')
dfFactura = spark.read.load(vPathFactura, format='parquet')
dfFacturaProducto = spark.read.load(vPathFacProducto, format='parquet')
dfProducto = spark.read.load(vPathProducto, format='parquet')
dfMail = spark.read.load(vPathMail, format='parquet')
```
**PASO 3.2:** Creación de tablas temporales

```python
##Crear las tablas temporales
dfCliente.createOrReplaceTempView("tmp_cliente")
dfFactura.createOrReplaceTempView("tmp_factura")
dfFacturaProducto.createOrReplaceTempView("tmp_facturaproducto")
dfProducto.createOrReplaceTempView("tmp_producto")
dfMail.createOrReplaceTempView("tmp_mail")
display(dfFacturaProducto)
```

**PASO 3.3:** Consultas para llegar al resultado

```python
##SQL Integración de tablas
vSql = """
SELECT c.rowidcliente,
    m.email email,
    p.producto,
    max(fp.fecha) fch_compra, 
    count(p.rowidproducto) cantidad,
    sum(fp.valorventaproducto) mnt_venta
FROM tmp_cliente c 
    INNER JOIN tmp_mail m on m.rowidcliente = c.rowidcliente
    INNER JOIN tmp_factura f on c.rowidcliente = f.rowidcliente
    INNER JOIN tmp_facturaproducto fp on fp.rowidfactura = f.rowidfactura
    INNER JOIN tmp_producto p on p.rowidproducto = fp.rowidproducto
    
GROUP BY c.rowidcliente,p.producto,m.email
"""
dfCompleto = spark.sql(vSql)
dfCompleto.createOrReplaceTempView("tmp_completo")
##SQL  máxima de producto por cliente
vSql = """
SELECT c.rowidcliente, 
    max(c.mnt_venta) maximo_valor,
    max(c.cantidad) maximo_producto
FROM tmp_completo c
GROUP BY c.rowidcliente
"""

dfProCliente = spark.sql(vSql)
dfProCliente.createOrReplaceTempView("tmp_maximos")

##SQL Unificación
vSql = """
SELECT c.rowidcliente,
    c.producto,
    c.email,
    c.fch_compra, 
    c.cantidad ,
    c.mnt_venta
FROM tmp_completo c 
INNER JOIN tmp_maximos m on c.rowidcliente = m.rowidcliente 
            and m.maximo_producto = c.cantidad 
            and c.mnt_venta = m.maximo_valor
"""
```
**PASO 3.4:** Almacenamiento del resultado

```python
##GRABAR RESULTADO FINAL
dfResultadoFinal = spark.sql(vSql)
dfResultadoFinal.write.mode("overwrite").saveAsTable("default.tbl_floja")

vPathFinal = path+'tbl_floja.parquet'
dfResultadoFinal.repartition(1).write.mode("overwrite").parquet(vPathFinal)
```

#### **PIPELINE FINAL**
El pipe final integrado con los tres pasos

![](https://raw.githubusercontent.com/faustol/synapse/main/PIPELINE%20FINAL.png)

El archivo final se almacena en el storage account

![](https://raw.githubusercontent.com/faustol/synapse/main/resultado.png)
