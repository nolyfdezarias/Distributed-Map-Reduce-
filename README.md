# Distributed-Map-Reduce-

Map-Reduce es un modelo de programacion para dar soporte a la computacion paralela sobre grandes colecciones de dato en grupos de computadoras.El nombre del framework esta inspirado en los nombres de dos importantes funciones de la programacion 
funcional.Actualmente el referente mundial de este software es Hadoop ,  una implementacion OpenSource que en sus inicios fue
desarrollada por Yahoo y actualmente lo desarrolla Apache.

## Especificaciones

Nuestro proyecto cuenta con 4 modulos:
- ```Client```
- ```Worker```
- ```Master```
- ```utils```

En utils asociamos un conjunto de metodos y  valores comunes a los demas modulos.Vale destacar el que el valor del ```BROADCASTPORT``` es uno de los mas importantes en todo el sistema.Asegurece de que ese puerto este disponible antes de correr el programa.

En una sistema Map-Reduce aparcen por lo general 3 tipos de nodos : ```Client```,```Worker``` y ```Master```.El primero es el proveedor de informacion,el trabajador es el encargado de realizar las funciones map y reduce a la orden del master y por consecuente el master es aquel que guia los hilos del proceso.

Habiamos mencionado previamente que en este sistema existen dos funciones principales : 

- ```Map``` -> mapea y filtra un conjunto de datos representados por una tupla (llave,valor)

- ```Reduce``` -> procesa todos los valores asociados a una llave determinada

Como se puede apreciar los metodos principales reciben de entrada (llave,valor).Por Consiguiente el texto de entrada debe cumplir con esto, en nuestro caso vamos a asumir que el texto esta separado por lineas y las lineas son de la siguiente forma : 
```
text1-one of the copies of the program is special the master the rest are workers that are assigned work by the master there are map tasks and reduce tasks to assign the master picks idle workers and assigns each one a map task or a reduce task
```
Notar q asumiremos que el separador de la llave y el valor sera ```'-'``` , por lo cual no debe aparecer mas de una vez por linea.

## Instalando dependecias y ajustando parametros

En la carpeta ```requirements``` estan las librerias de python que necesitamos instalar para nuestro desarrollo , usted puede instalarlas a mano o simplemente correr en una consola desde la carpeta del proyecto el comando :
```
make build
```
De ourrir algun problema de compatibilidad ejecute simplemente el siguiente comando desde una terminal en la carpeta del proyecto :
```
make Internet
```
Instaladas las dependencias necesarias,pasemos a levantar el sistema.El adiministrador debera ajustar ciertos parametros en el modulo utils.

- ```BROADCASTPORT```
- ```SIZEOFTASK```
- ```TIMEOUT```
- ```RETRYTIME```
- ```map1```
- ```reduce1```

Con respecto al ```BROADCASTPORT``` solo necesitamos que el puerto este disponible, defina ```SIZEOFTASK``` con respecto a su capacidad de computo esta variabe esta asociada a la cantidad de lineas a mapear por un trabajador; ```TIMEOUT``` esta asociado al tiempo de espera de los sockets notar q su valor esta en milisegundos y el ```RETRYTIME``` es para la cantidad de veces que se desea reintentar si algun socket da ```TIMEOUT``` se recomienda valores altos si hay congestion en la red.Las funciones ```map1``` y ```reduce1``` son las que utilizaran nuestros trabajadores; ahora mismo provemos de un ejemplo que sirve para el conteo de palabras:

```python
def map1(key,value):
    res = []
    for word in value.split(' '):
        res.append((word, 1))
    return res
    
def reduce1(key,values):
    res = 0
    for elem in values:
        res += int(elem)
    return res
```
## Levantar el sistema

Levantar un ```Master``` ,aunque se recomienda tener almenos 3 ```Master``` para evitar un punto de falla unica,eso si levantar los ```Masters``` en carpetas firentes si estan en la misma PC.La ejecucion del Master le pedira al administrador un valor por consola, asegrece que el primer master tenga valor "1" y el resto "0".

Para correr un master se puede hacer con el comando:

```
python3 Master.py
```
o simplemente
```
make Server
```

Una vez levantado el server ,levantamos los trabajadores con el comando :
```
make Worker
```
Finalmente levantemos el cliente con el comando:
```
make Client
```