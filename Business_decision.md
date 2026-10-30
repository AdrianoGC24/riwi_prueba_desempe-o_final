Deciciones tomadas para el proceso de analisis de datos empresarial para la empresa Datamart realizado por Adriano de Jesus González Cera

En este documento detallo el paso a paso que realicé junto con mi equipo para realizar el proyecto de analisis de datos y pipeline etl orquestado con airflow para el negocio Datamart.

Empezamos realizando un proceso de EDA a las fuentes de datos  y así tener la radiografia completa de como está la calidad de los datos actualmente. Se pudo observar que las fuentes de datos tienen exactamente las mismas facturas, con la diferencia de que online_retail.csv contiene una columna extra llamada index. Por ende se toma como fuente de datos oficial a data.csv para omitir la columna adicional de la otra fuente y que al momento de empezar a eliminar registros duplicados no vayan a quedar vacíos en los indices, estos se podrán sacar al final cuando los datos estén completamente limpios.

Se decidió realizar seguidamente el esquema de la base de datos teniendo 2 fact table y 3 dim ademas de la tabla de auditoria de rechazon. La tabla date nos sirve para facilitar las futuras consultas y en caso de que se conecte la DB con una herramienta BI ya tenga esta la dim de fechas y agilizar cualquier proceso de diseñar dashboards. 

Se creó la estructura de carpetas del sistema y la creacion de los archivos principales para levantar el entorno siguiendo buenas practicas seguido de tener un registro y control de commits para montar a github e ir haciendo push en cuanto se van realizando respectivos cambios importantes.

luego de haber comprobado que el entorno se levanta correctamente y sin ningun error seguimos con el diseño del dag el cual define los parametros y argumentos, se le da una programacion diaria a las 12:00 con @daily, se le coloca un retry de 2 intentos en caso de que algo falle y se ejecutan 4 tareas, 2 de extraccion para cada uno de los csv en paralelo (ya que aunque son iguales, el documento tecnico lo requiere)

Se identifico en la transformacion que los customer id tienen .0 al final el cual se quita ya que se realizó una investigacion y ninguna id de ninguno de los paises tiene algo parecido por lo que esto es meramente por error de formato 