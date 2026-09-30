Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y
Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la
cantidad de controles.

## Método de sincronización

Las instancias de Sum consumen de una misma cola de trabajo (`input_queue`), por lo que cada réplica acumula solo una
parte de los datos de cada cliente y el EOF de un cliente lo recibe una única réplica. Para que todas se enteren de que
el cliente terminó, se usa un exchange de coordinación entre las instancias de Sum (`<SUM_PREFIX>_coordination`, routing
key `<SUM_PREFIX>_eof`). Cada réplica bindea su propia cola anónima a esa misma
routing key, de modo que el exchange funciona como un fanout: la réplica que recibe el EOF desde `input_queue` no
flushea nada directamente, sino que publica el EOF en el exchange de coordinación y así llega a todas las instancias,
incluida ella misma.

Al recibir el EOF por el canal de coordinación, cada Sum envía sus resultados parciales de ese cliente y su propio EOF a
las instancias de Aggregation a través del exchange de salida (`<AGGREGATION_PREFIX>`, con routing keys
`<AGGREGATION_PREFIX>_i`), que es independiente del de coordinación. Luego descarta el estado de ese cliente.

Para no hacer broadcast de todos los datos a todas las instancias de Aggregation, cada Sum reparte las frutas en
buckets: se realiza un hash al nombre de la fruta y toma el módulo por la cantidad de instancias de
Aggregations, y el bucket `i` se envía solo con la routing key `<AGGREGATION_PREFIX>_i`. Junto con cada bucket se envía
la cantidad de registros de entrada que lo componen, que el Aggregation usa para saber cuándo recibió todo.

